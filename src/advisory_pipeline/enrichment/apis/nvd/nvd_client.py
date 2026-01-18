import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from threading import Lock
from typing import List, Dict, Any, Optional

import requests
from pyspark.sql import SparkSession, DataFrame
from ...schemas.raw_nvd_schema import raw_nvd_schema
from .config import (
    NVD_API_KEY,
    NVD_API_URL,
    RATE_LIMIT,
    MAX_RETRIES,
    RETRY_BACKOFF_SECONDS,
    PARALLEL_WORKERS,
)

logger = logging.getLogger(__name__)

# Thread-safe rate limiter for parallel requests
_rate_limit_lock = Lock()
_last_request_time = 0.0


def _get_headers() -> Dict[str, str]:
    """Get request headers including API key if available."""
    headers = {"Content-Type": "application/json"}
    if NVD_API_KEY:
        headers["apiKey"] = NVD_API_KEY
    return headers


def _wait_for_rate_limit() -> None:
    """Thread-safe rate limiting."""
    global _last_request_time

    with _rate_limit_lock:
        current_time = time.time()
        min_interval = 1.0 / RATE_LIMIT
        time_since_last = current_time - _last_request_time

        if time_since_last < min_interval:
            sleep_time = min_interval - time_since_last
            time.sleep(sleep_time)

        _last_request_time = time.time()


def query_cve(cve_id: str, use_rate_limit: bool = True) -> Dict[str, Any]:
    """
    Query NVD API for a single CVE.

    Args:
        cve_id: CVE identifier (e.g., "CVE-2024-3094")
        use_rate_limit: Whether to apply rate limiting

    Returns:
        API response dict with vulnerability data, or empty dict if not found
    """
    url = f"{NVD_API_URL}?cveId={cve_id}"
    headers = _get_headers()

    for attempt in range(MAX_RETRIES):
        try:
            if use_rate_limit:
                _wait_for_rate_limit()

            response = requests.get(url, headers=headers, timeout=30)

            if response.status_code == 404:
                return {}

            if response.status_code == 403:
                logger.warning(f"NVD API rate limited for {cve_id}, waiting 30s...")
                time.sleep(30)
                continue

            response.raise_for_status()
            data = response.json()

            vulnerabilities = data.get("vulnerabilities", [])
            if vulnerabilities:
                return vulnerabilities[0].get("cve", {})

            return {}

        except requests.exceptions.RequestException as e:
            logger.warning(
                f"NVD API request failed (attempt {attempt + 1}/{MAX_RETRIES}): {e}"
            )
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_BACKOFF_SECONDS * (attempt + 1))
            else:
                logger.error(f"NVD API request failed after {MAX_RETRIES} attempts")
                return {}

    return {}


def _query_cve_worker(cve_id: str) -> tuple:
    """Worker function for parallel querying."""
    try:
        result = query_cve(cve_id, use_rate_limit=True)
        return (cve_id, result)
    except Exception as e:
        logger.error(f"Worker error for {cve_id}: {e}")
        return (cve_id, {})


def query_batch_sequential(cve_ids: List[str]) -> Dict[str, Dict[str, Any]]:
    """Query NVD API sequentially."""
    results: Dict[str, Dict[str, Any]] = {}

    for i, cve_id in enumerate(cve_ids):
        if i > 0 and i % 50 == 0:
            logger.info(f"NVD progress: {i}/{len(cve_ids)} CVEs queried")

        try:
            results[cve_id] = query_cve(cve_id)
        except Exception as e:
            logger.error(f"Error querying NVD for {cve_id}: {e}")
            results[cve_id] = {}

    return results


def query_batch_parallel(
    cve_ids: List[str], max_workers: int = PARALLEL_WORKERS
) -> Dict[str, Dict[str, Any]]:
    """Query NVD API in parallel using ThreadPoolExecutor."""
    results: Dict[str, Dict[str, Any]] = {}
    completed = 0

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_cve = {
            executor.submit(_query_cve_worker, cve_id): cve_id for cve_id in cve_ids
        }

        for future in as_completed(future_to_cve):
            cve_id = future_to_cve[future]
            try:
                _, data = future.result()
                results[cve_id] = data
                completed += 1

                if completed % 50 == 0:
                    logger.info(f"NVD progress: {completed}/{len(cve_ids)} CVEs queried")
            except Exception as e:
                logger.error(f"CVE {cve_id} generated an exception: {e}")
                results[cve_id] = {}

    return results


def query_batch(cve_ids: List[str]) -> Dict[str, Dict[str, Any]]:
    """
    Query NVD API for multiple CVEs.

    Uses parallel processing when API key is available,
    otherwise falls back to sequential processing.
    """
    if not cve_ids:
        return {}

    if NVD_API_KEY:
        mode = f"parallel ({PARALLEL_WORKERS} workers)"
        logger.info(f"Querying NVD for {len(cve_ids)} CVEs - {mode}, {RATE_LIMIT}/sec")
        results = query_batch_parallel(cve_ids)
    else:
        mode = "sequential (no API key)"
        logger.info(f"Querying NVD for {len(cve_ids)} CVEs - {mode}, {RATE_LIMIT}/sec")
        results = query_batch_sequential(cve_ids)

    found_count = sum(1 for v in results.values() if v)
    logger.info(f"NVD batch complete: {found_count}/{len(cve_ids)} CVEs found")

    return results


def _extract_fixed_version(cve_data: Dict[str, Any]) -> Optional[str]:
    """
    Extract fixed version from NVD CVE data.

    Looks in configurations -> nodes -> cpeMatch for versionEndExcluding.
    """
    try:
        configurations = cve_data.get("configurations", [])

        for config in configurations:
            nodes = config.get("nodes", [])
            for node in nodes:
                cpe_matches = node.get("cpeMatch", [])
                for cpe in cpe_matches:
                    version_end = cpe.get("versionEndExcluding")
                    if version_end:
                        return version_end

                    version_end_inc = cpe.get("versionEndIncluding")
                    if version_end_inc:
                        return f">{version_end_inc}"

        return None
    except Exception:
        return None


def enrich_from_nvd(
    spark: SparkSession,
    cves_df: DataFrame,
) -> DataFrame:
    """
    Enrich CVEs with NVD data.

    Extracts only fields used in normalization:
    - nvd_fixed_version
    - nvd_status

    Args:
        spark: SparkSession
        cves_df: DataFrame with CVEs to enrich (must have cve_id column)

    Returns:
        DataFrame with NVD enrichment data
    """
    cve_count = cves_df.count()
    logger.info(f"Starting NVD enrichment for {cve_count} CVEs")

    # Collect CVE IDs
    cve_rows = cves_df.select("cve_id", "package").collect()
    cve_ids = [row["cve_id"] for row in cve_rows]
    package_map = {row["cve_id"]: row["package"] for row in cve_rows}

    # Batch query NVD
    nvd_results = query_batch(cve_ids)

    # Build enriched results - only essential fields
    enriched_results = []
    query_timestamp = datetime.now(timezone.utc).isoformat()

    for cve_id in cve_ids:
        cve_data = nvd_results.get(cve_id, {})
        package = package_map.get(cve_id)

        if cve_data:
            fixed_version = _extract_fixed_version(cve_data)
            if fixed_version:
                logger.info(f"NVD found fix for {cve_id}: {fixed_version}")

            enriched_results.append(
                {
                    "cve_id": cve_id,
                    "package": package,
                    "nvd_found": True,
                    "nvd_fixed_version": fixed_version,
                    "nvd_status": cve_data.get("vulnStatus"),
                    "nvd_query_timestamp": query_timestamp,
                }
            )
        else:
            enriched_results.append(
                {
                    "cve_id": cve_id,
                    "package": package,
                    "nvd_found": False,
                    "nvd_fixed_version": None,
                    "nvd_status": None,
                    "nvd_query_timestamp": query_timestamp,
                }
            )

    # Create DataFrame
    if enriched_results:
        enriched_df = spark.createDataFrame(enriched_results, schema=raw_nvd_schema)
    else:
        enriched_df = spark.createDataFrame([], schema=raw_nvd_schema)

    found_count = len([r for r in enriched_results if r["nvd_found"]])
    logger.info(
        f"NVD enrichment complete: {found_count}/{len(enriched_results)} CVEs found"
    )

    return enriched_df
