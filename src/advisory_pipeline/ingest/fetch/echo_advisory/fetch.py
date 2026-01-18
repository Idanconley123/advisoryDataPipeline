import logging
from typing import List, Dict, Any

from pyspark.sql import Row
from pyspark.sql.types import StructType

import requests

from ....dependencies import Dependencies
from ....config import Config
from ....pipeline_libs.spark import write_table, read_table

logger = logging.getLogger(__name__)


def _flatten_advisory_json(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Flatten nested Echo Advisory JSON into rows.

    """
    rows = []

    for package_name, cves in data.items():
        if not isinstance(cves, dict):
            continue

        for cve_id, cve_data in cves.items():
            if not cve_id.startswith("CVE-"):
                continue

            fixed_version = None
            if isinstance(cve_data, dict):
                fixed_version = cve_data.get("fixed_version")

            rows.append(
                {
                    "package_name": package_name,
                    "cve_id": cve_id,
                    "fixed_version": fixed_version,
                }
            )

    return rows


def ingest_echo_advisory_source(
    deps: Dependencies,
    schema: StructType,
    config: Config,
    url: str,
    run_id: str,
    table_name: str,
):
    """
    Ingest Echo Advisory source.

    Downloads data.json, flattens the nested structure, and saves to staging.
    """
    # Download the JSON data
    full_url = f"{url}/{table_name}.json"
    logger.info(f"Fetching Echo Advisory from: {full_url}")

    response = requests.get(full_url, timeout=60)
    response.raise_for_status()

    # Parse and flatten the nested JSON
    raw_data = response.json()
    flattened_rows = _flatten_advisory_json(raw_data)

    logger.info(f"Flattened {len(flattened_rows)} CVE entries from Echo Advisory")

    # Create DataFrame from flattened data (convert dicts to Row objects)
    rows = [Row(**row) for row in flattened_rows]
    df = deps.spark.createDataFrame(rows, schema=schema)

    logger.info(f"Created DataFrame with {df.count()} rows")
    df.show(10, truncate=False)

    # Write to staging
    output_path = f"{config.pipeline.staging_path}/run_id={run_id}/sources/{table_name}"
    write_table(
        spark=deps.spark,
        path=output_path,
        write_method="overwrite",
        partition_keys=[],
        partitions=1,
        schema=schema,
        sql="",
        df=df,
    )

    # Read back and register as temp view
    df = read_table(
        spark=deps.spark,
        path=output_path,
        schema=schema,
    )
    df.createGlobalTempView(table_name)
