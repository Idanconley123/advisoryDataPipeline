from ..dependencies import Dependencies

from ..config import Config
from ..pipeline_libs.spark import read_table, write_table
from .schemas.cache_schema import enrichment_cache_schema
import logging
from typing import Optional
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, current_timestamp
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


def _load_enrichment_cache(
    deps: Dependencies, 
    config: Config, 
    source_name: Optional[str] = None
) -> Optional[DataFrame]:
    cache_path = config.enrichment.cache_path
    
    if source_name:
        load_path = f"{cache_path}/source_name={source_name}"
    else:
        load_path = cache_path
    
    df = read_table(
        spark=deps.spark,
        path=load_path,
        schema=enrichment_cache_schema,
    )
    
    count = df.count()
    if count == 0:
        scope = f"source={source_name}" if source_name else "all sources"
        logger.info(f"No cache entries found ({scope})")
        return None
    
    df = df.persist()
    
    scope = f"source={source_name}" if source_name else "all sources"
    logger.info(f"Loaded and persisted {count} cache entries ({scope})")
    return df


def filter_recently_enriched(
    deps: Dependencies,
    config: Config,
    cves_to_enrich: DataFrame,
    cache_df: Optional[DataFrame],
    source_name: str,
) -> DataFrame:
    """
    Filter out CVEs that were recently enriched for this source.
    
    Args:
        cves_to_enrich: DataFrame with cve_id, package columns
        cache_df: Cache DataFrame (may contain multiple sources)
        source_name: Current source to filter for
    
    Returns:
        DataFrame of CVEs that need fresh enrichment
    """
    if cache_df is None or not config.enrichment.incremental_enabled:
        logger.info(f"No cache or incremental disabled - will enrich all {cves_to_enrich.count()} CVEs")
        return cves_to_enrich
    
    ttl_hours = config.enrichment.cache_ttl_hours
    cutoff_timestamp = datetime.now() - timedelta(hours=ttl_hours)
    
    # Filter cache for this source and recent entries
    source_cache = cache_df.filter(
        (col("source_name") == source_name) & 
        (col("last_accessed") >= lit(cutoff_timestamp))
    )
    
    recent_count = source_cache.count()
    ttl_minutes = int(ttl_hours * 60)
    logger.info(f"Found {recent_count} CVEs in cache for {source_name} (within {ttl_minutes} minutes)")
    
    if recent_count == 0:
        return cves_to_enrich
    
    # Get recently cached CVE+package combinations
    cached_keys = source_cache.select(
        col("cve_id"),
        col("package_name").alias("package")
    ).distinct()
    
    # Filter out recently enriched CVEs
    cves_needing_enrichment = cves_to_enrich.join(
        cached_keys,
        on=["cve_id", "package"],
        how="left_anti",
    )
    
    original_count = cves_to_enrich.count()
    new_count = cves_needing_enrichment.count()
    skipped_count = original_count - new_count
    
    logger.info(f"Incremental filtering for {source_name}:")
    logger.info(f"  - {original_count} total CVEs to enrich")
    logger.info(f"  - {skipped_count} skipped (recently cached)")
    logger.info(f"  - {new_count} need fresh enrichment")
    
    return cves_needing_enrichment


def update_enrichment_cache(
    deps: Dependencies,
    config: Config,
    source_name: str,
    enriched_cves: DataFrame,
    existing_cache_df: Optional[DataFrame] = None,
):
    """
    Update the enrichment cache with newly enriched CVEs.
    
    Args:
        existing_cache_df: Pre-loaded cache DataFrame (avoids re-reading from disk)
    """
    cache_path = config.enrichment.cache_path
    
    # Create cache entries from enriched CVEs
    cache_entries = enriched_cves.select(
        col("cve_id"),
        col("package").alias("package_name"),
    ).withColumn(
        "source_name", lit(source_name)
    ).withColumn(
        "last_accessed", current_timestamp()
    )
    
    # Use pre-loaded cache filtered by source (no disk re-read)
    if existing_cache_df is not None:
        existing_for_source = existing_cache_df.filter(col("source_name") == source_name)
        existing_count = existing_for_source.count()
        
        if existing_count > 0:
            # Remove old entries for CVEs we just re-enriched
            existing_without_new = existing_for_source.join(
                cache_entries.select("cve_id", "package_name"),
                on=["cve_id", "package_name"],
                how="left_anti",
            )
            merged_cache = cache_entries.unionByName(existing_without_new)
        else:
            merged_cache = cache_entries
    else:
        merged_cache = cache_entries
    
    # Write to partition
    partition_path = f"{cache_path}/source_name={source_name}"
    write_table(
        spark=deps.spark,
        path=partition_path,
        write_method="overwrite",
        partition_keys=[],
        partitions=1,
        schema=enrichment_cache_schema,
        sql="",
        df=merged_cache,
    )
    
    logger.info(f"Updated cache for {source_name}: {merged_cache.count()} total entries")
