import logging
from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql.types import StructType

from ..dependencies import Dependencies
from ..config import Config
from ..pipeline_libs.spark import write_table, read_table
from .cache import filter_recently_enriched, _load_enrichment_cache, update_enrichment_cache
from .config import raw_table_list
from .queries.packages_to_enrich import create_packages_to_enrich
from .sources.upstream_sources import upstream_sources_config
from .schemas.noramlized_schema import normalized_schema

logger = logging.getLogger(__name__)


def _load_dependencies(deps: Dependencies, config: Config, run_id: str):
    """Load raw ingestion tables into global temp views."""
    for table in raw_table_list:
        df = read_table(
            spark=deps.spark,
            path=f"{config.pipeline.staging_path}/run_id={run_id}/sources/{table.name}",
            schema=table.schema,
        )
        df.createGlobalTempView(f"raw_{table.name}")
        logger.info(f"Loaded {df.count()} rows from {table.name}")

def _enrich_packages(
    deps: Dependencies, 
    config: Config, 
    run_id: str, 
    packages_to_enrich_df: DataFrame,
    cache_df: Optional[DataFrame],
):
    """
    Enrich packages with incremental support.
    
    For each source:
    1. Filter out recently enriched CVEs (using pre-loaded cache)
    2. Query API only for CVEs needing enrichment
    3. Update cache with new results
    """
    for external_enrichment in upstream_sources_config.sources:
        if external_enrichment.enrichment_function is None:
            continue
        
        source_name = external_enrichment.name
        logger.info(f"\n{'='*60}")
        logger.info(f"Processing enrichment source: {source_name}")
        logger.info(f"{'='*60}")
        
        # Step 1: Filter recently enriched (uses pre-loaded cache)
        cves_needing_enrichment = filter_recently_enriched(
            deps, config, packages_to_enrich_df, cache_df, source_name
        )
        
        # Step 2: Check if we need to query API
        if cves_needing_enrichment.count() == 0:
            logger.info(f"All CVEs recently enriched - skipping API calls for {source_name}")
            continue
        
        # Query the API
        raw_df = external_enrichment.enrichment_function(
            spark=deps.spark,
            cves_df=cves_needing_enrichment,
        )
        
        if raw_df is None:
            logger.warning(f"No results from {source_name}")
            continue
        
        # Write raw results to staging
        write_table(
            spark=deps.spark,
            path=f"{config.pipeline.staging_path}/run_id={run_id}/enrichment/raw/{source_name}",
            write_method="overwrite",
            partition_keys=[],
            partitions=1,
            schema=(
                external_enrichment.schema
                if external_enrichment.schema is not None
                else StructType([])
            ),
            sql="",
            df=raw_df,
        )
        raw_df.createOrReplaceGlobalTempView(f"raw_{source_name}")
        
        logger.info(f"Raw enrichment results for {source_name}:")
        raw_df.show(5)
        
        # Step 3: Normalize
        if external_enrichment.normalization_function is not None:
            query = external_enrichment.normalization_function(
                priority=external_enrichment.priority
            )
            normalized_df = deps.spark.sql(query)
            
            logger.info(f"Normalized results for {source_name}:")
            normalized_df.show(5)
            
            # Write normalized to staging
            write_table(
                spark=deps.spark,
                path=f"{config.pipeline.staging_path}/run_id={run_id}/enrichment/normalized/enrichment_source={source_name}",
                write_method="overwrite",
                partition_keys=[],
                partitions=1,
                schema=normalized_schema,
                sql="",
                df=normalized_df,
            )
            
            # Step 4: Update cache (pass pre-loaded cache to avoid re-reading)
            update_enrichment_cache(
                deps, config, source_name, cves_needing_enrichment, cache_df
            )


def enrich_pipeline(deps: Dependencies, config: Config, run_id: str):
    """
    Run the enrichment pipeline with incremental support.
    
    Incremental enrichment:
    - Uses unified cache partitioned by source_name
    - Cache loaded once at start (or by partition)
    - Skips CVEs enriched within cache_ttl_hours
    - Significantly reduces API calls on subsequent runs
    
    Cache structure:
        enrichment_cache/
        ├── source_name=nvd/
        │   └── *.parquet
        └── source_name=github_advisory/
            └── *.parquet
    """
    logger.info("=" * 80)
    logger.info("ENRICHMENT PIPELINE")
    logger.info("=" * 80)
    
    if config.enrichment.incremental_enabled:
        ttl_minutes = int(config.enrichment.cache_ttl_hours * 60)
        logger.info(f"Incremental enrichment: ENABLED (TTL: {ttl_minutes} minutes)")
        logger.info(f"Cache path: {config.enrichment.cache_path}")
    else:
        logger.info("Incremental enrichment: DISABLED (will query all CVEs)")
    
    # Load ingestion results
    logger.info("\n[Step 1] Loading ingestion data...")
    _load_dependencies(deps, config, run_id)
    
    # Identify CVEs needing enrichment
    logger.info("\n[Step 2] Identifying CVEs needing enrichment...")
    packages_to_enrich = create_packages_to_enrich()
    packages_to_enrich_df = deps.spark.sql(packages_to_enrich)
    
    total_to_enrich = packages_to_enrich_df.count()
    logger.info(f"Found {total_to_enrich} CVEs in pending_upstream state")
    packages_to_enrich_df.show(10)
    
    # Load enrichment cache once (all sources)
    logger.info("\n[Step 3] Loading enrichment cache...")
    cache_df = _load_enrichment_cache(deps, config)
    
    # Run enrichment with incremental support
    logger.info("\n[Step 4] Running enrichment (incremental)...")
    _enrich_packages(deps, config, run_id, packages_to_enrich_df, cache_df)
    
    # Release persisted cache from memory
    if cache_df is not None:
        cache_df.unpersist()
        logger.info("Released cache from memory")
    
    logger.info("\n" + "=" * 80)
    logger.info("ENRICHMENT COMPLETE")
    logger.info("=" * 80)
