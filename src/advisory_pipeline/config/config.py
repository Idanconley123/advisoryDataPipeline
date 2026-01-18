from dataclasses import dataclass


@dataclass(frozen=True, kw_only=True)
class PipelineConfig:
    """Configuration for pipeline execution."""

    run_id: str
    checkpoint_path: str
    staging_path: str
    prod_path: str
    output_path: str
    temp_path: str
    batch_size: int


@dataclass(frozen=True, kw_only=True)
class EnrichmentConfig:
    """Configuration for enrichment caching."""

    cache_path: str  # Persistent path for enrichment cache
    cache_ttl_hours: float = 0.1  # Re-enrich CVEs older than this (0.25 = 15 minutes)
    incremental_enabled: bool = True  # Enable/disable incremental enrichment


@dataclass(frozen=True, kw_only=True)
class Config:
    """Master configuration for advisory pipeline."""

    pipeline: PipelineConfig
    enrichment: EnrichmentConfig
    spark_app_name: str = "advisory-pipeline"
    log_level: str = "INFO"

    @classmethod
    def from_defaults(
        cls,
        run_id: str,
        base_path: str = "/Users/idanconley/PycharmProjects/advisoryDataPipeline/src/advisory_pipeline",
        cache_ttl_hours: float = 0.05,
        incremental_enabled: bool = True,
    ) -> "Config":
        """Create config with sensible defaults."""
        return cls(
            pipeline=PipelineConfig(
                run_id=run_id,
                checkpoint_path=f"{base_path}/checkpoints",
                staging_path=f"{base_path}/output/staging",
                prod_path=f"{base_path}/output/prod",
                output_path=f"{base_path}/output",
                temp_path=f"{base_path}/temp",
                batch_size=1000,
            ),
            enrichment=EnrichmentConfig(
                cache_path=f"{base_path}/output/enrichment_cache",
                cache_ttl_hours=cache_ttl_hours,
                incremental_enabled=incremental_enabled,
            ),
            spark_app_name="advisory-pipeline",
            log_level="INFO",
        )
