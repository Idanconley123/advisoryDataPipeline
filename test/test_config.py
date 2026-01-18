"""
Unit tests for the config module.
"""

import pytest
from src.advisory_pipeline.config.config import Config, PipelineConfig, EnrichmentConfig


class TestPipelineConfig:
    """Tests for PipelineConfig dataclass."""

    def test_pipeline_config_creation(self):
        """Test creating a PipelineConfig with all required fields."""
        config = PipelineConfig(
            run_id="test_run_001",
            checkpoint_path="/test/checkpoints",
            staging_path="/test/staging",
            prod_path="/test/prod",
            output_path="/test/output",
            temp_path="/test/temp",
            batch_size=500,
        )

        assert config.run_id == "test_run_001"
        assert config.checkpoint_path == "/test/checkpoints"
        assert config.staging_path == "/test/staging"
        assert config.prod_path == "/test/prod"
        assert config.output_path == "/test/output"
        assert config.temp_path == "/test/temp"
        assert config.batch_size == 500

    def test_pipeline_config_is_frozen(self):
        """Test that PipelineConfig is immutable."""
        config = PipelineConfig(
            run_id="test_run",
            checkpoint_path="/test/checkpoints",
            staging_path="/test/staging",
            prod_path="/test/prod",
            output_path="/test/output",
            temp_path="/test/temp",
            batch_size=1000,
        )

        with pytest.raises(AttributeError):
            config.run_id = "new_run_id"


class TestEnrichmentConfig:
    """Tests for EnrichmentConfig dataclass."""

    def test_enrichment_config_creation(self):
        """Test creating an EnrichmentConfig with required fields."""
        config = EnrichmentConfig(cache_path="/test/cache")

        assert config.cache_path == "/test/cache"
        assert config.cache_ttl_hours == 0.1  # Default value
        assert config.incremental_enabled is True  # Default value

    def test_enrichment_config_with_custom_values(self):
        """Test creating an EnrichmentConfig with custom values."""
        config = EnrichmentConfig(
            cache_path="/custom/cache",
            cache_ttl_hours=2.5,
            incremental_enabled=False,
        )

        assert config.cache_path == "/custom/cache"
        assert config.cache_ttl_hours == 2.5
        assert config.incremental_enabled is False

    def test_enrichment_config_is_frozen(self):
        """Test that EnrichmentConfig is immutable."""
        config = EnrichmentConfig(cache_path="/test/cache")

        with pytest.raises(AttributeError):
            config.cache_path = "/new/cache"


class TestConfig:
    """Tests for the master Config dataclass."""

    def test_config_creation(self):
        """Test creating a Config with pipeline and enrichment configs."""
        pipeline_config = PipelineConfig(
            run_id="test_run",
            checkpoint_path="/test/checkpoints",
            staging_path="/test/staging",
            prod_path="/test/prod",
            output_path="/test/output",
            temp_path="/test/temp",
            batch_size=1000,
        )
        enrichment_config = EnrichmentConfig(cache_path="/test/cache")

        config = Config(
            pipeline=pipeline_config,
            enrichment=enrichment_config,
        )

        assert config.pipeline == pipeline_config
        assert config.enrichment == enrichment_config
        assert config.spark_app_name == "advisory-pipeline"  # Default
        assert config.log_level == "INFO"  # Default

    def test_config_from_defaults(self, sample_run_id, sample_base_path):
        """Test creating Config using from_defaults factory method."""
        config = Config.from_defaults(
            run_id=sample_run_id,
            base_path=sample_base_path,
        )

        assert config.pipeline.run_id == sample_run_id
        assert config.pipeline.checkpoint_path == f"{sample_base_path}/checkpoints"
        assert config.pipeline.staging_path == f"{sample_base_path}/output/staging"
        assert config.pipeline.prod_path == f"{sample_base_path}/output/prod"
        assert config.pipeline.output_path == f"{sample_base_path}/output"
        assert config.pipeline.temp_path == f"{sample_base_path}/temp"
        assert config.pipeline.batch_size == 1000
        assert config.enrichment.cache_path == f"{sample_base_path}/output/enrichment_cache"
        assert config.spark_app_name == "advisory-pipeline"
        assert config.log_level == "INFO"

    def test_config_from_defaults_with_custom_ttl(self, sample_run_id, sample_base_path):
        """Test from_defaults with custom cache TTL."""
        config = Config.from_defaults(
            run_id=sample_run_id,
            base_path=sample_base_path,
            cache_ttl_hours=1.5,
        )

        assert config.enrichment.cache_ttl_hours == 1.5

    def test_config_from_defaults_incremental_disabled(self, sample_run_id, sample_base_path):
        """Test from_defaults with incremental enrichment disabled."""
        config = Config.from_defaults(
            run_id=sample_run_id,
            base_path=sample_base_path,
            incremental_enabled=False,
        )

        assert config.enrichment.incremental_enabled is False

    def test_config_is_frozen(self):
        """Test that Config is immutable."""
        config = Config.from_defaults(run_id="test_run")

        with pytest.raises(AttributeError):
            config.spark_app_name = "new-app-name"
