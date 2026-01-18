"""
Unit tests for the definitions modules (ingest and enrichment).
"""

import pytest
from pyspark.sql.types import StructType, StructField, StringType

from src.advisory_pipeline.ingest.definitions import (
    Table,
    Source,
    PublicJSON,
    PostgresDB,
    PostgresProperties,
    Sources,
)
from src.advisory_pipeline.enrichment.definitions import (
    UpstreamSourceType,
    EchoStatus,
    RawTable,
    UpstreamSourceConfiguration,
    UpstreamSourcesConfig,
)


class TestIngestDefinitions:
    """Tests for ingest definitions."""

    @pytest.fixture
    def sample_schema(self):
        """Return a sample schema for testing."""
        return StructType(
            [
                StructField("id", StringType(), False),
                StructField("name", StringType(), True),
            ]
        )

    def test_table_creation(self, sample_schema):
        """Test creating a Table dataclass."""
        table = Table(table_name="test_table", schema=sample_schema)

        assert table.table_name == "test_table"
        assert table.schema == sample_schema

    def test_table_is_frozen(self, sample_schema):
        """Test that Table is immutable."""
        table = Table(table_name="test", schema=sample_schema)

        with pytest.raises(AttributeError):
            table.table_name = "new_name"

    def test_public_json_creation(self, sample_schema):
        """Test creating a PublicJSON source."""
        table = Table(table_name="data", schema=sample_schema)
        source = PublicJSON(
            name="test_source",
            url="https://example.com/data.json",
            tables=[table],
        )

        assert source.name == "test_source"
        assert source.url == "https://example.com/data.json"
        assert source.type == "PublicJSON"
        assert list(source.tables) == [table]

    def test_postgres_properties_creation(self):
        """Test creating PostgresProperties."""
        props = PostgresProperties(
            user="test_user",
            password="test_password",
        )

        assert props.user == "test_user"
        assert props.password == "test_password"
        assert props.driver == "org.postgresql.Driver"

    def test_postgres_properties_custom_driver(self):
        """Test PostgresProperties with custom driver."""
        props = PostgresProperties(
            user="user",
            password="pass",
            driver="com.custom.Driver",
        )

        assert props.driver == "com.custom.Driver"

    def test_postgres_db_creation(self, sample_schema):
        """Test creating a PostgresDB source."""
        table = Table(table_name="users", schema=sample_schema)
        props = PostgresProperties(user="admin", password="secret")

        source = PostgresDB(
            name="main_db",
            jdbc_url="jdbc:postgresql://localhost:5432/testdb",
            properties=props,
            tables=[table],
        )

        assert source.name == "main_db"
        assert source.jdbc_url == "jdbc:postgresql://localhost:5432/testdb"
        assert source.type == "postgresDB"
        assert source.properties == props
        assert list(source.tables) == [table]

    def test_sources_creation(self, sample_schema):
        """Test creating a Sources collection."""
        table = Table(table_name="data", schema=sample_schema)
        json_source = PublicJSON(
            name="json_source",
            url="https://example.com/data.json",
            tables=[table],
        )
        props = PostgresProperties(user="user", password="pass")
        pg_source = PostgresDB(
            name="pg_source",
            jdbc_url="jdbc:postgresql://localhost:5432/db",
            properties=props,
            tables=[table],
        )

        sources = Sources(sources=[json_source, pg_source])

        source_list = list(sources.sources)
        assert len(source_list) == 2
        assert isinstance(source_list[0], PublicJSON)
        assert isinstance(source_list[1], PostgresDB)


class TestEnrichmentDefinitions:
    """Tests for enrichment definitions."""

    def test_upstream_source_type_values(self):
        """Test UpstreamSourceType enum values."""
        assert UpstreamSourceType.OSV.value == "osv"
        assert UpstreamSourceType.GITHUB_ADVISORY.value == "github_advisory"
        assert UpstreamSourceType.NVD.value == "nvd"

    def test_echo_status_values(self):
        """Test EchoStatus enum values."""
        assert EchoStatus.PENDING.value == "pending_upstream"
        assert EchoStatus.FIXED.value == "fixed"
        assert EchoStatus.NOT_APPLICABLE.value == "not_applicable"

    def test_raw_table_creation(self):
        """Test creating a RawTable."""
        schema = StructType([StructField("cve_id", StringType(), False)])
        raw_table = RawTable(name="test_raw", schema=schema)

        assert raw_table.name == "test_raw"
        assert raw_table.schema == schema

    def test_upstream_source_configuration(self):
        """Test creating an UpstreamSourceConfiguration."""
        config = UpstreamSourceConfiguration(
            name="nvd",
            source_type=UpstreamSourceType.NVD,
            priority=100,
        )

        assert config.name == "nvd"
        assert config.source_type == UpstreamSourceType.NVD
        assert config.priority == 100
        assert config.schema is None
        assert config.enrichment_function is None
        assert config.normalization_function is None

    def test_upstream_source_configuration_with_functions(self):
        """Test UpstreamSourceConfiguration with functions."""
        def mock_enrichment():
            pass

        def mock_normalization():
            pass

        schema = StructType([StructField("id", StringType(), False)])
        config = UpstreamSourceConfiguration(
            name="github",
            source_type=UpstreamSourceType.GITHUB_ADVISORY,
            priority=50,
            schema=schema,
            enrichment_function=mock_enrichment,
            normalization_function=mock_normalization,
        )

        assert config.schema == schema
        assert config.enrichment_function == mock_enrichment
        assert config.normalization_function == mock_normalization

    def test_upstream_sources_config(self):
        """Test creating an UpstreamSourcesConfig."""
        source1 = UpstreamSourceConfiguration(
            name="nvd",
            source_type=UpstreamSourceType.NVD,
            priority=100,
        )
        source2 = UpstreamSourceConfiguration(
            name="osv",
            source_type=UpstreamSourceType.OSV,
            priority=50,
        )

        config = UpstreamSourcesConfig(sources=[source1, source2])

        sources = list(config.sources)
        assert len(sources) == 2
        assert sources[0].name == "nvd"
        assert sources[1].name == "osv"

    def test_echo_status_as_string_comparison(self):
        """Test EchoStatus values can be used in string comparisons."""
        status = EchoStatus.FIXED
        assert status.value == "fixed"
        assert f"{status.value}" == "fixed"


class TestDependencies:
    """Tests for the Dependencies dataclass (without Spark)."""

    def test_dependencies_dataclass_structure(self):
        """Test that Dependencies has the expected attributes."""
        from src.advisory_pipeline.dependencies import Dependencies

        # Just verify the dataclass has the expected fields
        annotations = Dependencies.__annotations__
        assert "spark" in annotations
        assert "config" in annotations
