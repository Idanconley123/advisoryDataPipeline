"""
Unit tests for schema definitions.
"""

import pytest
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    BooleanType,
    TimestampType,
)

from src.advisory_pipeline.ingest.schemas.echo_advisory_schema import echo_advisory_schema
from src.advisory_pipeline.ingest.schemas.not_applicable_schema import not_applicable_schema
from src.advisory_pipeline.enrichment.schemas.cache_schema import enrichment_cache_schema
from src.advisory_pipeline.enrichment.schemas.noramlized_schema import normalized_schema
from src.advisory_pipeline.state_machine.schemas.state import mapped_state_schema, VALID_STATES


class TestEchoAdvisorySchema:
    """Tests for the echo_advisory_schema."""

    def test_schema_is_struct_type(self):
        """Test that schema is a StructType."""
        assert isinstance(echo_advisory_schema, StructType)

    def test_schema_has_expected_fields(self):
        """Test that schema has expected fields."""
        field_names = [f.name for f in echo_advisory_schema.fields]
        assert "package_name" in field_names
        assert "cve_id" in field_names
        assert "fixed_version" in field_names

    def test_cve_id_is_not_nullable(self):
        """Test that cve_id is not nullable."""
        cve_field = echo_advisory_schema["cve_id"]
        assert cve_field.nullable is False

    def test_package_name_is_nullable(self):
        """Test that package_name is nullable."""
        pkg_field = echo_advisory_schema["package_name"]
        assert pkg_field.nullable is True

    def test_fixed_version_is_nullable(self):
        """Test that fixed_version is nullable."""
        version_field = echo_advisory_schema["fixed_version"]
        assert version_field.nullable is True

    def test_all_fields_are_string_type(self):
        """Test that all fields are StringType."""
        for field in echo_advisory_schema.fields:
            assert isinstance(field.dataType, StringType)


class TestNotApplicableSchema:
    """Tests for the not_applicable_schema."""

    def test_schema_is_struct_type(self):
        """Test that schema is a StructType."""
        assert isinstance(not_applicable_schema, StructType)

    def test_schema_has_expected_fields(self):
        """Test that schema has expected fields."""
        field_names = [f.name for f in not_applicable_schema.fields]
        assert "cve_id" in field_names
        assert "package" in field_names
        assert "status" in field_names
        assert "fixed_version" in field_names
        assert "internal_status" in field_names

    def test_required_fields_not_nullable(self):
        """Test that required fields are not nullable."""
        cve_field = not_applicable_schema["cve_id"]
        assert cve_field.nullable is False

        pkg_field = not_applicable_schema["package"]
        assert pkg_field.nullable is False

        status_field = not_applicable_schema["status"]
        assert status_field.nullable is False

        internal_field = not_applicable_schema["internal_status"]
        assert internal_field.nullable is False

    def test_fixed_version_is_nullable(self):
        """Test that fixed_version is nullable."""
        version_field = not_applicable_schema["fixed_version"]
        assert version_field.nullable is True


class TestEnrichmentCacheSchema:
    """Tests for the enrichment_cache_schema."""

    def test_schema_is_struct_type(self):
        """Test that schema is a StructType."""
        assert isinstance(enrichment_cache_schema, StructType)

    def test_schema_has_expected_fields(self):
        """Test that schema has expected fields."""
        field_names = [f.name for f in enrichment_cache_schema.fields]
        assert "cve_id" in field_names
        assert "package_name" in field_names
        assert "source_name" in field_names
        assert "last_accessed" in field_names

    def test_cve_id_is_not_nullable(self):
        """Test that cve_id is not nullable."""
        cve_field = enrichment_cache_schema["cve_id"]
        assert cve_field.nullable is False

    def test_source_name_is_not_nullable(self):
        """Test that source_name is not nullable."""
        source_field = enrichment_cache_schema["source_name"]
        assert source_field.nullable is False

    def test_last_accessed_is_timestamp(self):
        """Test that last_accessed is TimestampType."""
        ts_field = enrichment_cache_schema["last_accessed"]
        assert isinstance(ts_field.dataType, TimestampType)
        assert ts_field.nullable is False


class TestNormalizedSchema:
    """Tests for the normalized_schema."""

    def test_schema_is_struct_type(self):
        """Test that schema is a StructType."""
        assert isinstance(normalized_schema, StructType)

    def test_schema_has_expected_fields(self):
        """Test that schema has expected fields."""
        field_names = [f.name for f in normalized_schema.fields]
        assert "cve_id" in field_names
        assert "package" in field_names
        assert "status" in field_names
        assert "fixed_version" in field_names
        assert "priority" in field_names
        assert "internal_status" in field_names
        assert "enrichment_timestamp" in field_names

    def test_priority_is_integer_type(self):
        """Test that priority is IntegerType."""
        priority_field = normalized_schema["priority"]
        assert isinstance(priority_field.dataType, IntegerType)

    def test_string_fields_are_nullable(self):
        """Test that string fields are nullable."""
        for field in normalized_schema.fields:
            if isinstance(field.dataType, StringType):
                assert field.nullable is True


class TestMappedStateSchema:
    """Tests for the mapped_state_schema."""

    def test_schema_is_struct_type(self):
        """Test that schema is a StructType."""
        assert isinstance(mapped_state_schema, StructType)

    def test_schema_has_expected_fields(self):
        """Test that schema has expected fields."""
        field_names = [f.name for f in mapped_state_schema.fields]
        expected_fields = [
            "cve_id",
            "package",
            "status",
            "previous_status",
            "fixed_version",
            "internal_status",
            "data_source",
            "priority",
            "enrichment_timestamp",
            "transition_valid",
            "transition_reason",
            "change_type",
        ]
        for expected in expected_fields:
            assert expected in field_names

    def test_cve_id_is_not_nullable(self):
        """Test that cve_id is not nullable."""
        cve_field = mapped_state_schema["cve_id"]
        assert cve_field.nullable is False

    def test_transition_valid_is_boolean(self):
        """Test that transition_valid is BooleanType."""
        tv_field = mapped_state_schema["transition_valid"]
        assert isinstance(tv_field.dataType, BooleanType)

    def test_priority_is_integer(self):
        """Test that priority is IntegerType."""
        priority_field = mapped_state_schema["priority"]
        assert isinstance(priority_field.dataType, IntegerType)


class TestValidStates:
    """Tests for the VALID_STATES constant."""

    def test_valid_states_list(self):
        """Test that VALID_STATES contains expected states."""
        expected_states = [
            "unknown",
            "pending_upstream",
            "fixed",
            "not_applicable",
            "will_not_fix",
        ]
        assert VALID_STATES == expected_states

    def test_valid_states_count(self):
        """Test that VALID_STATES has correct count."""
        assert len(VALID_STATES) == 5

    def test_valid_states_are_lowercase(self):
        """Test that all states are lowercase."""
        for state in VALID_STATES:
            assert state == state.lower()
