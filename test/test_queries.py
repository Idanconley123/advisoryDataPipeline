"""
Unit tests for SQL query generator functions.
"""

import pytest
from src.advisory_pipeline.enrichment.queries.packages_to_enrich import create_packages_to_enrich
from src.advisory_pipeline.enrichment.queries.nvd_normalization import normalize_nvd
from src.advisory_pipeline.state_machine.queries.map_new_info_with_udf import map_new_info_with_udf
from src.advisory_pipeline.state_machine.queries.upsert_data import upsert_data


class TestPackagesToEnrichQuery:
    """Tests for the packages_to_enrich query generator."""

    def test_create_packages_to_enrich_returns_string(self):
        """Test that create_packages_to_enrich returns a SQL string."""
        query = create_packages_to_enrich()
        assert isinstance(query, str)
        assert len(query) > 0

    def test_query_references_raw_data(self):
        """Test that query references the raw_data temp view."""
        query = create_packages_to_enrich()
        assert "global_temp.raw_data" in query

    def test_query_references_raw_not_applicable_cves(self):
        """Test that query references the not_applicable_cves temp view."""
        query = create_packages_to_enrich()
        assert "global_temp.raw_not_applicable_cves" in query

    def test_query_selects_cve_id_and_package(self):
        """Test that query selects cve_id and package columns."""
        query = create_packages_to_enrich()
        assert "cve_id" in query
        assert "package" in query

    def test_query_filters_pending_upstream(self):
        """Test that query filters for pending_upstream state."""
        query = create_packages_to_enrich()
        assert "pending_upstream" in query

    def test_query_handles_state_priority(self):
        """Test that query handles state priority correctly."""
        query = create_packages_to_enrich()
        # Should check for not_applicable first (manual override)
        assert "not_applicable" in query
        # Should check for fixed next
        assert "fixed" in query

    def test_query_is_valid_sql_syntax(self):
        """Test that query has valid SQL structure."""
        query = create_packages_to_enrich()
        # Basic SQL keywords check
        assert "SELECT" in query.upper()
        assert "FROM" in query.upper()
        assert "WHERE" in query.upper()
        assert "WITH" in query.upper()  # Uses CTEs


class TestNvdNormalizationQuery:
    """Tests for the NVD normalization query generator."""

    def test_normalize_nvd_returns_string(self):
        """Test that normalize_nvd returns a SQL string."""
        query = normalize_nvd(priority=100)
        assert isinstance(query, str)
        assert len(query) > 0

    def test_query_includes_priority(self):
        """Test that query includes the provided priority."""
        query = normalize_nvd(priority=100)
        assert "100" in query

        query2 = normalize_nvd(priority=50)
        assert "50" in query2

    def test_query_references_raw_nvd(self):
        """Test that query references the raw_nvd temp view."""
        query = normalize_nvd(priority=100)
        assert "global_temp.raw_nvd" in query

    def test_query_selects_expected_columns(self):
        """Test that query selects expected output columns."""
        query = normalize_nvd(priority=100)
        assert "cve_id" in query
        assert "package" in query
        assert "fixed_version" in query
        assert "internal_status" in query
        assert "status" in query  # Customer explanation
        assert "priority" in query
        assert "enrichment_timestamp" in query

    def test_query_handles_nvd_status_rejected(self):
        """Test that query handles Rejected NVD status."""
        query = normalize_nvd(priority=100)
        assert "Rejected" in query
        assert "not_applicable" in query

    def test_query_handles_nvd_status_analyzed(self):
        """Test that query handles Analyzed NVD status."""
        query = normalize_nvd(priority=100)
        assert "Analyzed" in query

    def test_query_handles_nvd_status_awaiting_analysis(self):
        """Test that query handles Awaiting Analysis status."""
        query = normalize_nvd(priority=100)
        assert "Awaiting Analysis" in query

    def test_query_handles_fixed_version(self):
        """Test that query handles fixed versions correctly."""
        query = normalize_nvd(priority=100)
        assert "nvd_fixed_version" in query

    def test_query_filters_nvd_found(self):
        """Test that query filters for nvd_found = true."""
        query = normalize_nvd(priority=100)
        assert "nvd_found = true" in query


class TestMapNewInfoWithUdfQuery:
    """Tests for the map_new_info_with_udf query generator."""

    def test_map_new_info_returns_string(self):
        """Test that map_new_info_with_udf returns a SQL string."""
        query = map_new_info_with_udf()
        assert isinstance(query, str)
        assert len(query) > 0

    def test_query_references_prod_state(self):
        """Test that query references production state view."""
        query = map_new_info_with_udf()
        assert "global_temp.prod_cve_state_machine" in query

    def test_query_references_normalized_enrichment(self):
        """Test that query references normalized enrichment view."""
        query = map_new_info_with_udf()
        assert "global_temp.normalized_enrichment" in query

    def test_query_references_raw_data(self):
        """Test that query references raw_data view."""
        query = map_new_info_with_udf()
        assert "global_temp.raw_data" in query

    def test_query_uses_state_transition_udfs(self):
        """Test that query uses registered UDFs."""
        query = map_new_info_with_udf()
        assert "apply_transition" in query
        assert "is_valid_transition" in query
        assert "get_transition_explanation" in query

    def test_query_has_expected_output_columns(self):
        """Test that query outputs expected columns."""
        query = map_new_info_with_udf()
        assert "cve_id" in query
        assert "package" in query
        assert "status" in query
        assert "previous_status" in query
        assert "fixed_version" in query
        assert "internal_status" in query
        assert "data_source" in query
        assert "priority" in query
        assert "transition_valid" in query
        assert "transition_reason" in query
        assert "change_type" in query

    def test_query_classifies_change_types(self):
        """Test that query classifies change types."""
        query = map_new_info_with_udf()
        assert "'new'" in query
        assert "'blocked'" in query
        assert "'status_changed'" in query
        assert "'enriched_unchanged'" in query or "'unchanged'" in query

    def test_query_uses_ctes(self):
        """Test that query uses Common Table Expressions."""
        query = map_new_info_with_udf()
        assert "WITH" in query.upper()
        assert "prod_state AS" in query
        assert "enrichment_ranked AS" in query or "new_enrichment AS" in query

    def test_query_ranks_by_priority(self):
        """Test that query ranks enrichment by priority."""
        query = map_new_info_with_udf()
        assert "ROW_NUMBER()" in query.upper()
        assert "ORDER BY priority" in query


class TestUpsertDataQuery:
    """Tests for the upsert_data query generator."""

    def test_upsert_data_returns_string(self):
        """Test that upsert_data returns a SQL string."""
        query = upsert_data()
        assert isinstance(query, str)
        assert len(query) > 0

    def test_query_references_processed_state(self):
        """Test that query references processed state view."""
        query = upsert_data()
        assert "global_temp.processed_cve_state_machine" in query

    def test_query_references_prod_state(self):
        """Test that query references production state view."""
        query = upsert_data()
        assert "global_temp.prod_cve_state_machine" in query

    def test_query_performs_union(self):
        """Test that query performs union for upsert."""
        query = upsert_data()
        assert "UNION ALL" in query.upper()

    def test_query_handles_prod_only_entries(self):
        """Test that query preserves entries only in production."""
        query = upsert_data()
        assert "prod_only AS" in query
        assert "LEFT JOIN" in query.upper()

    def test_query_outputs_expected_columns(self):
        """Test that query outputs expected columns."""
        query = upsert_data()
        expected_columns = [
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
        for col in expected_columns:
            assert col in query

    def test_query_orders_result(self):
        """Test that query orders the result."""
        query = upsert_data()
        assert "ORDER BY" in query.upper()
