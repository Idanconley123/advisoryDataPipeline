def upsert_data() -> str:
    """
    Upsert processed state into production.

    The processed state (global_temp.processed_cve_state_machine) already contains:
    - Validated state transitions (via UDFs)
    - Merged data from enrichment + production + echo
    - Change classification (new, status_changed, blocked, unchanged)

    This query:
    1. Takes all rows from processed state (already deduplicated by CVE+package)
    2. For entries not in processed state but in prod, keeps them unchanged
    3. Outputs the final merged state for writing to production

    Returns:
        SQL query string for upserting to production
    """
    return """
        -- Get the processed state (new/updated entries)
        WITH processed AS (
            SELECT
                cve_id,
                package,
                status,
                previous_status,
                fixed_version,
                internal_status,
                data_source,
                priority,
                enrichment_timestamp,
                transition_valid,
                transition_reason,
                change_type
            FROM global_temp.processed_cve_state_machine
        ),
        
        -- Get current production state
        prod AS (
            SELECT
                cve_id,
                package,
                status,
                previous_status,
                fixed_version,
                internal_status,
                data_source,
                priority,
                enrichment_timestamp,
                transition_valid,
                transition_reason,
                change_type
            FROM global_temp.prod_cve_state_machine
        ),
        
        -- Entries only in production (not touched by this run)
        prod_only AS (
            SELECT p.*
            FROM prod p
            LEFT JOIN processed pr ON p.cve_id = pr.cve_id AND p.package = pr.package
            WHERE pr.cve_id IS NULL
        ),
        
        -- Combine: processed takes priority, then prod-only entries
        combined AS (
            SELECT * FROM processed
            UNION ALL
            SELECT * FROM prod_only
        )
        
        -- Final deduplicated output
        SELECT
            cve_id,
            package,
            status,
            previous_status,
            fixed_version,
            internal_status,
            data_source,
            priority,
            enrichment_timestamp,
            transition_valid,
            transition_reason,
            change_type
        FROM combined
        ORDER BY cve_id, package
    """
