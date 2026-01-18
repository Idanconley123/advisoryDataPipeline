def create_packages_to_enrich() -> str:
    """
    SQL query to identify CVEs that need upstream enrichment.

    Output columns match enrichment pipeline format:
        - cve_id: CVE identifier (e.g., "CVE-2024-3094")
        - package: Package name (e.g., "xz", "requests")
        - ecosystem: Package ecosystem (e.g., "pypi", "maven", "linux")
    """
    return """
        WITH current_advisory AS (
            -- This represents the ingested data.json
            SELECT cve_id, package_name, fixed_version
            FROM global_temp.raw_data
        ),
        manual_overrides AS (
            -- This represents the ingested CSV
            SELECT cve_id, package, internal_status as reason
            FROM global_temp.raw_not_applicable_cves
        ),
        current_state_before_enrichment AS (
            SELECT 
                a.cve_id,
                a.package_name AS package,
                CASE 
                    -- Priority 1: If it's in the CSV, it's Not Applicable
                    WHEN m.cve_id IS NOT NULL THEN 'not_applicable'
                    -- Priority 2: If it's already fixed in our JSON
                    WHEN a.fixed_version IS NOT NULL AND a.fixed_version != '' THEN 'fixed'
                    -- Priority 3: Otherwise, we need to ask the Upstream API
                    ELSE 'pending_upstream'
                END AS state
            FROM current_advisory a
            LEFT JOIN manual_overrides m ON lower(a.cve_id) = lower(m.cve_id) and lower(a.package_name) = lower(m.package)
        )
        -- Select only CVEs that need enrichment, with columns matching enrichment pipeline
        SELECT 
            DISTINCT
            cve_id,
            package
        FROM current_state_before_enrichment
        WHERE state = 'pending_upstream'
    """
