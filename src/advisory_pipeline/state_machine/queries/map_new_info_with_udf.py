def map_new_info_with_udf() -> str:
    """
    Generate SQL query that uses UDFs for state transition validation.

    Prerequisites (loaded as global temp views):
        - prod_cve_state_machine: Current production state from PostgreSQL/parquet
        - normalized_enrichment: New enrichment data (NVD, etc.)
        - raw_data: Echo advisory data.json

    The UDFs must be registered before running this query:
        register_state_machine_udfs(deps)

    Returns:
        SQL query string using registered UDFs
    """
    return """
        -- Step 1: Get current production state
        WITH prod_state AS (
            SELECT
                cve_id,
                package,
                COALESCE(status, 'unknown') AS prod_status,
                fixed_version AS prod_fixed_version,
                internal_status AS prod_internal_status,
                priority AS prod_priority,
                enrichment_timestamp AS prod_enrichment_timestamp
            FROM global_temp.prod_cve_state_machine
        ),
        
        -- Step 2: Get best enrichment result by priority
        -- NOTE: In normalized_enrichment schema:
        --   internal_status = echo state (fixed, pending_upstream, etc.)
        --   status = customer-facing explanation text
        enrichment_ranked AS (
            SELECT
                cve_id,
                package,
                internal_status AS enrichment_status,  -- Echo state for transitions
                fixed_version AS enrichment_fixed_version,
                status AS enrichment_internal_status,  -- Customer explanation
                priority AS enrichment_priority,
                enrichment_timestamp,
                ROW_NUMBER() OVER (
                    PARTITION BY cve_id, package 
                    ORDER BY priority DESC, enrichment_timestamp DESC
                ) AS rn
            FROM global_temp.normalized_enrichment
        ),
        new_enrichment AS (
            SELECT * FROM enrichment_ranked WHERE rn = 1
        ),
        
        -- Step 3: Get Echo advisory data (baseline CVE list)
        echo_advisory AS (
            SELECT
                cve_id,
                package_name AS package,
                fixed_version AS echo_fixed_version
            FROM global_temp.raw_data
        ),
        
        -- Step 4: Combine all sources - echo is the master list
        combined AS (
            SELECT
                -- Use echo as the source of truth for CVE/package pairs
                e.cve_id,
                e.package,
                
                -- Previous status from production (default unknown for new entries)
                COALESCE(p.prod_status, 'unknown') AS previous_status,
                
                -- Proposed status: enrichment > production > default
                COALESCE(
                    n.enrichment_status,
                    p.prod_status,
                    'pending_upstream'
                ) AS proposed_status,
                
                -- Fixed version priority: enrichment > production > echo
                COALESCE(
                    n.enrichment_fixed_version,
                    p.prod_fixed_version,
                    e.echo_fixed_version
                ) AS fixed_version,
                
                -- Internal status: enrichment > production > default
                COALESCE(
                    n.enrichment_internal_status,
                    p.prod_internal_status,
                    'CVE identified. Awaiting analysis.'
                ) AS internal_status,
                
                -- Data source tracking
                CASE
                    WHEN n.cve_id IS NOT NULL THEN 'enrichment'
                    WHEN p.cve_id IS NOT NULL THEN 'production'
                    ELSE 'echo_advisory'
                END AS data_source,
                
                -- Priority: use enrichment priority if available
                COALESCE(n.enrichment_priority, p.prod_priority, 0) AS priority,
                
                -- Timestamp: use enrichment timestamp if available
                COALESCE(n.enrichment_timestamp, p.prod_enrichment_timestamp) AS enrichment_timestamp,
                
                -- Flags for change detection
                CASE WHEN n.cve_id IS NOT NULL THEN true ELSE false END AS has_new_enrichment,
                CASE WHEN p.cve_id IS NOT NULL THEN true ELSE false END AS exists_in_prod
                
            FROM echo_advisory e
            LEFT JOIN new_enrichment n 
                ON e.cve_id = n.cve_id AND e.package = n.package
            LEFT JOIN prod_state p 
                ON e.cve_id = p.cve_id AND e.package = p.package
        ),
        
        -- Step 5: Apply UDF-based state transitions
        with_transitions AS (
            SELECT
                cve_id,
                package,
                previous_status,
                proposed_status,
                
                -- UDF: Validate and apply transition (returns new state, keeps old if invalid)
                apply_transition(previous_status, proposed_status) AS status,
                
                -- UDF: Check if transition was valid
                is_valid_transition(previous_status, proposed_status) AS transition_valid,
                
                -- UDF: Get customer-facing explanation for transition
                get_transition_explanation(previous_status, proposed_status) AS transition_reason,
                
                fixed_version,
                internal_status,
                data_source,
                priority,
                enrichment_timestamp,
                has_new_enrichment,
                exists_in_prod
                
            FROM combined
        )
        
        -- Step 6: Final output with change classification
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
            
            -- Classify the type of change
            CASE
                WHEN NOT exists_in_prod THEN 'new'
                WHEN NOT transition_valid THEN 'blocked'
                WHEN has_new_enrichment AND previous_status != status THEN 'status_changed'
                WHEN has_new_enrichment THEN 'enriched_unchanged'
                ELSE 'unchanged'
            END AS change_type
            
        FROM with_transitions
        WHERE cve_id IS NOT NULL
        ORDER BY 
            CASE change_type 
                WHEN 'new' THEN 1
                WHEN 'status_changed' THEN 2
                WHEN 'blocked' THEN 3
                WHEN 'enriched_unchanged' THEN 4
                ELSE 5
            END,
            cve_id
    """
