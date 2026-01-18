from ..definitions import EchoStatus


def normalize_nvd(priority: int) -> str:
    """
    Generate SQL query to normalize NVD enrichment data.

    Derives echo_state and echo_state_explanation based on:
    - nvd_fixed_version (if exists -> fixed, regardless of status)
    - nvd_status (Rejected -> not_applicable, others -> pending_upstream)

    Args:
        priority: Priority for this source in conflict resolution

    Returns:
        SQL query string
    """
    return f"""
    SELECT
        cve_id,
        package,
        nvd_fixed_version AS fixed_version,
        
        -- Echo State derivation: fixed version takes priority (except Rejected)
        CASE
            WHEN nvd_status = 'Rejected' THEN '{EchoStatus.NOT_APPLICABLE.value}'
            WHEN nvd_fixed_version IS NOT NULL THEN '{EchoStatus.FIXED.value}'
            WHEN nvd_status = 'Analyzed' THEN '{EchoStatus.PENDING.value}'
            WHEN nvd_status = 'Awaiting Analysis' THEN '{EchoStatus.PENDING.value}'
            WHEN nvd_status = 'Undergoing Analysis' THEN '{EchoStatus.PENDING.value}'
            WHEN nvd_status = 'Modified' THEN '{EchoStatus.PENDING.value}'
            ELSE '{EchoStatus.PENDING.value}'
        END AS internal_status,
        
        -- Customer-facing explanation
        CASE
            WHEN nvd_status = 'Rejected' 
                THEN 'NVD has marked this record as invalid or retracted. No action required.'
            WHEN nvd_fixed_version IS NOT NULL 
                THEN CONCAT('Fix version identified: ', nvd_fixed_version, '. Update to this version or later.')
            WHEN nvd_status = 'Analyzed' 
                THEN 'Analysis complete, but no fix has been officially released by the maintainers.'
            WHEN nvd_status = 'Awaiting Analysis' 
                THEN 'CVE is published, but NVD enrichment (CPE mapping) is still in progress.'
            WHEN nvd_status = 'Undergoing Analysis' 
                THEN 'NIST analysts are currently verifying the affected and fixed versions.'
            WHEN nvd_status = 'Modified' 
                THEN 'CVE metadata was recently updated; no fix version identified yet.'
            ELSE CONCAT('Unknown NVD status: ', COALESCE(nvd_status, 'NULL'), '. Manual review may be required.')
        END AS status,
        
        {priority} AS priority,
        nvd_query_timestamp AS enrichment_timestamp
        
    FROM global_temp.raw_nvd
    WHERE nvd_found = true
    """
