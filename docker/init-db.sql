-- Initialize advisory database schema

-- Create schema
CREATE SCHEMA IF NOT EXISTS advisory;

-- Create not_applicable_cves table
CREATE TABLE IF NOT EXISTS advisory.not_applicable_cves (
    cve_id VARCHAR(50) PRIMARY KEY,
    package VARCHAR(255) NOT NULL,
    ecosystem VARCHAR(50),
    status VARCHAR(50) NOT NULL,
    fixed_version VARCHAR(50),
    internal_status VARCHAR(50) NOT NULL,
    reason TEXT,
    source VARCHAR(100),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create index for faster lookups
CREATE INDEX IF NOT EXISTS idx_not_applicable_package ON advisory.not_applicable_cves(package);
CREATE INDEX IF NOT EXISTS idx_not_applicable_ecosystem ON advisory.not_applicable_cves(ecosystem);
CREATE INDEX IF NOT EXISTS idx_not_applicable_status ON advisory.not_applicable_cves(status);

-- Grant permissions
GRANT ALL PRIVILEGES ON SCHEMA advisory TO advisory_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA advisory TO advisory_user;

COMMENT ON TABLE advisory.not_applicable_cves IS 'CVEs marked as not applicable or with manual overrides';
