#!/bin/bash
set -e

echo "Loading CSV data into PostgreSQL..."

# Check if CSV exists
if [ ! -f /tmp/advisory_not_applicable.csv ]; then
    echo "Warning: CSV file not found at /tmp/advisory_not_applicable.csv"
    echo "Skipping data load"
    exit 0
fi

# Check if CSV is empty or only has header
line_count=$(wc -l < /tmp/advisory_not_applicable.csv)
if [ "$line_count" -le 1 ]; then
    echo "Warning: CSV file is empty or only contains header"
    echo "Skipping data load"
    exit 0
fi

# Load CSV into PostgreSQL
echo "Loading data from advisory_not_applicable.csv..."

# First, load into temp table
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE TEMP TABLE temp_load (
        cve_id VARCHAR(50),
        package VARCHAR(255),
        status VARCHAR(50),
        fixed_version VARCHAR(50),
        internal_status VARCHAR(50)
    );
    
    \copy temp_load(cve_id, package, status, fixed_version, internal_status) FROM '/tmp/advisory_not_applicable.csv' DELIMITER ',' CSV HEADER;
    
    INSERT INTO advisory.not_applicable_cves (cve_id, package, ecosystem, status, fixed_version, internal_status)
    SELECT DISTINCT ON (cve_id) cve_id, package, NULL as ecosystem, status, fixed_version, internal_status
    FROM temp_load
    ON CONFLICT (cve_id) DO NOTHING;
    
    SELECT COUNT(*) as loaded_rows FROM advisory.not_applicable_cves;
EOSQL

echo "✅ CSV data loaded successfully!"
