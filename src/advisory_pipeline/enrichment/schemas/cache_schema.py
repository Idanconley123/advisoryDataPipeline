from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
)

# Cache schema - tracks when each CVE was last enriched by each source
enrichment_cache_schema = StructType(
    [
        StructField("cve_id", StringType(), False),
        StructField("package_name", StringType(), True),
        StructField("source_name", StringType(), False),
        StructField("last_accessed", TimestampType(), False),
    ]
)
