from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
)

# Schema for the not-applicable CSV
# Format: cve_id, package, status, fixed_version, internal_status
not_applicable_schema = StructType(
    [
        StructField("cve_id", StringType(), False),
        StructField("package", StringType(), False),
        StructField("status", StringType(), False),
        StructField("fixed_version", StringType(), True),
        StructField("internal_status", StringType(), False),
    ]
)
