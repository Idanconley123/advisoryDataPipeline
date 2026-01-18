from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
)

# Schema for the Echo advisory data.json
echo_advisory_schema = StructType(
    [
        StructField("package_name", StringType(), True),
        StructField("cve_id", StringType(), False),
        StructField("fixed_version", StringType(), True),
    ]
)
