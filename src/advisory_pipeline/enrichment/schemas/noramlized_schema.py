from pyspark.sql.types import StructType, StructField, StringType, IntegerType

normalized_schema = StructType(
    [
        StructField("cve_id", StringType(), True),
        StructField("package", StringType(), True),
        StructField("status", StringType(), True),
        StructField("fixed_version", StringType(), True),
        StructField("priority", IntegerType(), True),
        StructField("internal_status", StringType(), True),
        StructField("enrichment_timestamp", StringType(), True),
    ]
)
