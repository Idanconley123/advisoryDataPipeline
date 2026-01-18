from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    BooleanType,
)

# Simplified NVD schema - only fields used in normalization
raw_nvd_schema = StructType(
    [
        # Identifiers
        StructField("cve_id", StringType(), False),
        StructField("package", StringType(), True),
        
        # Response status
        StructField("nvd_found", BooleanType(), False),
        
        # Fields used in state derivation
        StructField("nvd_fixed_version", StringType(), True),  # versionEndExcluding from CPE
        StructField("nvd_status", StringType(), True),  # Analyzed, Modified, Rejected, etc.
        
        # Query metadata
        StructField("nvd_query_timestamp", StringType(), False),  # ISO 8601
    ]
)
