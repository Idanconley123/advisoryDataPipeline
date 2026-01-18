from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    BooleanType,
)


# Valid states for the advisory state machine
# States: unknown, pending_upstream, fixed, not_applicable, will_not_fix
VALID_STATES = [
    "unknown",
    "pending_upstream",
    "fixed",
    "not_applicable",
    "will_not_fix",
]


# Output schema for mapped state
mapped_state_schema = StructType(
    [
        StructField("cve_id", StringType(), False),
        StructField("package", StringType(), True),
        StructField("status", StringType(), True),  # Current state after transition
        StructField("previous_status", StringType(), True),  # State before transition
        StructField("fixed_version", StringType(), True),
        StructField(
            "internal_status", StringType(), True
        ),  # Customer-facing explanation
        StructField("data_source", StringType(), True),  # Where the data came from
        StructField("priority", IntegerType(), True),
        StructField("enrichment_timestamp", StringType(), True),
        StructField(
            "transition_valid", BooleanType(), True
        ),  # Was the transition valid?
        StructField(
            "transition_reason", StringType(), True
        ),  # Why transition succeeded/failed
        StructField(
            "change_type", StringType(), True
        ),  # new, status_changed, unchanged, etc.
    ]
)
