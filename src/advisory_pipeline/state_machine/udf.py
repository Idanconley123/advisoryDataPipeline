from ..dependencies import Dependencies
from .state_transitions import (
    is_valid_transition,
    apply_transition,
    get_transition_explanation,
)
import logging

logger = logging.getLogger(__name__)


def register_state_machine_udfs(deps: Dependencies):
    """
    Register state machine UDFs for use in Spark SQL.

    Registered UDFs:
    - is_valid_transition(from_state, to_state) -> boolean
    - apply_transition(from_state, to_state) -> string (new_state)
    - get_transition_explanation(from_state, to_state) -> string
    """
    from pyspark.sql.functions import udf
    from pyspark.sql.types import StringType, BooleanType, StructType, StructField
    from typing import Optional

    # UDF 1: Check if transition is valid
    @udf(returnType=BooleanType())
    def is_valid_transition_udf(
        from_state: Optional[str], to_state: Optional[str]
    ) -> bool:
        if from_state is None:
            from_state = "unknown"
        if to_state is None:
            return False
        return is_valid_transition(from_state, to_state)

    # UDF 2: Apply transition and return new state (keeps old if invalid)
    @udf(returnType=StringType())
    def apply_transition_udf(from_state: Optional[str], to_state: Optional[str]) -> str:
        if from_state is None:
            from_state = "unknown"
        if to_state is None:
            return from_state
        result = apply_transition(from_state, to_state)
        return result.new_state

    # UDF 3: Get customer-facing explanation for transition
    @udf(returnType=StringType())
    def get_transition_explanation_udf(
        from_state: Optional[str], to_state: Optional[str]
    ) -> str:
        if from_state is None:
            from_state = "unknown"
        if to_state is None:
            return "No state change proposed"
        return get_transition_explanation(from_state, to_state)

    # UDF 4: Full transition result with all fields
    transition_result_schema = StructType(
        [
            StructField("success", BooleanType(), False),
            StructField("old_state", StringType(), True),
            StructField("new_state", StringType(), True),
            StructField("reason", StringType(), True),
        ]
    )

    @udf(returnType=transition_result_schema)
    def full_transition_udf(from_state: Optional[str], to_state: Optional[str]):
        if from_state is None:
            from_state = "unknown"
        if to_state is None:
            to_state = from_state
        result = apply_transition(from_state, to_state)
        return (result.success, result.old_state, result.new_state, result.reason)

    # Register UDFs with Spark SQL
    deps.spark.udf.register("is_valid_transition", is_valid_transition_udf)
    deps.spark.udf.register("apply_transition", apply_transition_udf)
    deps.spark.udf.register(
        "get_transition_explanation", get_transition_explanation_udf
    )
    deps.spark.udf.register("full_transition", full_transition_udf)

    logger.info(
        "Registered state machine UDFs: is_valid_transition, apply_transition, get_transition_explanation, full_transition"
    )
