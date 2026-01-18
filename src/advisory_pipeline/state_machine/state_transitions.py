from enum import Enum
from typing import Optional
from dataclasses import dataclass


class AdvisoryState(str, Enum):
    """Valid advisory states."""

    UNKNOWN = "unknown"
    PENDING_UPSTREAM = "pending_upstream"
    FIXED = "fixed"
    NOT_APPLICABLE = "not_applicable"
    WILL_NOT_FIX = "will_not_fix"


# Define valid transitions: {from_state: [valid_to_states]}
VALID_TRANSITIONS = {
    AdvisoryState.UNKNOWN: [
        AdvisoryState.PENDING_UPSTREAM,
        AdvisoryState.FIXED,
    ],
    AdvisoryState.PENDING_UPSTREAM: [
        AdvisoryState.FIXED,
        AdvisoryState.NOT_APPLICABLE,
        AdvisoryState.WILL_NOT_FIX,
    ],
    # Terminal states - no transitions out
    AdvisoryState.FIXED: [],
    AdvisoryState.NOT_APPLICABLE: [],
    AdvisoryState.WILL_NOT_FIX: [],
}

# Terminal states that cannot be changed
TERMINAL_STATES = {
    AdvisoryState.FIXED,
    AdvisoryState.NOT_APPLICABLE,
    AdvisoryState.WILL_NOT_FIX,
}


@dataclass
class TransitionResult:
    """Result of a state transition attempt."""

    success: bool
    old_state: str
    new_state: str
    reason: str


def is_valid_transition(from_state: str, to_state: str) -> bool:
    """
    Check if a state transition is valid.

    Args:
        from_state: Current state
        to_state: Proposed new state

    Returns:
        True if transition is valid, False otherwise
    """
    try:
        from_enum = AdvisoryState(from_state)
        to_enum = AdvisoryState(to_state)
    except ValueError:
        return False

    # Same state is always valid (no change)
    if from_enum == to_enum:
        return True

    # Check if transition is in valid transitions list
    return to_enum in VALID_TRANSITIONS.get(from_enum, [])


def apply_transition(
    current_state: Optional[str],
    proposed_state: str,
    allow_invalid: bool = False,
) -> TransitionResult:
    """
    Apply a state transition with validation.

    Args:
        current_state: Current state (None = unknown)
        proposed_state: Proposed new state
        allow_invalid: If True, log warning but allow invalid transitions

    Returns:
        TransitionResult with success status and reason
    """
    # Handle None/empty as unknown
    if current_state is None or current_state == "":
        current_state = AdvisoryState.UNKNOWN.value

    # Normalize states
    current_state = current_state.lower().strip()
    proposed_state = proposed_state.lower().strip()

    # Same state - no change needed
    if current_state == proposed_state:
        return TransitionResult(
            success=True,
            old_state=current_state,
            new_state=proposed_state,
            reason="No change required",
        )

    # Check if current state is terminal
    try:
        current_enum = AdvisoryState(current_state)
        if current_enum in TERMINAL_STATES:
            return TransitionResult(
                success=False,
                old_state=current_state,
                new_state=current_state,  # Keep current state
                reason=f"Cannot transition from terminal state '{current_state}'",
            )
    except ValueError:
        pass  # Unknown state, will be handled below

    # Validate transition
    if is_valid_transition(current_state, proposed_state):
        return TransitionResult(
            success=True,
            old_state=current_state,
            new_state=proposed_state,
            reason=f"Valid transition: {current_state} -> {proposed_state}",
        )
    else:
        if allow_invalid:
            return TransitionResult(
                success=True,
                old_state=current_state,
                new_state=proposed_state,
                reason=f"WARNING: Invalid transition allowed: {current_state} -> {proposed_state}",
            )
        else:
            return TransitionResult(
                success=False,
                old_state=current_state,
                new_state=current_state,  # Keep current state
                reason=f"Invalid transition: {current_state} -> {proposed_state}",
            )


def get_transition_explanation(from_state: str, to_state: str) -> str:
    """
    Get customer-facing explanation for a state transition.

    Args:
        from_state: Previous state
        to_state: New state

    Returns:
        Human-readable explanation
    """
    explanations = {
        (
            "unknown",
            "pending_upstream",
        ): "CVE identified. Awaiting fix from upstream maintainer.",
        ("unknown", "fixed"): "CVE identified with fix already available.",
        (
            "pending_upstream",
            "fixed",
        ): "Fix version has been released by upstream maintainer.",
        (
            "pending_upstream",
            "not_applicable",
        ): "After analysis, this CVE does not apply to your context.",
        (
            "pending_upstream",
            "will_not_fix",
        ): "Upstream maintainer has decided not to fix this vulnerability.",
    }

    key = (from_state.lower(), to_state.lower())
    return explanations.get(key, f"State changed from {from_state} to {to_state}")


# ============================================================================
# Spark UDF for state transitions
# ============================================================================


def create_transition_udf():
    """
    Create a Spark UDF for applying state transitions.

    Usage:
        from pyspark.sql.functions import udf
        transition_udf = create_transition_udf()
        df = df.withColumn("new_state", transition_udf(col("current_state"), col("proposed_state")))
    """
    from pyspark.sql.functions import udf
    from pyspark.sql.types import StructType, StructField, StringType, BooleanType

    # Define return schema
    result_schema = StructType(
        [
            StructField("success", BooleanType(), False),
            StructField("old_state", StringType(), True),
            StructField("new_state", StringType(), True),
            StructField("reason", StringType(), True),
        ]
    )

    def _transition_udf(current_state: Optional[str], proposed_state: str):
        result = apply_transition(current_state, proposed_state)
        return (result.success, result.old_state, result.new_state, result.reason)

    return udf(_transition_udf, result_schema)


def create_simple_transition_udf():
    """
    Create a simple Spark UDF that returns only the new state.

    Returns current state if transition is invalid.
    """
    from pyspark.sql.functions import udf
    from pyspark.sql.types import StringType

    def _simple_transition(current_state: Optional[str], proposed_state: str) -> str:
        result = apply_transition(current_state, proposed_state)
        return result.new_state

    return udf(_simple_transition, StringType())
