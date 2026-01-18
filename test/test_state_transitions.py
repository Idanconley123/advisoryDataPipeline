"""
Unit tests for the state_transitions module.
"""

import pytest
from src.advisory_pipeline.state_machine.state_transitions import (
    AdvisoryState,
    VALID_TRANSITIONS,
    TERMINAL_STATES,
    TransitionResult,
    is_valid_transition,
    apply_transition,
    get_transition_explanation,
)


class TestAdvisoryState:
    """Tests for the AdvisoryState enum."""

    def test_advisory_state_values(self):
        """Test that all expected states exist with correct values."""
        assert AdvisoryState.UNKNOWN.value == "unknown"
        assert AdvisoryState.PENDING_UPSTREAM.value == "pending_upstream"
        assert AdvisoryState.FIXED.value == "fixed"
        assert AdvisoryState.NOT_APPLICABLE.value == "not_applicable"
        assert AdvisoryState.WILL_NOT_FIX.value == "will_not_fix"

    def test_advisory_state_is_string_enum(self):
        """Test that AdvisoryState inherits from str."""
        assert isinstance(AdvisoryState.UNKNOWN, str)
        assert AdvisoryState.UNKNOWN == "unknown"


class TestValidTransitions:
    """Tests for the VALID_TRANSITIONS mapping."""

    def test_unknown_transitions(self):
        """Test valid transitions from unknown state."""
        valid_from_unknown = VALID_TRANSITIONS[AdvisoryState.UNKNOWN]
        assert AdvisoryState.PENDING_UPSTREAM in valid_from_unknown
        assert AdvisoryState.FIXED in valid_from_unknown
        assert len(valid_from_unknown) == 2

    def test_pending_upstream_transitions(self):
        """Test valid transitions from pending_upstream state."""
        valid_from_pending = VALID_TRANSITIONS[AdvisoryState.PENDING_UPSTREAM]
        assert AdvisoryState.FIXED in valid_from_pending
        assert AdvisoryState.NOT_APPLICABLE in valid_from_pending
        assert AdvisoryState.WILL_NOT_FIX in valid_from_pending
        assert len(valid_from_pending) == 3

    def test_terminal_states_have_no_transitions(self):
        """Test that terminal states have no valid transitions."""
        assert VALID_TRANSITIONS[AdvisoryState.FIXED] == []
        assert VALID_TRANSITIONS[AdvisoryState.NOT_APPLICABLE] == []
        assert VALID_TRANSITIONS[AdvisoryState.WILL_NOT_FIX] == []


class TestTerminalStates:
    """Tests for the TERMINAL_STATES set."""

    def test_terminal_states_content(self):
        """Test that terminal states are correctly defined."""
        assert AdvisoryState.FIXED in TERMINAL_STATES
        assert AdvisoryState.NOT_APPLICABLE in TERMINAL_STATES
        assert AdvisoryState.WILL_NOT_FIX in TERMINAL_STATES
        assert len(TERMINAL_STATES) == 3

    def test_non_terminal_states_not_in_terminal_states(self):
        """Test that non-terminal states are not in TERMINAL_STATES."""
        assert AdvisoryState.UNKNOWN not in TERMINAL_STATES
        assert AdvisoryState.PENDING_UPSTREAM not in TERMINAL_STATES


class TestTransitionResult:
    """Tests for the TransitionResult dataclass."""

    def test_transition_result_creation(self):
        """Test creating a TransitionResult."""
        result = TransitionResult(
            success=True,
            old_state="unknown",
            new_state="pending_upstream",
            reason="Valid transition",
        )

        assert result.success is True
        assert result.old_state == "unknown"
        assert result.new_state == "pending_upstream"
        assert result.reason == "Valid transition"

    def test_transition_result_failed(self):
        """Test creating a failed TransitionResult."""
        result = TransitionResult(
            success=False,
            old_state="fixed",
            new_state="fixed",
            reason="Cannot transition from terminal state",
        )

        assert result.success is False
        assert result.old_state == "fixed"
        assert result.new_state == "fixed"


class TestIsValidTransition:
    """Tests for the is_valid_transition function."""

    def test_valid_unknown_to_pending_upstream(self):
        """Test valid transition from unknown to pending_upstream."""
        assert is_valid_transition("unknown", "pending_upstream") is True

    def test_valid_unknown_to_fixed(self):
        """Test valid transition from unknown to fixed."""
        assert is_valid_transition("unknown", "fixed") is True

    def test_valid_pending_upstream_to_fixed(self):
        """Test valid transition from pending_upstream to fixed."""
        assert is_valid_transition("pending_upstream", "fixed") is True

    def test_valid_pending_upstream_to_not_applicable(self):
        """Test valid transition from pending_upstream to not_applicable."""
        assert is_valid_transition("pending_upstream", "not_applicable") is True

    def test_valid_pending_upstream_to_will_not_fix(self):
        """Test valid transition from pending_upstream to will_not_fix."""
        assert is_valid_transition("pending_upstream", "will_not_fix") is True

    def test_same_state_is_valid(self):
        """Test that transitioning to the same state is valid."""
        assert is_valid_transition("unknown", "unknown") is True
        assert is_valid_transition("fixed", "fixed") is True
        assert is_valid_transition("pending_upstream", "pending_upstream") is True

    def test_invalid_transition_from_terminal_state(self):
        """Test that transitions from terminal states are invalid."""
        assert is_valid_transition("fixed", "pending_upstream") is False
        assert is_valid_transition("not_applicable", "fixed") is False
        assert is_valid_transition("will_not_fix", "pending_upstream") is False

    def test_invalid_transition_unknown_to_not_applicable(self):
        """Test that unknown cannot directly go to not_applicable."""
        assert is_valid_transition("unknown", "not_applicable") is False

    def test_invalid_transition_unknown_to_will_not_fix(self):
        """Test that unknown cannot directly go to will_not_fix."""
        assert is_valid_transition("unknown", "will_not_fix") is False

    def test_invalid_state_values(self):
        """Test handling of invalid state values."""
        assert is_valid_transition("invalid_state", "fixed") is False
        assert is_valid_transition("unknown", "invalid_state") is False
        assert is_valid_transition("invalid", "also_invalid") is False


class TestApplyTransition:
    """Tests for the apply_transition function."""

    def test_apply_valid_transition(self):
        """Test applying a valid transition."""
        result = apply_transition("unknown", "pending_upstream")

        assert result.success is True
        assert result.old_state == "unknown"
        assert result.new_state == "pending_upstream"
        assert "Valid transition" in result.reason

    def test_apply_same_state_no_change(self):
        """Test applying same state results in no change."""
        result = apply_transition("pending_upstream", "pending_upstream")

        assert result.success is True
        assert result.old_state == "pending_upstream"
        assert result.new_state == "pending_upstream"
        assert "No change required" in result.reason

    def test_apply_transition_from_terminal_state(self):
        """Test that transition from terminal state fails."""
        result = apply_transition("fixed", "pending_upstream")

        assert result.success is False
        assert result.old_state == "fixed"
        assert result.new_state == "fixed"  # Keeps current state
        assert "Cannot transition from terminal state" in result.reason

    def test_apply_invalid_transition(self):
        """Test applying an invalid transition."""
        result = apply_transition("unknown", "will_not_fix")

        assert result.success is False
        assert result.old_state == "unknown"
        assert result.new_state == "unknown"  # Keeps current state
        assert "Invalid transition" in result.reason

    def test_apply_transition_with_none_current_state(self):
        """Test applying transition when current state is None."""
        result = apply_transition(None, "pending_upstream")

        assert result.success is True
        assert result.old_state == "unknown"
        assert result.new_state == "pending_upstream"

    def test_apply_transition_with_empty_current_state(self):
        """Test applying transition when current state is empty string."""
        result = apply_transition("", "pending_upstream")

        assert result.success is True
        assert result.old_state == "unknown"
        assert result.new_state == "pending_upstream"

    def test_apply_transition_normalizes_case(self):
        """Test that state values are normalized to lowercase."""
        result = apply_transition("UNKNOWN", "PENDING_UPSTREAM")

        assert result.success is True
        assert result.old_state == "unknown"
        assert result.new_state == "pending_upstream"

    def test_apply_transition_strips_whitespace(self):
        """Test that state values have whitespace stripped."""
        result = apply_transition("  unknown  ", "  pending_upstream  ")

        assert result.success is True
        assert result.old_state == "unknown"
        assert result.new_state == "pending_upstream"

    def test_apply_transition_allow_invalid(self):
        """Test allowing invalid transitions."""
        result = apply_transition("unknown", "will_not_fix", allow_invalid=True)

        assert result.success is True
        assert result.old_state == "unknown"
        assert result.new_state == "will_not_fix"
        assert "WARNING" in result.reason


class TestGetTransitionExplanation:
    """Tests for the get_transition_explanation function."""

    def test_unknown_to_pending_upstream_explanation(self):
        """Test explanation for unknown to pending_upstream."""
        explanation = get_transition_explanation("unknown", "pending_upstream")
        assert "Awaiting fix" in explanation or "upstream" in explanation.lower()

    def test_unknown_to_fixed_explanation(self):
        """Test explanation for unknown to fixed."""
        explanation = get_transition_explanation("unknown", "fixed")
        assert "fix" in explanation.lower()

    def test_pending_upstream_to_fixed_explanation(self):
        """Test explanation for pending_upstream to fixed."""
        explanation = get_transition_explanation("pending_upstream", "fixed")
        assert "fix" in explanation.lower() or "released" in explanation.lower()

    def test_pending_upstream_to_not_applicable_explanation(self):
        """Test explanation for pending_upstream to not_applicable."""
        explanation = get_transition_explanation("pending_upstream", "not_applicable")
        assert "not apply" in explanation.lower() or "not applicable" in explanation.lower()

    def test_pending_upstream_to_will_not_fix_explanation(self):
        """Test explanation for pending_upstream to will_not_fix."""
        explanation = get_transition_explanation("pending_upstream", "will_not_fix")
        assert "not" in explanation.lower() and "fix" in explanation.lower()

    def test_unknown_transition_explanation(self):
        """Test explanation for transitions not in explanations dict."""
        explanation = get_transition_explanation("some_state", "another_state")
        assert "some_state" in explanation
        assert "another_state" in explanation

    def test_case_insensitive_explanations(self):
        """Test that explanations work with different cases."""
        explanation = get_transition_explanation("UNKNOWN", "FIXED")
        assert "fix" in explanation.lower()
