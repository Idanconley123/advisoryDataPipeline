"""
Pytest configuration and fixtures for Advisory Data Pipeline tests.
"""

import pytest
import sys
from pathlib import Path

# Add source path for imports
project_root = Path(__file__).parent.parent / "advisoryDataPipeline"
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(project_root / "src"))


@pytest.fixture
def sample_run_id():
    """Return a sample run ID for testing."""
    return "20260118_120000"


@pytest.fixture
def sample_base_path(tmp_path):
    """Return a temporary base path for testing."""
    return str(tmp_path / "advisory_pipeline")


@pytest.fixture
def sample_cve_data():
    """Return sample CVE data for testing."""
    return [
        {
            "cve_id": "CVE-2024-0001",
            "package": "test-package",
            "status": "pending_upstream",
        },
        {
            "cve_id": "CVE-2024-0002",
            "package": "another-package",
            "status": "fixed",
            "fixed_version": "1.2.3",
        },
        {
            "cve_id": "CVE-2024-0003",
            "package": "third-package",
            "status": "not_applicable",
        },
    ]
