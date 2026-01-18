import os
from pathlib import Path
from dotenv import load_dotenv
import logging

logger = logging.getLogger(__name__)


# Load .env file from project root
_project_root = Path(__file__).parent.parent.parent.parent.parent.parent
_env_path = _project_root / ".env"
if _env_path.exists():
    load_dotenv(_env_path)
    logger.info(f"Loaded environment from {_env_path}")

# NVD API configuration
NVD_API_URL = "https://services.nvd.nist.gov/rest/json/cves/2.0"
NVD_API_KEY = os.getenv("NVD_API_KEY")

# Rate limits (requests per second)
RATE_LIMIT_WITH_KEY = 1.5  # ~50 requests per 30 seconds
RATE_LIMIT_WITHOUT_KEY = 0.15  # ~5 requests per 30 seconds

# Use faster rate if API key is available
RATE_LIMIT = RATE_LIMIT_WITH_KEY if NVD_API_KEY else RATE_LIMIT_WITHOUT_KEY

MAX_RETRIES = 3
RETRY_BACKOFF_SECONDS = 5
PARALLEL_WORKERS = 3
