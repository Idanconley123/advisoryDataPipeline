#!/usr/bin/env python3
import logging
import sys
from datetime import datetime
from pathlib import Path

# Add src to path for local development
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root / "src"))

from src.advisory_pipeline.config import Config
from src.advisory_pipeline.dependencies import create_dependencies
from src.advisory_pipeline.ingest.ingestion_pipeline import run_ingestion_pipeline
from src.advisory_pipeline.enrichment.pipeline import enrich_pipeline
from src.advisory_pipeline.state_machine.pipeline import state_machine_pipeline

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
    ],
)

logger = logging.getLogger(__name__)


def main():
    """Run ingestion pipeline locally."""

    logger.info("=" * 80)
    logger.info("INGESTION PIPELINE - LOCAL RUN")
    logger.info("=" * 80)

    # Generate run ID
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    logger.info(f"Run ID: {run_id}")

    # Create configuration
    logger.info("\n[Config] Creating pipeline configuration...")
    config = Config.from_defaults(run_id=run_id)

    # Create dependencies
    logger.info("\n[Setup] Creating dependencies (SparkSession, etc.)...")
    deps = create_dependencies(
        app_name=f"advisory_ingestion_local_{run_id}",
        config=config,
    )

    try:
        # Run ingestion pipeline
        logger.info("\n" + "=" * 80)
        logger.info("STARTING INGESTION")
        logger.info("=" * 80)

        run_ingestion_pipeline(deps, config, run_id)
        enrich_pipeline(deps, config, run_id)
        state_machine_pipeline(deps, config, run_id)

        return 0

    except Exception as e:
        logger.error("\n" + "=" * 80)
        logger.error("❌ PIPELINE FAILED")
        logger.error("=" * 80)
        logger.error(f"\nError: {e}", exc_info=True)
        return 1

    finally:
        # Cleanup
        logger.info("\n[Cleanup] Stopping Spark session...")
        deps.spark.stop()
        logger.info("Done.")


if __name__ == "__main__":
    sys.exit(main())
