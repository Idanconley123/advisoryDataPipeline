from pyspark.sql import DataFrame
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
)
from pyspark.sql.functions import lit, current_timestamp
from ..dependencies import Dependencies
from ..config import Config
from ..pipeline_libs.spark import read_table, write_table
from ..enrichment.schemas.noramlized_schema import normalized_schema
from .schemas.state import mapped_state_schema
from .config import state_machine_objects, StateMachinObject
from .udf import register_state_machine_udfs
import logging

logger = logging.getLogger(__name__)


def _add_enrichment_source_field(schema: StructType) -> StructType:
    return schema.add(StructField("enrichment_source", StringType(), True))


def _load_normalized_enrichment(deps: Dependencies, config: Config, run_id: str):
    """Load normalized enrichment data into global temp view."""
    path = f"{config.pipeline.staging_path}/run_id={run_id}/enrichment/normalized"
    schema = _add_enrichment_source_field(normalized_schema)
    df = read_table(
        spark=deps.spark,
        path=path,
        schema=schema,
    )
    df.createGlobalTempView("normalized_enrichment")
    print(f"Loaded {df.count()} rows of normalized enrichment")


def _load_prod_state(deps: Dependencies, config: Config):
    """Load production state from PostgreSQL."""
    for object in state_machine_objects:
        path = f"{config.pipeline.prod_path}/state_machine/{object.name}"
        schema = object.schema
        df = read_table(
            spark=deps.spark,
            path=path,
            schema=schema,
        )
        df.createGlobalTempView(f"prod_{object.name}")


def __process_state_machine_object(
    deps: Dependencies, config: Config, run_id: str, object: StateMachinObject
):
    """Map new enrichment info to existing advisory state using UDFs."""
    query = object.processing_function()
    write_table(
        spark=deps.spark,
        path=f"{config.pipeline.staging_path}/run_id={run_id}/state_machine/mapped_state",
        write_method="overwrite",
        partition_keys=[],
        partitions=1,
        schema=mapped_state_schema,
        sql=query,
    )
    df = read_table(
        spark=deps.spark,
        path=f"{config.pipeline.staging_path}/run_id={run_id}/state_machine/mapped_state",
        schema=mapped_state_schema,
    )
    df.createGlobalTempView(f"processed_{object.name}")


def __upsert_state_machine_object(
    deps: Dependencies, config: Config, run_id: str, object: StateMachinObject
):
    """Upsert state machine object into production state."""
    query = object.upsert_function()
    write_table(
        spark=deps.spark,
        path=f"{config.pipeline.prod_path}/state_machine/{object.name}",
        write_method="overwrite",
        partition_keys=[],
        partitions=1,
        schema=object.schema,
        sql=query,
    )
    logger.info(f"Upserted {object.name} to production")
    df = read_table(
        spark=deps.spark,
        path=f"{config.pipeline.prod_path}/state_machine/{object.name}",
        schema=object.schema,
    )
    df.show()
    logger.info(
        f"You can view the new state in the prod path: {config.pipeline.prod_path}/state_machine/{object.name}"
    )


def state_machine_pipeline(deps: Dependencies, config: Config, run_id: str):
    """
    Run the state machine pipeline.

    Prerequisites:
    - raw_not_applicable_cves: Loaded from PostgreSQL in ingestion
    - raw_data: Loaded from Echo advisory data.json in ingestion
    - normalized_enrichment: Loaded from enrichment pipeline

    Steps:
    1. Register state machine UDFs
    2. Load normalized enrichment data
    3. Load production state
    4. Map new enrichment info to existing advisory state (uses UDFs)
    5. Write diff CSV (changes only)
    6. Upsert to production

    Returns:
        dict with run summary including diff statistics
    """
    logger.info("=" * 80)
    logger.info("STATE MACHINE PIPELINE")
    logger.info("=" * 80)

    # Step 1: Register state machine UDFs
    logger.info("\n[Step 1] Registering state machine UDFs...")
    register_state_machine_udfs(deps)

    # Step 2: Load normalized enrichment
    logger.info("\n[Step 2] Loading normalized enrichment...")
    _load_normalized_enrichment(deps, config, run_id)

    # Step 3: Load production state
    logger.info("\n[Step 3] Loading production state...")
    _load_prod_state(deps, config)

    # Step 4: Process each state machine object
    logger.info("\n[Step 4] Processing state machine objects...")

    for obj in state_machine_objects:
        logger.info(f"\n  Processing: {obj.name}")
        __process_state_machine_object(deps, config, run_id, obj)
        __upsert_state_machine_object(deps, config, run_id, obj)

    # Final summary
    logger.info("\n" + "=" * 80)
    logger.info("PIPELINE COMPLETE")
    logger.info("=" * 80)
