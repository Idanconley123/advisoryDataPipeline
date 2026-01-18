from ....dependencies import Dependencies
from ....config import Config
from ....pipeline_libs.spark import read_postgres_table, write_table, read_table
from pyspark.sql.types import StructType
import logging

logger = logging.getLogger(__name__)


def ingest_postgres_source(
    deps: Dependencies,
    schema: StructType,
    config: Config,
    jdbc_url: str,
    user: str,
    password: str,
    run_id: str,
    table_name: str,
    source_name: str = "advisory",
):
    """
    Ingest Not Applicable CVEs from PostgreSQL.

    Reads from postgres and saves to staging area.
    """

    df = read_postgres_table(
        spark=deps.spark,
        jdbc_url=jdbc_url,
        table_name=f"{source_name}.{table_name}",
        schema=schema,
        user=user,
        password=password,
    )

    # Write to staging (inventory pattern)
    output_path = f"{config.pipeline.staging_path}/run_id={run_id}/sources/{table_name}"
    write_table(
        spark=deps.spark,
        path=output_path,
        write_method="overwrite",
        partition_keys=[],
        partitions=1,
        schema=schema,
        sql="",
        df=df,
    )
    df = read_table(
        spark=deps.spark,
        path=output_path,
        schema=schema,
    )
    df.createGlobalTempView(table_name)
