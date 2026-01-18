from dataclasses import dataclass
from pyspark.sql import SparkSession
from .config import Config


@dataclass(frozen=True, kw_only=True)
class Dependencies:
    """
    Pipeline dependencies.

    Same pattern as inventory - contains spark session,
    config, and any other shared resources.
    """

    spark: SparkSession
    config: Config


def create_dependencies(
    app_name: str,
    config: Config,
) -> Dependencies:
    """
    Create dependencies for pipeline.

    Args:
        app_name: Spark application name
        config: Pipeline configuration

    Returns:
        Dependencies with spark session and config
    """
    # Create Spark session with PostgreSQL driver if needed
    builder = SparkSession.builder.appName(app_name)  # type: ignore[union-attr]

    # Add PostgreSQL JDBC driver if postgres is configured
    builder = builder.config("spark.jars.packages", "org.postgresql:postgresql:42.7.1")

    # Set other Spark configs
    builder = builder.config("spark.sql.sources.partitionOverwriteMode", "dynamic")

    spark = builder.getOrCreate()

    # Set checkpoint directory
    spark.sparkContext.setCheckpointDir(config.pipeline.checkpoint_path)

    return Dependencies(
        spark=spark,
        config=config,
    )
