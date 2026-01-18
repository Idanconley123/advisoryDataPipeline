from logging import Logger
from typing import Optional, List
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType
from pyspark.sql.functions import col
from pyspark.errors import AnalysisException

import logging

log = logging.getLogger("Spark Utils")


# PostgreSQL JDBC settings
POSTGRES_DRIVER = "org.postgresql.Driver"


def write_table(
    spark: SparkSession,
    path: str,
    write_method: str,
    partition_keys: List[str],
    partitions: int,
    schema: StructType,
    sql: str,
    df: Optional[DataFrame] = None,
):
    """
    Write table to storage.

    Exact same signature and behavior as inventory pipeline.

    Args:
        spark: SparkSession
        path: Output path
        write_method: Write mode (overwrite, append, etc.)
        partition_keys: List of partition columns
        partitions: Number of partitions to repartition to
        schema: Target schema
        sql: SQL query to execute (if df not provided)
        df: Optional DataFrame to write (if provided, sql is ignored)
    """
    log.info(
        f"start writing to {path} with method {write_method} and partitions {partitions}"
    )

    if not df:
        df = spark.sql(sql)

    if schema:
        df = df.select([col(c.name).cast(c.dataType) for c in schema])

    if partitions:
        df = df.repartition(partitions)
        df.write.mode(write_method).partitionBy(*partition_keys).parquet(path)
    else:
        df.write.mode(write_method).partitionBy(*partition_keys).parquet(path)

    log.info(f"Done writing successfully to {path}")


def read_table(
    spark: SparkSession,
    path: str,
    schema: StructType,
    paths: Optional[List[str]] = None,
    type: str = "parquet",
    header: bool = False,
    # PostgreSQL parameters (like inventory pattern)
    jdbc_url: Optional[str] = None,
    jdbc_table: Optional[str] = None,
    jdbc_user: Optional[str] = None,
    jdbc_password: Optional[str] = None,
) -> DataFrame:
    """
    Read table from storage or PostgreSQL.

    Extended inventory pattern with PostgreSQL support.

    Args:
        spark: SparkSession
        path: Path to read from (for file-based sources)
        schema: Schema to apply
        paths: Optional list of paths (for reading multiple paths)
        type: File type (parquet, json, csv, postgres)
        header: Whether CSV has header row
        jdbc_url: PostgreSQL JDBC URL (for postgres type)
        jdbc_table: Table name in PostgreSQL (for postgres type)
        jdbc_user: PostgreSQL user (for postgres type)
        jdbc_password: PostgreSQL password (for postgres type)

    Returns:
        DataFrame with data
    """
    # Handle PostgreSQL reads
    if type == "postgres":
        if not all([jdbc_url, jdbc_table, jdbc_user, jdbc_password]):
            raise ValueError(
                "PostgreSQL reads require jdbc_url, jdbc_table, jdbc_user, jdbc_password"
            )

        log.info(f"start reading from PostgreSQL table: {jdbc_table}")

        try:
            df = (
                spark.read.format("jdbc")
                .option("url", jdbc_url)
                .option("dbtable", jdbc_table)
                .option("user", jdbc_user)
                .option("password", jdbc_password)
                .option("driver", POSTGRES_DRIVER)
                .load()
            )

            # Apply schema if provided (cast columns)
            if schema:
                df = df.select([col(c.name).cast(c.dataType) for c in schema])

            log.info(f"end reading successfully from PostgreSQL table: {jdbc_table}")
            return df

        except Exception as e:
            log.error(f"Failed to read from PostgreSQL: {e}")
            # Return empty DataFrame with schema on error
            if schema:
                return spark.createDataFrame([], schema)
            raise

    # Handle file-based reads (original inventory logic)
    log.info(f"start reading from {path}")

    if schema:
        df: DataFrame | None = None
        try:
            if type == "parquet" and path:
                df = spark.read.option("basePath", path).schema(schema).parquet(path)
            elif type == "parquet" and paths:
                df = (
                    spark.read.option("basePath", paths[0])
                    .schema(schema)
                    .parquet(*paths)
                )
            elif type == "json":
                df = spark.read.option("basePath", path).schema(schema).json(f"{path}")
            elif type == "csv":
                if header:
                    df = (
                        spark.read.option("basePath", path)
                        .option("header", "true")
                        .schema(schema)
                        .csv(f"{path}")
                    )
                else:
                    df = (
                        spark.read.option("basePath", path)
                        .schema(schema)
                        .csv(f"{path}")
                    )
        except AnalysisException as ae:
            if "Path does not exist" in str(ae):
                log.info(f"Path not found: {path}")
                df = spark.createDataFrame([], schema)
            else:
                log.info(f"try to read from {path}, schema is {schema}")
                raise Exception("Schema or partition column issue:", str(ae))

        if df is None:
            raise ValueError("Failed to read data - no DataFrame created")

        log.info(f"end reading successfully from {path}")
        return df
    else:
        raise Exception("Schema is None, please create the relevant schema")


def read_table_multipath(
    spark: SparkSession,
    base_path: str,
    paths: Optional[List[str]],
    schema: StructType,
    log: Logger,
):
    """
    Read table from multiple paths.

    Exact same signature and behavior as inventory pipeline.

    Args:
        spark: SparkSession
        base_path: Base path for relative paths
        paths: List of paths to read
        schema: Schema to apply
        log: Logger instance

    Returns:
        DataFrame with data
    """
    log.info(f"start reading from {base_path}")

    if schema:
        if paths is None:
            raise ValueError("paths parameter is required")
        try:
            df = spark.read.option("basePath", base_path).schema(schema).parquet(*paths)
        except AnalysisException as ae:
            if "Path does not exist" in str(ae):
                log.info(f"Path not found: {base_path}")
                df = spark.createDataFrame([], schema)
            else:
                log.info(f"try to read from {base_path}, schema is {schema}")
                raise Exception("Schema or partition column issue:", str(ae))

        log.info(f"end reading successfully from {base_path}")
        return df
    else:
        raise Exception("Schema is None, please create the relevant schema")


def read_postgres_table(
    spark: SparkSession,
    jdbc_url: str,
    table_name: str,
    schema: StructType,
    user: str,
    password: str,
) -> DataFrame:
    """
    Read a table from PostgreSQL using JDBC.

    Generic PostgreSQL reader - can be used for any table.
    Like transaction pipeline's postgres read pattern.

    Args:
        spark: SparkSession
        jdbc_url: JDBC connection URL (e.g., "jdbc:postgresql://host:port/database")
        table_name: Full table name with schema (e.g., "schema.table_name")
        schema: PySpark StructType schema
        user: PostgreSQL username
        password: PostgreSQL password

    Returns:
        DataFrame with table data

    """
    log.info(f"Reading PostgreSQL table: {table_name} from {jdbc_url}")

    try:
        # Create properties dict (like transaction pipeline pattern)
        properties = {
            "user": user,
            "password": password,
            "driver": POSTGRES_DRIVER,
        }

        # Read using spark.read.jdbc with properties (like transaction pipeline)
        df = spark.read.jdbc(url=jdbc_url, table=table_name, properties=properties)

        # Apply schema if provided (cast columns)
        if schema:
            df = df.select([col(c.name).cast(c.dataType) for c in schema])

        count = df.count()
        log.info(f"✓ Read {count} records from PostgreSQL table: {table_name}")

        return df

    except AnalysisException as e:
        if "Table or view not found" in str(e) or "no such table" in str(e):
            log.info(
                f"PostgreSQL table not found: {table_name}. Returning empty DataFrame."
            )
            return spark.createDataFrame([], schema)
        else:
            log.error(f"Error reading from PostgreSQL table {table_name}: {e}")
            raise
    except Exception as e:
        log.error(f"Failed to read PostgreSQL table {table_name}: {e}", exc_info=True)
        raise
