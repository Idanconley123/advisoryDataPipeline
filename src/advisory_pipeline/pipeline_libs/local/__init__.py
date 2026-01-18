"""Local filesystem write operations."""

import logging
from pyspark.sql import SparkSession
from typing import Union

log = logging.getLogger(__name__)


def write_raw_file(
    spark: SparkSession,
    content: Union[str, bytes],
    path: str,
    format: str = "text",
) -> None:
    """
    Write raw content to local filesystem using Spark.

    Like inventory pattern - uses Spark for all I/O operations.

    Args:
        spark: SparkSession
        content: Raw content to write (string or bytes)
        path: Output path
        format: Format of the content ('text', 'json', 'csv', 'binary')

    Example:
        >>> write_raw_file(spark, json_content, "/tmp/data.json", "json")
    """
    log.info(f"Writing raw {format} file to: {path}")

    try:
        if format in ["text", "json", "csv"]:
            # For text-based formats, create RDD with single partition
            if isinstance(content, bytes):
                content = content.decode("utf-8")

            # Create RDD with single element (the entire content)
            rdd = spark.sparkContext.parallelize([content], 1)

            # Save as text file (single partition = single file)
            # Spark adds part-00000 suffix, so we need to handle that
            temp_path = f"{path}.tmp"
            rdd.saveAsTextFile(temp_path)

            # Move the part file to the final location
            import os
            import shutil

            # Ensure parent directory exists
            os.makedirs(os.path.dirname(path), exist_ok=True)

            # Find the part file
            part_file = f"{temp_path}/part-00000"
            if os.path.exists(part_file):
                shutil.move(part_file, path)
                shutil.rmtree(temp_path)
            else:
                log.warning(f"Part file not found: {part_file}")

        elif format == "binary":
            # For binary content
            if isinstance(content, str):
                content = content.encode("utf-8")

            rdd = spark.sparkContext.parallelize([content], 1)

            # Use custom binary writer
            import os

            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "wb") as f:
                f.write(content)
        else:
            raise ValueError(f"Unsupported format: {format}")

        log.info(f"✓ Successfully wrote raw file: {path}")

    except Exception as e:
        log.error(f"Failed to write raw file: {e}", exc_info=True)
        raise


def write_raw_json(
    spark: SparkSession,
    json_content: str,
    path: str,
) -> None:
    """
    Write raw JSON content to local filesystem.

    Convenience function for JSON files.

    Args:
        spark: SparkSession
        json_content: JSON string
        path: Output path (should end with .json)
    """
    write_raw_file(spark, json_content, path, format="json")


def write_raw_csv(
    spark: SparkSession,
    csv_content: str,
    path: str,
) -> None:
    """
    Write raw CSV content to local filesystem.

    Convenience function for CSV files.

    Args:
        spark: SparkSession
        csv_content: CSV string
        path: Output path (should end with .csv)
    """
    write_raw_file(spark, csv_content, path, format="csv")


def write_raw_text(
    spark: SparkSession,
    text_content: str,
    path: str,
) -> None:
    """
    Write raw text content to local filesystem.

    Convenience function for text files.

    Args:
        spark: SparkSession
        text_content: Text string
        path: Output path
    """
    write_raw_file(spark, text_content, path, format="text")
