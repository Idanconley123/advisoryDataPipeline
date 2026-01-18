"""AWS S3 write operations."""

import logging
from pyspark.sql import SparkSession
from typing import Union, Optional

log = logging.getLogger(__name__)


def write_raw_file(
    spark: SparkSession,
    content: Union[str, bytes],
    path: str,
    format: str = "text",
    bucket: Optional[str] = None,
) -> None:
    """
    Write raw content to AWS S3 using Spark.

    Like inventory pattern - uses Spark for all I/O operations.

    Args:
        spark: SparkSession
        content: Raw content to write (string or bytes)
        path: S3 path (s3://bucket/key or just key if bucket provided)
        format: Format of the content ('text', 'json', 'csv', 'binary')
        bucket: Optional bucket name (if path doesn't include s3://)

    Example:
        >>> write_raw_file(spark, json_content, "s3://mybucket/data.json", "json")
        >>> write_raw_file(spark, json_content, "data.json", "json", bucket="mybucket")
    """
    # Normalize S3 path
    if bucket and not path.startswith("s3://"):
        s3_path = f"s3://{bucket}/{path.lstrip('/')}"
    else:
        s3_path = path

    log.info(f"Writing raw {format} file to S3: {s3_path}")

    try:
        if format in ["text", "json", "csv"]:
            # For text-based formats, create RDD with single partition
            if isinstance(content, bytes):
                content = content.decode("utf-8")

            # Create RDD with single element
            rdd = spark.sparkContext.parallelize([content], 1)

            # Save as text file to S3
            rdd.saveAsTextFile(s3_path)

        elif format == "binary":
            # For binary content to S3
            if isinstance(content, str):
                content = content.encode("utf-8")

            # Use boto3 for binary S3 writes
            import boto3

            # Parse S3 path
            if s3_path.startswith("s3://"):
                parts = s3_path[5:].split("/", 1)
                bucket_name = parts[0]
                key = parts[1] if len(parts) > 1 else ""
            else:
                raise ValueError(f"Invalid S3 path: {s3_path}")

            s3_client = boto3.client("s3")
            s3_client.put_object(Bucket=bucket_name, Key=key, Body=content)
        else:
            raise ValueError(f"Unsupported format: {format}")

        log.info(f"✓ Successfully wrote raw file to S3: {s3_path}")

    except Exception as e:
        log.error(f"Failed to write raw file to S3: {e}", exc_info=True)
        raise


def write_raw_json(
    spark: SparkSession,
    json_content: str,
    path: str,
    bucket: Optional[str] = None,
) -> None:
    """
    Write raw JSON content to S3.

    Convenience function for JSON files.

    Args:
        spark: SparkSession
        json_content: JSON string
        path: S3 path
        bucket: Optional bucket name
    """
    write_raw_file(spark, json_content, path, format="json", bucket=bucket)


def write_raw_csv(
    spark: SparkSession,
    csv_content: str,
    path: str,
    bucket: Optional[str] = None,
) -> None:
    """
    Write raw CSV content to S3.

    Convenience function for CSV files.

    Args:
        spark: SparkSession
        csv_content: CSV string
        path: S3 path
        bucket: Optional bucket name
    """
    write_raw_file(spark, csv_content, path, format="csv", bucket=bucket)


def write_raw_text(
    spark: SparkSession,
    text_content: str,
    path: str,
    bucket: Optional[str] = None,
) -> None:
    """
    Write raw text content to S3.

    Convenience function for text files.

    Args:
        spark: SparkSession
        text_content: Text string
        path: S3 path
        bucket: Optional bucket name
    """
    write_raw_file(spark, text_content, path, format="text", bucket=bucket)
