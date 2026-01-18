from dataclasses import dataclass, field
from typing import Callable, Iterable, Union, Literal
from pyspark.sql.types import StructType

from ..pipeline_libs.spark import read_table


@dataclass(frozen=True, kw_only=True)
class Table:
    table_name: str
    schema: StructType


@dataclass(frozen=True, kw_only=True)
class Source:
    tables: Iterable["Table"]
    read_table: Callable = field(default=read_table)
    name: str


@dataclass(frozen=True, kw_only=True)
class PublicJSON(Source):
    url: str
    type: Literal["PublicJSON"] = field(default="PublicJSON")


@dataclass(frozen=True, kw_only=True)
class PostgresProperties:
    user: str
    password: str
    driver: str = "org.postgresql.Driver"


@dataclass(frozen=True, kw_only=True)
class PostgresDB(Source):
    type: Literal["postgresDB"] = field(default="postgresDB")
    jdbc_url: str
    properties: PostgresProperties


@dataclass(frozen=True, kw_only=True)
class Sources:
    sources: Iterable[Union[PublicJSON, PostgresDB]]
