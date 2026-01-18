from dataclasses import dataclass
from typing import Callable, Optional, Iterable
from enum import Enum
from pyspark.sql.types import StructType


class UpstreamSourceType(Enum):
    OSV = "osv"
    GITHUB_ADVISORY = "github_advisory"
    NVD = "nvd"


class EchoStatus(Enum):
    PENDING = "pending_upstream"
    FIXED = "fixed"
    NOT_APPLICABLE = "not_applicable"


@dataclass(frozen=True, kw_only=True)
class RawTable:
    name: str
    schema: StructType


@dataclass(frozen=True, kw_only=True)
class UpstreamSourceConfiguration:
    name: str
    source_type: UpstreamSourceType
    priority: int
    schema: Optional[StructType] = None
    enrichment_function: Optional[Callable] = None
    normalization_function: Optional[Callable] = None


@dataclass(frozen=True, kw_only=True)
class UpstreamSourcesConfig:
    sources: Iterable[UpstreamSourceConfiguration]
