from dataclasses import dataclass
from typing import Callable
from pyspark.sql.types import StructType
from .schemas.state import mapped_state_schema
from .queries.map_new_info_with_udf import map_new_info_with_udf
from .queries.upsert_data import upsert_data


@dataclass(frozen=True, kw_only=True)
class StateMachinObject:
    """Configuration for state machine pipeline."""

    name: str
    processing_function: Callable
    upsert_function: Callable
    schema: StructType


CVE_STATE_MACHINE_OBJECT = StateMachinObject(
    name="cve_state_machine",
    processing_function=map_new_info_with_udf,
    upsert_function=upsert_data,
    schema=mapped_state_schema,
)

state_machine_objects = [
    CVE_STATE_MACHINE_OBJECT,
]
