from ..ingest.schemas.echo_advisory_schema import echo_advisory_schema
from ..ingest.schemas.not_applicable_schema import not_applicable_schema
from .definitions import RawTable


raw_table_list = [
    RawTable(name="data", schema=echo_advisory_schema),
    RawTable(name="not_applicable_cves", schema=not_applicable_schema),
]
