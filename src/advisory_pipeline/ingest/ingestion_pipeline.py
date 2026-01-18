import logging

from ..dependencies import Dependencies
from ..config import Config
from .config import sources_config
from .definitions import PublicJSON, PostgresDB, Table, Source
from typing import Iterable

logger = logging.getLogger(__name__)


def _load_public_json_source_table(
    deps: Dependencies,
    config: Config,
    run_id: str,
    table_list: Iterable[Table],
    url: str,
    source: Source,
):
    for table in table_list:
        source.read_table(deps, table.schema, config, url, run_id, table.table_name)


def _load_postgres_source_table(
    deps: Dependencies,
    config: Config,
    run_id: str,
    table_list: Iterable[Table],
    jdbc_url: str,
    user: str,
    password: str,
    source: Source,
):
    for table in table_list:
        source.read_table(
            deps,
            table.schema,
            config,
            jdbc_url,
            user,
            password,
            run_id,
            table.table_name,
            source.name,
        )


def run_ingestion_pipeline(
    deps: Dependencies,
    config: Config,
    run_id: str,
):
    for source in sources_config.sources:
        if isinstance(source, PublicJSON):
            _load_public_json_source_table(
                deps, config, run_id, source.tables, source.url, source
            )
        elif isinstance(source, PostgresDB):
            _load_postgres_source_table(
                deps,
                config,
                run_id,
                source.tables,
                source.jdbc_url,
                source.properties.user,
                source.properties.password,
                source,
            )


# TODO add quality tests
