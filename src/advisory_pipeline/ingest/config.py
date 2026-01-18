from .definitions import Sources, PostgresDB, PublicJSON, Table, PostgresProperties
from .schemas.echo_advisory_schema import echo_advisory_schema
from .schemas.not_applicable_schema import not_applicable_schema
from .fetch.echo_advisory.fetch import ingest_echo_advisory_source
from .fetch.pg_tables.fetch import ingest_postgres_source


sources_config = Sources(
    sources=[
        PublicJSON(
            name="echo_advisory",
            url="https://advisory.echohq.com",
            tables=[Table(table_name="data", schema=echo_advisory_schema)],
            read_table=ingest_echo_advisory_source,
        ),
        PostgresDB(
            name="advisory",
            tables=[
                Table(table_name="not_applicable_cves", schema=not_applicable_schema)
            ],
            jdbc_url="jdbc:postgresql://localhost:5432/advisory_db",
            properties=PostgresProperties(
                user="advisory_user", password="advisory_pass"
            ),
            read_table=ingest_postgres_source,
        ),
    ]
)
