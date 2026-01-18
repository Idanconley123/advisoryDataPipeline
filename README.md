# Advisory Data Pipeline

Echo's CVE Advisory Management System - A PySpark-based data pipeline for tracking vulnerability remediation status.

## Overview

This pipeline automates the tracking of CVE (Common Vulnerabilities and Exposures) lifecycle from discovery through resolution. It ingests data from multiple sources, enriches it with external APIs, and maintains a consistent state machine for advisory status.

## Features

- **Automated CVE tracking** from discovery to resolution
- **Multi-source enrichment** (NVD, GitHub Security Advisories, distro feeds)
- **State machine validation** to prevent invalid status transitions
- **Customer-facing explanations** for every advisory state
- **Audit trail** with diff tracking between runs
- **Run isolation** - failed runs don't affect production data

## Quick Start

### Prerequisites

- Python 3.12+
- Docker & Docker Compose
- Poetry

### Run Locally

```bash
# Make the script executable (first time only)
chmod +x local-run.sh

# Run the full pipeline
./local-run.sh

# Run with PgAdmin UI
./local-run.sh --with-pgadmin

# Stop containers
./local-run.sh --down

# Clean up (removes data)
./local-run.sh --clean
```

### Manual Setup

```bash
# Start PostgreSQL
docker-compose up -d

# Install dependencies
poetry install

# Run pipeline
poetry run advisory-pipeline
```

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                      External Sources                            │
├─────────────────┬─────────────────┬─────────────────────────────┤
│  Echo Advisory  │    NVD API      │  GitHub Security Advisories │
└────────┬────────┴────────┬────────┴────────┬────────────────────┘
         │                 │                 │
         └─────────────────┴─────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│                        Pipeline                                  │
├─────────────────┬─────────────────┬─────────────────────────────┤
│    Ingest       │     Enrich      │       State Machine         │
└─────────────────┴─────────────────┴─────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Production State                              │
│                   (S3 + Iceberg)                                 │
└─────────────────────────────────────────────────────────────────┘
```

## State Machine

CVEs transition through the following states:

| State | Description |
|-------|-------------|
| `unknown` | Newly discovered, awaiting analysis |
| `pending_upstream` | Confirmed CVE, waiting for fix |
| `fixed` | Fix version available |
| `not_applicable` | CVE rejected or doesn't apply |
| `will_not_fix` | Vendor decided not to patch |

### Valid Transitions

```
unknown → pending_upstream
unknown → fixed
pending_upstream → fixed
pending_upstream → not_applicable
pending_upstream → will_not_fix
```

Terminal states (`fixed`, `not_applicable`, `will_not_fix`) cannot transition to other states.

## Project Structure

```
advisory-pipeline/
├── src/advisory_pipeline/
│   ├── ingest/              # Data ingestion from sources
│   ├── enrichment/          # External API enrichment (NVD, etc.)
│   ├── state_machine/       # State transition logic
│   ├── config/              # Configuration management
│   └── pipeline_libs/       # Shared utilities (Spark, S3)
├── docker-compose.yml       # Local PostgreSQL setup
├── local-run.sh            # Local development runner
├── run.py                  # Pipeline entry point
└── pyproject.toml          # Python dependencies
```

## Configuration

### Environment Variables

Create a `.env` file in the project root:

```bash
# .env
NVD_API_KEY=your-api-key-here
```

Get your NVD API key at: https://nvd.nist.gov/developers/request-an-api-key

| Variable | Default | Description |
|----------|---------|-------------|
| `NVD_API_KEY` | (none) | NVD API key for faster rate limits (50 req/30s vs 5 req/30s) |
| `POSTGRES_HOST` | `localhost` | PostgreSQL host |
| `POSTGRES_PORT` | `5432` | PostgreSQL port |
| `POSTGRES_DB` | `advisory_db` | Database name |
| `POSTGRES_USER` | `advisory_user` | Database user |
| `POSTGRES_PASSWORD` | `advisory_pass` | Database password |

### S3 Paths

```
staging/run_id={timestamp}/
├── ingest/
├── enrichment/
│   ├── raw_nvd/
│   └── normalized/
└── state_machine/
    ├── mapped_state/
    └── diff_csv/

prod/state_machine/
└── cve_state_machine/
```

## Development

### Install Dependencies

```bash
poetry install
```

### Run Tests

```bash
poetry run pytest
```

### Linting

```bash
poetry run ruff check .
poetry run pyright
```

## Services

| Service | URL | Credentials |
|---------|-----|-------------|
| PostgreSQL | `localhost:5432` | `advisory_user` / `advisory_pass` |
| PgAdmin | `localhost:5050` | `admin@example.com` / `admin` |

## License
