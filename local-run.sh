#!/bin/bash
#
# Local Development Runner for Advisory Data Pipeline
#
# This script:
# 1. Checks for required dependencies (Docker, docker-compose, Poetry)
# 2. Starts Docker containers (PostgreSQL)
# 3. Waits for services to be healthy
# 4. Installs Python dependencies via Poetry
# 5. Runs the advisory pipeline
#
# Usage:
#   ./local-run.sh              # Run full pipeline
#   ./local-run.sh --with-pgadmin   # Also start PgAdmin UI
#   ./local-run.sh --down           # Stop all containers
#   ./local-run.sh --clean          # Stop containers and remove volumes
#

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Load .env file if it exists (loaded early, before log functions defined)
if [ -f ".env" ]; then
    set -a  # Auto-export all variables
    source .env
    set +a
fi

# ============================================================================
# Helper Functions
# ============================================================================

log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

check_command() {
    if ! command -v "$1" &> /dev/null; then
        log_error "$1 is not installed. Please install it first."
        return 1
    fi
    return 0
}

# ============================================================================
# Parse Arguments
# ============================================================================

WITH_PGADMIN=false
STOP_ONLY=false
CLEAN=false

while [[ $# -gt 0 ]]; do
    case $1 in
        --with-pgadmin)
            WITH_PGADMIN=true
            shift
            ;;
        --down)
            STOP_ONLY=true
            shift
            ;;
        --clean)
            CLEAN=true
            shift
            ;;
        -h|--help)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --with-pgadmin   Start PgAdmin UI at http://localhost:5050"
            echo "  --down           Stop all Docker containers"
            echo "  --clean          Stop containers and remove volumes (deletes data!)"
            echo "  -h, --help       Show this help message"
            exit 0
            ;;
        *)
            log_error "Unknown option: $1"
            exit 1
            ;;
    esac
done

# ============================================================================
# Handle --down and --clean
# ============================================================================

if [ "$STOP_ONLY" = true ]; then
    log_info "Stopping Docker containers..."
    docker-compose down
    log_success "Containers stopped."
    exit 0
fi

if [ "$CLEAN" = true ]; then
    log_warn "This will delete all data! Are you sure? (y/N)"
    read -r confirm
    if [[ "$confirm" =~ ^[Yy]$ ]]; then
        log_info "Stopping containers and removing volumes..."
        docker-compose down -v
        log_success "Containers stopped and volumes removed."
    else
        log_info "Cancelled."
    fi
    exit 0
fi

# ============================================================================
# Main Execution
# ============================================================================

echo ""
echo "============================================================================"
echo "  Advisory Data Pipeline - Local Runner"
echo "============================================================================"
echo ""

# Step 1: Check dependencies
log_info "Step 1: Checking dependencies..."

MISSING_DEPS=false

if ! check_command docker; then
    log_error "Docker is required. Install from: https://docs.docker.com/get-docker/"
    MISSING_DEPS=true
fi

if ! check_command docker-compose; then
    # Try docker compose (v2)
    if docker compose version &> /dev/null; then
        # Create alias for docker-compose
        docker-compose() {
            docker compose "$@"
        }
        log_info "Using 'docker compose' (v2)"
    else
        log_error "docker-compose is required. Install from: https://docs.docker.com/compose/install/"
        MISSING_DEPS=true
    fi
fi

if ! check_command poetry; then
    log_error "Poetry is required. Install from: https://python-poetry.org/docs/#installation"
    log_error "  curl -sSL https://install.python-poetry.org | python3 -"
    MISSING_DEPS=true
fi

if [ "$MISSING_DEPS" = true ]; then
    log_error "Please install missing dependencies and try again."
    exit 1
fi

log_success "All dependencies found!"

# Check for .env file
if [ -f ".env" ]; then
    log_success "Environment loaded from .env"
    if [ -n "$NVD_API_KEY" ]; then
        log_success "NVD API key configured (faster rate limits)"
    fi
else
    log_warn "No .env file found. Create one with NVD_API_KEY for faster rate limits."
    log_warn "See README.md for setup instructions."
fi

# Step 2: Check Docker daemon
log_info "Step 2: Checking Docker daemon..."

if ! docker info &> /dev/null; then
    log_error "Docker daemon is not running. Please start Docker Desktop or the Docker service."
    exit 1
fi

log_success "Docker daemon is running."

# Step 3: Start Docker containers
log_info "Step 3: Starting Docker containers..."

if [ "$WITH_PGADMIN" = true ]; then
    log_info "Starting PostgreSQL + PgAdmin..."
    docker-compose --profile with-pgadmin up -d
else
    log_info "Starting PostgreSQL..."
    docker-compose up -d
fi

# Step 4: Wait for PostgreSQL to be healthy
log_info "Step 4: Waiting for PostgreSQL to be healthy..."

MAX_RETRIES=30
RETRY_COUNT=0

while [ $RETRY_COUNT -lt $MAX_RETRIES ]; do
    if docker-compose exec -T postgres pg_isready -U advisory_user -d advisory_db &> /dev/null; then
        log_success "PostgreSQL is ready!"
        break
    fi
    
    RETRY_COUNT=$((RETRY_COUNT + 1))
    echo -n "."
    sleep 2
done

if [ $RETRY_COUNT -eq $MAX_RETRIES ]; then
    log_error "PostgreSQL failed to become healthy after $MAX_RETRIES attempts."
    log_error "Check logs with: docker-compose logs postgres"
    exit 1
fi

# Step 5: Install Python dependencies
log_info "Step 5: Installing Python dependencies..."

if [ ! -f "poetry.lock" ]; then
    log_info "No poetry.lock found, running poetry install..."
    poetry install
else
    log_info "Checking for dependency updates..."
    poetry install --sync
fi

log_success "Python dependencies installed!"

# Step 6: Run the pipeline
log_info "Step 6: Running Advisory Pipeline..."

echo ""
echo "============================================================================"
echo "  Starting Pipeline"
echo "============================================================================"
echo ""

# Run the pipeline
poetry run advisory-pipeline

PIPELINE_EXIT_CODE=$?

echo ""
echo "============================================================================"

if [ $PIPELINE_EXIT_CODE -eq 0 ]; then
    log_success "Pipeline completed successfully!"
else
    log_error "Pipeline failed with exit code: $PIPELINE_EXIT_CODE"
fi

# Print service info
echo ""
echo "============================================================================"
echo "  Service Information"
echo "============================================================================"
echo ""
echo "  PostgreSQL: localhost:5432"
echo "    Database: advisory_db"
echo "    User:     advisory_user"
echo "    Password: advisory_pass"
echo ""

if [ "$WITH_PGADMIN" = true ]; then
    echo "  PgAdmin:    http://localhost:5050"
    echo "    Email:    admin@example.com"
    echo "    Password: admin"
    echo ""
fi

echo "  Commands:"
echo "    Stop containers:  ./local-run.sh --down"
echo "    Clean everything: ./local-run.sh --clean"
echo "    View logs:        docker-compose logs -f postgres"
echo ""
echo "============================================================================"

exit $PIPELINE_EXIT_CODE
