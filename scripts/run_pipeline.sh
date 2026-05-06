#!/bin/bash
# =============================================================================
# run_pipeline.sh — Big Data E-Commerce Pipeline Runner
# =============================================================================
# Usage:
#   bash scripts/run_pipeline.sh          → runs full pipeline
#   bash scripts/run_pipeline.sh producer → runs Kafka producer only
#   bash scripts/run_pipeline.sh spark    → runs Spark processing only
# =============================================================================

set -e   # Exit on any error

# ── Colors ───────────────────────────────────────────────────────────────────
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'  # No color

# ── Helpers ──────────────────────────────────────────────────────────────────
log_info()    { echo -e "${BLUE}[INFO]${NC}  $1"; }
log_success() { echo -e "${GREEN}[OK]${NC}    $1"; }
log_warn()    { echo -e "${YELLOW}[WARN]${NC}  $1"; }
log_error()   { echo -e "${RED}[ERROR]${NC} $1"; exit 1; }

# ── Project root (one level above scripts/) ──────────────────────────────────
PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$PROJECT_ROOT"

echo ""
echo "============================================================"
echo "  Big Data E-Commerce Pipeline"
echo "  Project root: $PROJECT_ROOT"
echo "============================================================"
echo ""

# ── 1. Check Python ───────────────────────────────────────────────────────────
log_info "Checking Python..."
if ! command -v python3 &>/dev/null; then
    log_error "Python 3 not found. Please install Python 3.8+."
fi
PYTHON_VERSION=$(python3 --version)
log_success "Found: $PYTHON_VERSION"

# ── 2. Check / create virtual environment ─────────────────────────────────────
if [ ! -d "venv" ]; then
    log_info "Creating virtual environment..."
    python3 -m venv venv
    log_success "Virtual environment created."
fi

log_info "Activating virtual environment..."
source venv/bin/activate
log_success "venv activated."

# ── 3. Install dependencies ────────────────────────────────────────────────────
log_info "Installing dependencies from requirements.txt..."
pip install --quiet -r requirements.txt
log_success "Dependencies installed."

# ── 4. Create output directories ──────────────────────────────────────────────
mkdir -p data/raw data/processed logs
log_success "Directories ready."

# ── 5. Run selected component ─────────────────────────────────────────────────
MODE="${1:-full}"

case "$MODE" in
  producer)
    log_info "Running Kafka producer only..."
    python3 src/ingestion/kafka_producer.py
    log_success "Kafka producer finished."
    ;;

  spark)
    log_info "Running Spark processing pipeline only..."
    python3 src/processing/spark_pipeline.py
    log_success "Spark pipeline finished."
    ;;

  full|*)
    log_info "Running FULL pipeline (producer → spark)..."

    log_info "Step 1/2 — Starting Kafka producer (demo mode)..."
    python3 src/ingestion/kafka_producer.py &
    PRODUCER_PID=$!
    sleep 3

    log_info "Step 2/2 — Running Spark processing pipeline..."
    python3 src/processing/spark_pipeline.py

    # Clean up background producer if still running
    if kill -0 $PRODUCER_PID 2>/dev/null; then
        kill $PRODUCER_PID
    fi

    log_success "Full pipeline complete!"
    ;;
esac

echo ""
echo "============================================================"
echo -e "  ${GREEN}✅ Pipeline run finished successfully!${NC}"
echo "  Output data: ./data/processed/"
echo "  Logs:        ./logs/"
echo "============================================================"
echo ""
