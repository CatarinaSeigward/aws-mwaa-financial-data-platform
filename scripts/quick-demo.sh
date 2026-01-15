#!/bin/bash
################################################################################
# Quick Demo Script - 5 分钟完整演示
# ====================================
# 生成样本数据 → 构建 JAR → 运行 ETL → 查询结果
#
# Usage:
#   ./scripts/quick-demo.sh
################################################################################

set -e  # Exit on error

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Project root
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

# Functions
print_header() {
    echo ""
    echo -e "${CYAN}╔════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${CYAN}║${NC} $1"
    echo -e "${CYAN}╚════════════════════════════════════════════════════════════╝${NC}"
}

print_step() {
    echo -e "\n${BLUE}▶ $1${NC}"
}

print_success() {
    echo -e "${GREEN}✅ $1${NC}"
}

print_error() {
    echo -e "${RED}❌ $1${NC}"
}

print_info() {
    echo -e "${YELLOW}ℹ️  $1${NC}"
}

# Main script
clear

echo -e "${CYAN}"
cat << "EOF"
╔═══════════════════════════════════════════════════════════════╗
║                                                               ║
║        Financial Data Platform - Quick Demo                  ║
║        Scala + Spark ETL Pipeline                            ║
║                                                               ║
╚═══════════════════════════════════════════════════════════════╝
EOF
echo -e "${NC}"

echo -e "${YELLOW}This demo will:${NC}"
echo "  1. Generate sample financial data (90 days)"
echo "  2. Build Scala JAR"
echo "  3. Start Docker Spark cluster"
echo "  4. Run ETL transformation"
echo "  5. Query results from PostgreSQL"
echo ""
echo -e "${YELLOW}Estimated time: ~5 minutes${NC}"
echo ""
read -p "Press Enter to start..."

################################################################################
# Step 1: Generate Sample Data
################################################################################

print_header "Step 1/5: Generate Sample Data"

print_step "Generating stock data for AAPL, GOOGL, MSFT, AMZN, TSLA..."

python scripts/generate_sample_data.py \
    --symbols AAPL GOOGL MSFT AMZN TSLA \
    --days 90

print_success "Sample data generated!"

print_info "Check generated files:"
ls -lh data/raw/date=*/symbol=*/*.json | head -5

read -p "Press Enter to continue..."

################################################################################
# Step 2: Build Scala JAR
################################################################################

print_header "Step 2/5: Build Scala JAR"

print_step "Compiling Scala code and creating assembly JAR..."

if [ -f "target/scala-2.12/financial-etl-1.0.0.jar" ]; then
    print_info "JAR already exists, skipping build (use --rebuild to force)"
    JAR_SIZE=$(du -h "target/scala-2.12/financial-etl-1.0.0.jar" | cut -f1)
    print_success "Using existing JAR: $JAR_SIZE"
else
    sbt clean assembly
    JAR_SIZE=$(du -h "target/scala-2.12/financial-etl-1.0.0.jar" | cut -f1)
    print_success "JAR built successfully: $JAR_SIZE"
fi

read -p "Press Enter to continue..."

################################################################################
# Step 3: Start Docker Spark Cluster
################################################################################

print_header "Step 3/5: Start Docker Spark Cluster"

print_step "Starting Spark Master + 2 Workers + PostgreSQL + Airflow..."

# Check if already running
if docker ps | grep -q "financial-spark-master"; then
    print_info "Spark cluster is already running"
else
    docker-compose -f docker-compose-spark.yml up -d

    print_step "Waiting for services to start (30s)..."
    sleep 30
fi

print_success "Docker services started!"

print_info "Service Status:"
docker-compose -f docker-compose-spark.yml ps

print_info "Access URLs:"
echo "  - Spark Master UI: http://localhost:8081"
echo "  - Airflow UI: http://localhost:8080 (admin/admin)"
echo "  - Grafana: http://localhost:3000 (admin/admin)"

read -p "Press Enter to continue..."

################################################################################
# Step 4: Run ETL Transformation
################################################################################

print_header "Step 4/5: Run Scala Spark ETL"

print_step "Submitting Spark job to cluster..."

# Copy JAR to Spark master
docker cp target/scala-2.12/financial-etl-1.0.0.jar \
    financial-spark-master:/jars/

# Submit job
docker exec financial-spark-master \
    spark-submit \
    --class com.financial.etl.transform.FinancialDataTransform \
    --master "spark://spark-master:7077" \
    --driver-memory 1g \
    --executor-memory 2g \
    --total-executor-cores 4 \
    /jars/financial-etl-1.0.0.jar \
    --source-path /data/raw \
    --target-path /data/curated \
    --execution-date $(date +%Y-%m-%d)

print_success "ETL transformation complete!"

print_info "Output files:"
find data/curated/processed -name "*.parquet" 2>/dev/null | head -5 || echo "  (Files in Docker volume)"

read -p "Press Enter to continue..."

################################################################################
# Step 5: Query Results
################################################################################

print_header "Step 5/5: Query Results from PostgreSQL"

print_step "Loading Parquet data to PostgreSQL..."

# Simple Python script to load Parquet
python << 'PYTHON_SCRIPT'
import pandas as pd
import psycopg2
from pathlib import Path
import sys

try:
    # Find parquet files
    parquet_files = list(Path('data/curated/processed').glob('**/*.parquet'))

    if not parquet_files:
        print("⚠️  No parquet files found. ETL may have failed.")
        sys.exit(1)

    # Read parquet
    df = pd.read_parquet(parquet_files[0])
    print(f"✅ Loaded {len(df)} records from Parquet")

    # Connect to PostgreSQL
    conn = psycopg2.connect(
        host='localhost',
        database='financial_dw',
        user='airflow',
        password='airflow'
    )

    # Create table if not exists
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS fact_stock_prices (
                id SERIAL PRIMARY KEY,
                symbol VARCHAR(10),
                trade_date DATE,
                close_price DECIMAL(18,4),
                volume BIGINT,
                sma_20 DECIMAL(18,4),
                daily_return DECIMAL(10,6)
            )
        """)
        conn.commit()

    # Load data (simple insert)
    for _, row in df.head(100).iterrows():  # Load first 100 rows for demo
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO fact_stock_prices
                (symbol, trade_date, close_price, volume, sma_20, daily_return)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (
                row.get('symbol'),
                row.get('trade_date'),
                row.get('close_price'),
                row.get('volume'),
                row.get('sma_20'),
                row.get('daily_return')
            ))

    conn.commit()
    conn.close()

    print(f"✅ Loaded sample data to PostgreSQL")

except Exception as e:
    print(f"❌ Error: {e}")
    sys.exit(1)
PYTHON_SCRIPT

print_success "Data loaded to PostgreSQL!"

print_step "Querying stock prices..."

docker exec -it financial-postgres psql -U airflow -d financial_dw << 'SQL'
-- Summary by symbol
\echo ''
\echo '📊 Summary by Symbol:'
\echo '======================================'
SELECT
    symbol,
    COUNT(*) as record_count,
    ROUND(AVG(close_price)::numeric, 2) as avg_price,
    ROUND(AVG(sma_20)::numeric, 2) as avg_sma_20,
    ROUND(AVG(daily_return)::numeric, 4) as avg_return
FROM fact_stock_prices
GROUP BY symbol
ORDER BY symbol;

-- Latest prices
\echo ''
\echo '📈 Latest Prices:'
\echo '======================================'
SELECT
    symbol,
    trade_date,
    close_price,
    sma_20,
    daily_return
FROM fact_stock_prices
ORDER BY trade_date DESC, symbol
LIMIT 10;
SQL

print_success "Query complete!"

################################################################################
# Summary
################################################################################

print_header "Demo Complete! 🎉"

echo ""
echo -e "${GREEN}✅ Pipeline Summary:${NC}"
echo "  1. Generated sample data for 5 stocks (90 days)"
echo "  2. Built Scala JAR (~35 MB)"
echo "  3. Started Docker Spark cluster"
echo "  4. Transformed JSON → Parquet (Scala + Spark)"
echo "  5. Loaded to PostgreSQL data warehouse"
echo ""

echo -e "${YELLOW}📊 Data Pipeline Flow:${NC}"
echo "  Alpha Vantage API (simulated)"
echo "        ↓"
echo "  JSON (data/raw/)"
echo "        ↓"
echo "  Scala + Spark ETL"
echo "        ↓"
echo "  Parquet (data/curated/)"
echo "        ↓"
echo "  PostgreSQL (financial_dw)"
echo ""

echo -e "${CYAN}🌐 Access Services:${NC}"
echo "  • Spark Master UI: http://localhost:8081"
echo "  • Airflow UI: http://localhost:8080 (admin/admin)"
echo "  • Grafana: http://localhost:3000 (admin/admin)"
echo ""

echo -e "${YELLOW}📚 Next Steps:${NC}"
echo "  1. View Spark UI to see job execution details"
echo "  2. Create Grafana dashboards for monitoring"
echo "  3. Add more technical indicators in Scala code"
echo "  4. Deploy to AWS (Glue + Redshift)"
echo ""

echo -e "${GREEN}🚀 Demo complete! Ready for interview presentation.${NC}"
echo ""

# Optional: Keep services running
read -p "Stop Docker services? (y/N): " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    print_step "Stopping Docker services..."
    docker-compose -f docker-compose-spark.yml down
    print_success "Services stopped"
else
    print_info "Services still running. Stop with: docker-compose -f docker-compose-spark.yml down"
fi

echo ""
echo -e "${CYAN}╔═══════════════════════════════════════════════════════════════╗${NC}"
echo -e "${CYAN}║${NC}  Thank you for trying the Financial Data Platform!           ${CYAN}║${NC}"
echo -e "${CYAN}╚═══════════════════════════════════════════════════════════════╝${NC}"
