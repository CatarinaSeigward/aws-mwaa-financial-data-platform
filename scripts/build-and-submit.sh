#!/bin/bash
################################################################################
# Build and Submit Spark Job Script
# ==================================
# 构建 Scala JAR 并提交到 Spark 集群
#
# Usage:
#   ./scripts/build-and-submit.sh [local|cluster]
#
# Examples:
#   ./scripts/build-and-submit.sh local       # 本地模式运行
#   ./scripts/build-and-submit.sh cluster     # 集群模式运行
################################################################################

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
JAR_NAME="financial-etl-1.0.0.jar"
JAR_PATH="${PROJECT_ROOT}/target/scala-2.12/${JAR_NAME}"
MAIN_CLASS="com.financial.etl.transform.FinancialDataTransform"

# Default values
MODE="${1:-local}"
SOURCE_PATH="${PROJECT_ROOT}/data/raw"
TARGET_PATH="${PROJECT_ROOT}/data/curated"
EXECUTION_DATE=$(date +%Y-%m-%d)

################################################################################
# Functions
################################################################################

print_header() {
    echo -e "${BLUE}================================${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}================================${NC}"
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

check_sbt() {
    if ! command -v sbt &> /dev/null; then
        print_error "SBT is not installed"
        echo ""
        echo "Install SBT:"
        echo "  macOS:   brew install sbt"
        echo "  Ubuntu:  sudo apt-get install sbt"
        echo "  Windows: choco install sbt"
        exit 1
    fi
    print_success "SBT found: $(sbt --version | grep 'sbt script version')"
}

check_spark() {
    if ! command -v spark-submit &> /dev/null; then
        print_error "spark-submit not found"
        echo ""
        echo "Options:"
        echo "  1. Install Spark locally"
        echo "  2. Use Docker Compose: docker-compose -f docker-compose-spark.yml up"
        exit 1
    fi
    print_success "Spark found: $(spark-submit --version 2>&1 | grep version | head -1)"
}

build_jar() {
    print_header "Building Scala JAR"

    cd "$PROJECT_ROOT"

    print_info "Cleaning previous builds..."
    sbt clean

    print_info "Compiling Scala code..."
    sbt compile

    print_info "Running tests..."
    sbt test || print_error "Tests failed (continuing anyway...)"

    print_info "Creating assembly JAR..."
    sbt assembly

    if [ -f "$JAR_PATH" ]; then
        JAR_SIZE=$(du -h "$JAR_PATH" | cut -f1)
        print_success "JAR built successfully: $JAR_PATH ($JAR_SIZE)"
    else
        print_error "JAR build failed"
        exit 1
    fi
}

submit_local() {
    print_header "Submitting to Local Spark"

    print_info "Configuration:"
    echo "  Mode: Local[*]"
    echo "  Source: $SOURCE_PATH"
    echo "  Target: $TARGET_PATH"
    echo "  Date: $EXECUTION_DATE"

    spark-submit \
        --class "$MAIN_CLASS" \
        --master "local[*]" \
        --driver-memory 2g \
        --executor-memory 2g \
        --conf spark.sql.shuffle.partitions=4 \
        --conf spark.default.parallelism=4 \
        "$JAR_PATH" \
        --source-path "$SOURCE_PATH" \
        --target-path "$TARGET_PATH" \
        --execution-date "$EXECUTION_DATE"
}

submit_cluster() {
    print_header "Submitting to Spark Cluster"

    # Check if Docker Spark cluster is running
    if ! docker ps | grep -q "financial-spark-master"; then
        print_error "Spark cluster is not running"
        echo ""
        echo "Start the cluster:"
        echo "  docker-compose -f docker-compose-spark.yml up -d"
        exit 1
    fi

    print_info "Configuration:"
    echo "  Mode: Cluster"
    echo "  Master: spark://spark-master:7077"
    echo "  Source: /data/raw"
    echo "  Target: /data/curated"
    echo "  Date: $EXECUTION_DATE"

    # Copy JAR to Spark master container
    print_info "Copying JAR to Spark master..."
    docker cp "$JAR_PATH" financial-spark-master:/jars/

    # Submit via Docker
    docker exec financial-spark-master \
        spark-submit \
        --class "$MAIN_CLASS" \
        --master "spark://spark-master:7077" \
        --driver-memory 1g \
        --executor-memory 2g \
        --total-executor-cores 4 \
        --conf spark.sql.shuffle.partitions=8 \
        "/jars/${JAR_NAME}" \
        --source-path "/data/raw" \
        --target-path "/data/curated" \
        --execution-date "$EXECUTION_DATE"
}

submit_aws_glue() {
    print_header "Submitting to AWS Glue"

    print_info "Not implemented yet"
    print_info "For AWS Glue, upload JAR to S3 and configure Glue Job"

    echo ""
    echo "Steps:"
    echo "  1. Upload JAR to S3:"
    echo "     aws s3 cp $JAR_PATH s3://your-bucket/glue-jars/"
    echo ""
    echo "  2. Create/Update Glue Job:"
    echo "     aws glue update-job --job-name financial-transform \\"
    echo "       --job-update ScriptLocation=s3://your-bucket/glue-jars/${JAR_NAME}"
}

show_spark_ui() {
    print_header "Spark UI Links"

    if [ "$MODE" == "local" ]; then
        echo "  Local Spark UI: http://localhost:4040"
    elif [ "$MODE" == "cluster" ]; then
        echo "  Spark Master UI: http://localhost:8081"
        echo "  Spark Application UI: http://localhost:4040"
    fi
}

################################################################################
# Main Script
################################################################################

print_header "Scala + Spark ETL Build & Submit"

# Parse arguments
case "$MODE" in
    local)
        print_info "Mode: Local Spark"
        ;;
    cluster)
        print_info "Mode: Docker Spark Cluster"
        ;;
    aws)
        print_info "Mode: AWS Glue"
        ;;
    *)
        print_error "Invalid mode: $MODE"
        echo ""
        echo "Usage: $0 [local|cluster|aws]"
        exit 1
        ;;
esac

# Pre-flight checks
print_info "Running pre-flight checks..."
check_sbt

if [ "$MODE" != "aws" ]; then
    check_spark
fi

# Build JAR
if [ ! -f "$JAR_PATH" ] || [ "$2" == "--rebuild" ]; then
    build_jar
else
    print_info "Using existing JAR: $JAR_PATH"
    print_info "Use --rebuild flag to force rebuild"
fi

# Submit job
case "$MODE" in
    local)
        submit_local
        ;;
    cluster)
        submit_cluster
        ;;
    aws)
        submit_aws_glue
        ;;
esac

# Show UI links
show_spark_ui

print_success "Job submission complete!"

echo ""
print_info "Check output at: $TARGET_PATH"

# Optional: Show output
if [ -d "$TARGET_PATH/processed" ]; then
    echo ""
    print_info "Output files:"
    find "$TARGET_PATH/processed" -type f -name "*.parquet" | head -5
fi
