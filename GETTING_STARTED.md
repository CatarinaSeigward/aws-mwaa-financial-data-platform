# Getting Started - Step-by-Step Setup Guide

This guide will walk you through setting up and running the Financial Data Platform from scratch using **Local Docker deployment**.

**Looking for AWS deployment?** See [AWS_FREE_TIER_DEPLOYMENT.md](AWS_FREE_TIER_DEPLOYMENT.md) instead.

---

## 📋 Table of Contents

1. [Prerequisites](#prerequisites)
2. [Installation](#installation)
3. [Configuration](#configuration)
4. [Running Your First Pipeline](#running-your-first-pipeline)
5. [Verifying the Results](#verifying-the-results)
6. [Troubleshooting](#troubleshooting)
7. [Next Steps](#next-steps)
8. [AWS Deployment](#aws-deployment)

---

## Prerequisites

### Required Software

Before starting, ensure you have the following installed on your system:

#### 1. **Docker Desktop**

Docker is required to run the Spark cluster and PostgreSQL database.

- **Windows/Mac**: Download from [https://www.docker.com/products/docker-desktop](https://www.docker.com/products/docker-desktop)
- **Linux**: Install via package manager
  ```bash
  # Ubuntu/Debian
  sudo apt-get update
  sudo apt-get install docker.io docker-compose

  # Start Docker service
  sudo systemctl start docker
  sudo systemctl enable docker
  ```

**Verify Installation:**
```bash
docker --version
# Expected output: Docker version 20.10.x or higher

docker-compose --version
# Expected output: docker-compose version 1.29.x or higher
```

#### 2. **Python 3.9+**

Python is used for data generation and Airflow orchestration.

- **Windows**: Download from [https://www.python.org/downloads/](https://www.python.org/downloads/)
- **Mac**: Use Homebrew
  ```bash
  brew install python@3.9
  ```
- **Linux**: Usually pre-installed, or use package manager
  ```bash
  sudo apt-get install python3.9 python3-pip
  ```

**Verify Installation:**
```bash
python --version
# Expected output: Python 3.9.x or higher

pip --version
# Expected output: pip 21.x or higher
```

#### 3. **Java JDK 11**

Java is required to compile Scala code and run Spark.

- **Windows/Mac/Linux**: Download from [https://adoptium.net/](https://adoptium.net/) (OpenJDK)
- **Or use package manager:**
  ```bash
  # Ubuntu/Debian
  sudo apt-get install openjdk-11-jdk

  # Mac
  brew install openjdk@11
  ```

**Verify Installation:**
```bash
java -version
# Expected output: openjdk version "11.0.x" or higher

echo $JAVA_HOME
# Should show path to Java installation
# If empty, set it:
# Windows: C:\Program Files\Java\jdk-11.x.x
# Mac/Linux: /usr/lib/jvm/java-11-openjdk
```

**Set JAVA_HOME (if not set):**
```bash
# Mac/Linux - Add to ~/.bashrc or ~/.zshrc
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk
export PATH=$JAVA_HOME/bin:$PATH

# Windows - Set via System Environment Variables
# JAVA_HOME = C:\Program Files\Java\jdk-11.x.x
# Add %JAVA_HOME%\bin to PATH
```

#### 4. **SBT (Scala Build Tool)**

SBT is used to compile Scala code and build JAR files.

- **Installation:**
  ```bash
  # Mac
  brew install sbt

  # Linux
  echo "deb https://repo.scala-sbt.org/scalasbt/debian all main" | sudo tee /etc/apt/sources.list.d/sbt.list
  curl -sL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x2EE0EA64E40A89B84B2DF73499E82A75642AC823" | sudo apt-key add
  sudo apt-get update
  sudo apt-get install sbt

  # Windows - Download MSI installer from
  # https://www.scala-sbt.org/download.html
  ```

**Verify Installation:**
```bash
sbt version
# Expected output: sbt version 1.9.x or higher
```

#### 5. **Git**

Git is used for version control and cloning the repository.

```bash
# Verify installation
git --version
# Expected output: git version 2.x.x or higher
```

### System Requirements

- **RAM**: Minimum 8GB (16GB recommended)
- **Disk Space**: At least 10GB free space
- **CPU**: 4 cores recommended
- **OS**: Windows 10+, macOS 10.14+, or Linux (Ubuntu 20.04+)

---

## Installation

### Step 1: Clone the Repository

Open your terminal and clone the project repository:

```bash
# Navigate to your projects directory
cd ~/projects  # or C:\Users\YourName\projects on Windows

# Clone the repository
git clone https://github.com/your-username/aws-mwaa-financial-data-platform.git

# Enter the project directory
cd aws-mwaa-financial-data-platform
```

**Verify:**
```bash
ls -la
# You should see directories: dags/, scripts/, src/, docker-compose-spark.yml, etc.
```

### Step 2: Install Python Dependencies

Install the required Python packages for data generation:

```bash
# Create a virtual environment (recommended)
python -m venv venv

# Activate the virtual environment
# Mac/Linux:
source venv/bin/activate
# Windows:
venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

**If `requirements.txt` doesn't exist, install manually:**
```bash
pip install pandas numpy requests alpha_vantage pyarrow
```

**Verify:**
```bash
python -c "import pandas, numpy; print('Python packages installed successfully')"
```

### Step 3: Verify Docker is Running

Ensure Docker Desktop is running:

```bash
# Test Docker
docker ps
# Should show an empty list or running containers (no errors)

# Test Docker Compose
docker-compose --version
```

**If you get permission errors on Linux:**
```bash
sudo usermod -aG docker $USER
# Log out and log back in for changes to take effect
```

---

## Configuration

### Step 4: Create Environment Configuration

Copy the example environment file and customize it:

```bash
# Copy the example file
cp .env.example .env

# Open the file in your text editor
# Windows: notepad .env
# Mac: open -e .env
# Linux: nano .env or vim .env
```

**Basic configuration for local development:**

```bash
# DEPLOYMENT MODE - Keep this as 'local' for Docker deployment
DEPLOYMENT_MODE=local  # DO NOT change this for local setup

# Data Source Configuration
DATA_SOURCE=simulated  # Use simulated data (no API key needed)

# Spark Configuration
SPARK_MASTER_URL=spark://spark-master:7077
SPARK_EXECUTOR_MEMORY=2g
SPARK_EXECUTOR_CORES=2

# Database Configuration
POSTGRES_HOST=financial-postgres
POSTGRES_PORT=5432
POSTGRES_DB=financial_dw
POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow

# Data Generator Settings
DATA_GENERATOR_PRESET=demo
DATA_GENERATOR_SYMBOLS=AAPL GOOGL MSFT AMZN TSLA
DATA_GENERATOR_START_DATE=2024-01-01
DATA_GENERATOR_END_DATE=2024-03-31
MARKET_CONDITION=normal
```

**Important:** Make sure `DEPLOYMENT_MODE=local` for Docker deployment. Only change this to `aws` if deploying to AWS (see [AWS_FREE_TIER_DEPLOYMENT.md](AWS_FREE_TIER_DEPLOYMENT.md)).

**Save and close the file.**

### Step 5: Verify Project Structure

Ensure all necessary directories exist:

```bash
# Create data directories if they don't exist
mkdir -p data/raw/sample
mkdir -p data/curated
mkdir -p data/validation_reports
mkdir -p logs

# Create Airflow directories
mkdir -p logs/airflow
```

**Verify:**
```bash
ls -la data/
# Should show: raw/, curated/, validation_reports/
```

---

## Running Your First Pipeline

### Step 6: Generate Sample Data

Generate simulated stock market data:

```bash
# Generate demo dataset (5 stocks, 90 days)
python scripts/data_generator.py --preset demo

# You should see output like:
# ✅ Generated 65 records for AAPL
# ✅ Generated 65 records for GOOGL
# ✅ Generated 65 records for MSFT
# ✅ Generated 65 records for AMZN
# ✅ Generated 65 records for TSLA
# Total: 325 records
```

**Verify data was created:**
```bash
# Check raw data directory
ls -la data/raw/sample/

# You should see directories like:
# date=2024-01-15/symbol=AAPL/
# date=2024-01-15/symbol=GOOGL/
# etc.

# View a sample file
cat data/raw/sample/date=2024-01-15/symbol=AAPL/AAPL_2024-01-15.json | head -20
```

### Step 7: Build the Scala ETL JAR

Compile the Scala code into an executable JAR file:

```bash
# Build the JAR (this may take 3-5 minutes on first run)
sbt clean assembly

# You should see output ending with:
# [success] Total time: XXX s
# Assembly JAR created at: target/scala-2.12/financial-etl-assembly-1.0.0.jar
```

**Verify the JAR was created:**
```bash
ls -lh target/scala-2.12/financial-etl-assembly-*.jar

# Should show a file ~50-100MB in size
```

### Step 8: Start Docker Environment

Launch the Spark cluster, Airflow, and PostgreSQL:

```bash
# Start all services in detached mode
docker-compose -f docker-compose-spark.yml up -d

# You should see output like:
# Creating network "financial-platform_default" ...
# Creating financial-postgres ...
# Creating spark-master ...
# Creating airflow-webserver ...
# etc.
```

**This may take 2-3 minutes the first time as Docker downloads images.**

**Verify all containers are running:**
```bash
docker-compose -f docker-compose-spark.yml ps

# Expected output (all containers should show "Up"):
# NAME                    STATUS
# spark-master           Up
# spark-worker-1         Up
# spark-worker-2         Up
# airflow-webserver      Up
# airflow-scheduler      Up
# financial-postgres     Up
```

**Check container logs if any failed:**
```bash
docker-compose -f docker-compose-spark.yml logs spark-master
```

### Step 9: Wait for Services to Initialize

Services need 1-2 minutes to fully initialize:

```bash
# Wait for Spark Master to be ready
echo "Waiting for Spark Master..."
sleep 30

# Check Spark Master UI (should open in browser)
# Visit: http://localhost:8081
```

**You should see:**
- Spark Master Web UI
- 2 workers listed
- Each worker with 2 cores and 2GB RAM

### Step 10: Run the ETL Pipeline

Execute the complete data pipeline:

```bash
# Run the build-and-submit script
./scripts/build-and-submit.sh local

# Expected output:
# ✅ Validating data quality...
# ✅ Submitting Spark job to cluster...
# ✅ Job completed successfully
# ✅ Loading data to PostgreSQL...
# ✅ Pipeline finished!
```

**If you see errors, check:**
1. All Docker containers are running: `docker-compose -f docker-compose-spark.yml ps`
2. Spark Master is accessible: Visit http://localhost:8081
3. Data files exist: `ls data/raw/sample/`

---

## Verifying the Results

### Step 11: Check Spark Job Execution

Open the Spark Master UI to see job execution:

```bash
# Open in browser
# Mac: open http://localhost:8081
# Linux: xdg-open http://localhost:8081
# Windows: start http://localhost:8081
```

**What to look for:**
- **Completed Applications**: Should show your ETL job
- **Execution Time**: Typically 10-30 seconds
- **Executors Used**: Should show 2 workers

### Step 12: Query the Data Warehouse

Connect to PostgreSQL and query the processed data:

```bash
# Connect to PostgreSQL container
docker exec -it financial-postgres psql -U airflow -d financial_dw

# You should see the PostgreSQL prompt:
# financial_dw=#
```

**Run sample queries:**

```sql
-- Check how many records were loaded
SELECT COUNT(*) FROM fact_stock_prices;
-- Expected: ~325 rows

-- View recent stock prices
SELECT
    symbol,
    trade_date,
    close_price,
    sma_20,
    daily_return
FROM fact_stock_prices
ORDER BY trade_date DESC, symbol
LIMIT 10;

-- Calculate average closing price by symbol
SELECT
    symbol,
    COUNT(*) as record_count,
    ROUND(AVG(close_price)::numeric, 2) as avg_price,
    ROUND(MIN(close_price)::numeric, 2) as min_price,
    ROUND(MAX(close_price)::numeric, 2) as max_price
FROM fact_stock_prices
GROUP BY symbol
ORDER BY symbol;

-- Exit PostgreSQL
\q
```

### Step 13: Check Data Quality Reports

Verify data validation reports were generated:

```bash
# List validation reports
ls -la data/validation_reports/

# View a sample report
cat data/validation_reports/validation_report_*.json | python -m json.tool
```

**Expected report structure:**
```json
{
  "validation_results": [
    {
      "symbol": "AAPL",
      "records_validated": 65,
      "errors": 0,
      "pass_rate": 1.0
    }
  ],
  "total_records": 325,
  "total_errors": 0,
  "overall_pass_rate": 1.0
}
```

### Step 14: Check Airflow Web UI

Open Airflow to see the DAG execution:

```bash
# Open Airflow UI in browser
# Visit: http://localhost:8080

# Default credentials (if prompted):
# Username: admin
# Password: admin
```

**What to check:**
- DAG: `financial_data_pipeline_scala`
- Last Run: Should show success (green)
- Tasks: All tasks should be green
- Duration: Typically 2-5 minutes

---

## Troubleshooting

### Common Issues and Solutions

#### Issue 1: Docker containers won't start

**Symptoms:**
```bash
docker-compose -f docker-compose-spark.yml ps
# Shows containers with "Exit 1" or "Restarting"
```

**Solutions:**
```bash
# Check logs for specific container
docker-compose -f docker-compose-spark.yml logs spark-master

# Common fixes:
# 1. Ensure ports are not in use
sudo lsof -i :8081  # Spark Master
sudo lsof -i :7077  # Spark
sudo lsof -i :5432  # PostgreSQL

# 2. Increase Docker memory (Docker Desktop Settings)
# Recommended: 4GB RAM minimum

# 3. Restart Docker
docker-compose -f docker-compose-spark.yml down
docker-compose -f docker-compose-spark.yml up -d

# 4. Remove old containers and volumes
docker-compose -f docker-compose-spark.yml down -v
docker system prune -f
docker-compose -f docker-compose-spark.yml up -d
```

#### Issue 2: SBT build fails

**Symptoms:**
```bash
sbt assembly
# Error: Could not find or load main class
```

**Solutions:**
```bash
# 1. Verify Java is installed and JAVA_HOME is set
echo $JAVA_HOME
java -version

# 2. Clean and rebuild
sbt clean
sbt compile
sbt assembly

# 3. If dependency issues, refresh
sbt update
rm -rf ~/.sbt
rm -rf ~/.ivy2/cache
sbt assembly
```

#### Issue 3: Data generator fails

**Symptoms:**
```bash
python scripts/data_generator.py --preset demo
# ModuleNotFoundError: No module named 'pandas'
```

**Solutions:**
```bash
# 1. Ensure virtual environment is activated
source venv/bin/activate  # Mac/Linux
venv\Scripts\activate     # Windows

# 2. Reinstall dependencies
pip install --upgrade pip
pip install pandas numpy pyarrow

# 3. Verify Python version
python --version  # Should be 3.9+
```

#### Issue 4: PostgreSQL connection fails

**Symptoms:**
```bash
docker exec -it financial-postgres psql -U airflow -d financial_dw
# psql: error: connection to server failed
```

**Solutions:**
```bash
# 1. Check if PostgreSQL container is running
docker ps | grep postgres

# 2. Check PostgreSQL logs
docker logs financial-postgres

# 3. Restart PostgreSQL container
docker-compose -f docker-compose-spark.yml restart financial-postgres

# 4. Wait 30 seconds and try again
sleep 30
docker exec -it financial-postgres psql -U airflow -d financial_dw
```

#### Issue 5: Spark job fails

**Symptoms:**
- Spark UI shows failed job
- Error in logs: "Out of Memory" or "Connection refused"

**Solutions:**
```bash
# 1. Check Spark Master logs
docker logs spark-master

# 2. Increase worker memory (edit docker-compose-spark.yml)
# Change: SPARK_WORKER_MEMORY=2G → SPARK_WORKER_MEMORY=4G

# 3. Restart Spark cluster
docker-compose -f docker-compose-spark.yml restart spark-master spark-worker-1 spark-worker-2

# 4. Reduce data size for testing
python scripts/data_generator.py --preset demo --days 30
```

#### Issue 6: Permission denied on scripts

**Symptoms:**
```bash
./scripts/build-and-submit.sh local
# Permission denied
```

**Solutions:**
```bash
# Make scripts executable
chmod +x scripts/*.sh

# Run again
./scripts/build-and-submit.sh local
```

### Getting Help

If you encounter issues not covered here:

1. **Check logs:**
   ```bash
   # Docker container logs
   docker-compose -f docker-compose-spark.yml logs

   # Specific container
   docker logs spark-master
   docker logs financial-postgres
   ```

2. **Search GitHub Issues:**
   Visit the project's GitHub repository and search existing issues.

3. **Create a new issue:**
   Include:
   - Operating system and version
   - Docker version
   - Python version
   - Complete error message
   - Steps to reproduce

---

## Next Steps

### Congratulations! 🎉

You've successfully set up and run the Financial Data Platform. Here's what to explore next:

### 1. **Customize the Data Generator**

Generate different datasets:

```bash
# Generate 1 year of data for 10 stocks
python scripts/data_generator.py \
    --symbols AAPL GOOGL MSFT AMZN TSLA NVDA META JPM BAC WMT \
    --start-date 2023-01-01 \
    --end-date 2023-12-31 \
    --market-condition volatile
```

See [DATA_GENERATION_GUIDE.md](DATA_GENERATION_GUIDE.md) for details.

### 2. **Explore the Scala Code**

Understand the ETL logic:

```bash
# Open the main transformation file
cat src/transformation/scala/FinancialDataTransform.scala
```

Key sections to review:
- `readRawData()` - Data ingestion (src/transformation/scala/FinancialDataTransform.scala:120)
- `validateDataQuality()` - Validation logic (src/transformation/scala/FinancialDataTransform.scala:180)
- `calculateMovingAverages()` - Window functions (src/transformation/scala/FinancialDataTransform.scala:240)
- `loadToPostgres()` - Data warehouse loading (src/transformation/scala/FinancialDataTransform.scala:350)

### 3. **Run Advanced Queries**

Analyze the data with complex SQL:

```sql
-- Calculate 50-day moving average and volatility
SELECT
    symbol,
    trade_date,
    close_price,
    sma_50,
    volatility_20d,
    CASE
        WHEN close_price > sma_50 THEN 'BULLISH'
        ELSE 'BEARISH'
    END as trend
FROM fact_stock_prices
WHERE trade_date >= '2024-02-01'
ORDER BY trade_date DESC, symbol;

-- Find highest volatility periods
SELECT
    symbol,
    trade_date,
    volatility_20d,
    close_price
FROM fact_stock_prices
WHERE volatility_20d > 0.30
ORDER BY volatility_20d DESC
LIMIT 20;
```

### 4. **Schedule Automated Runs**

Set up daily pipeline execution with Airflow:

1. Open Airflow UI: http://localhost:8080
2. Enable the DAG: `financial_data_pipeline_scala`
3. Set schedule: Daily at 6 AM
4. Monitor execution in the UI

### 5. **Deploy to AWS (Optional)**

For production deployment, see:
- [SCALA_SPARK_GUIDE.md](SCALA_SPARK_GUIDE.md) - AWS Glue deployment
- [COST_ANALYSIS_SCALA.md](COST_ANALYSIS_SCALA.md) - Cost optimization strategies

### 6. **Add New Features**

Extend the pipeline with:
- More technical indicators (RSI, MACD, Bollinger Bands)
- Real-time data ingestion
- Machine learning predictions
- Interactive dashboards with Grafana

### 7. **Prepare for Interviews**

Use the 5-minute demo script:

```bash
./scripts/quick-demo.sh
```

Practice explaining:
- Architecture decisions
- Scala vs Python trade-offs
- Cost optimization strategies
- Data quality validation approach

---

## Useful Commands Reference

### Docker Management

```bash
# Start all services
docker-compose -f docker-compose-spark.yml up -d

# Stop all services
docker-compose -f docker-compose-spark.yml down

# Restart specific service
docker-compose -f docker-compose-spark.yml restart spark-master

# View logs
docker-compose -f docker-compose-spark.yml logs -f

# Remove all containers and volumes
docker-compose -f docker-compose-spark.yml down -v
```

### Data Operations

```bash
# Generate data
python scripts/data_generator.py --preset demo

# Validate data
python scripts/validate_data.py

# Clean data directories
rm -rf data/raw/* data/curated/* data/validation_reports/*
```

### Spark Job Management

```bash
# Submit job locally
./scripts/build-and-submit.sh local

# Submit to cluster
./scripts/build-and-submit.sh cluster

# View Spark UI
open http://localhost:8081
```

### Database Operations

```bash
# Connect to PostgreSQL
docker exec -it financial-postgres psql -U airflow -d financial_dw

# Dump database
docker exec financial-postgres pg_dump -U airflow financial_dw > backup.sql

# Restore database
docker exec -i financial-postgres psql -U airflow financial_dw < backup.sql

# Reset database
docker exec financial-postgres psql -U airflow -d financial_dw -c "DROP TABLE IF EXISTS fact_stock_prices CASCADE;"
```

---

## AWS Deployment

### Want to Deploy to AWS?

If you want to deploy this project to AWS Free Tier for an online portfolio:

**See:** [AWS_FREE_TIER_DEPLOYMENT.md](AWS_FREE_TIER_DEPLOYMENT.md)

**Quick Summary:**
1. Configure AWS CLI: `aws configure`
2. Run deployment script: `./scripts/deploy-to-aws.sh`
3. Access via public URL: `http://<PUBLIC_IP>:8080`

**Cost:** $0-3/month (within AWS Free Tier for first 12 months)

**Advantages:**
- ✅ 24/7 availability
- ✅ Public URL to share with recruiters
- ✅ Real cloud environment experience
- ✅ Demonstrates AWS skills (EC2, S3, IAM, CloudFormation)

**Comparison:**

| Feature | Local Docker | AWS Free Tier |
|---------|--------------|---------------|
| Cost | $0 | $0-3/month |
| Setup Time | 10 minutes | 30 minutes |
| Internet Required | No | Yes |
| Public Access | No | Yes (http://x.x.x.x) |
| Best For | Interview demos | Online portfolio |

---

## Additional Resources

### Documentation
- **Local Setup**: This guide (GETTING_STARTED.md)
- **AWS Setup**: [AWS_FREE_TIER_DEPLOYMENT.md](AWS_FREE_TIER_DEPLOYMENT.md)
- **AWS Summary**: [AWS_DEPLOYMENT_SUMMARY.md](AWS_DEPLOYMENT_SUMMARY.md)
- **Scala Guide**: [SCALA_SPARK_GUIDE.md](SCALA_SPARK_GUIDE.md)
- **Cost Analysis**: [COST_ANALYSIS_SCALA.md](COST_ANALYSIS_SCALA.md)

### Local Services
- **Airflow UI**: http://localhost:8080
- **Spark UI**: http://localhost:8081
- **Grafana**: http://localhost:3000 (if enabled)

### Sample Data
- Pre-generated samples in `data/raw/sample/`
- Configuration examples in `.env.example`

---

## Support

If you found this guide helpful:
- ⭐ Star the project on GitHub
- 📝 Share your experience
- 🐛 Report bugs via GitHub Issues
- 💡 Suggest improvements via Pull Requests

---

**Happy Data Engineering! 🚀**

Made with ❤️ for Data Engineering Job Seekers
