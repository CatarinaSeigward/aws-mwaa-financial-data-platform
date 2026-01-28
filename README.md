# Financial Data Platform - Scala + Spark ETL Pipeline

[![CI Pipeline](https://github.com/your-username/aws-mwaa-financial-data-platform/actions/workflows/ci.yml/badge.svg)](https://github.com/your-username/aws-mwaa-financial-data-platform/actions/workflows/ci.yml)
[![Python 3.11](https://img.shields.io/badge/python-3.11-blue.svg)](https://www.python.org/downloads/)
[![Scala 2.12](https://img.shields.io/badge/scala-2.12-red.svg)](https://www.scala-lang.org/)
[![Spark 3.4](https://img.shields.io/badge/spark-3.4-orange.svg)](https://spark.apache.org/)

**Enterprise-Grade Financial Data ETL Pipeline** - Scala + Spark for Time-Series Analytics

---

## Project Overview

A production-ready ETL platform built with **Scala + Spark** for financial time-series processing. Features window functions for technical indicator calculations (SMA, EMA, volatility), adaptive shuffle partitions for optimized performance, and comprehensive data validation with Great Expectations.

### Key Features

- **Scala + Spark 3.4.1** - Type-safe Dataset API with compile-time checks
- **Window Functions** - SMA, EMA, volatility calculations for time-series analytics
- **Adaptive Query Execution** - Dynamic shuffle partition optimization
- **Data Quality Gates** - Great Expectations integration with 17 validation rules
- **Synthetic Data Engine** - Geometric Brownian Motion for realistic market simulation
- **Dual Deployment** - Local Docker ($0) or AWS MWAA (managed Airflow)
- **Infrastructure as Code** - CloudFormation templates for AWS resources
- **CI/CD Pipeline** - GitHub Actions for automated testing and validation

---

## Quick Start

### Option 1: Local Docker (Recommended for Demo)

```bash
# 1. Generate sample data
python scripts/data_generator.py --preset demo

# 2. Build Scala JAR
sbt assembly

# 3. Start Docker environment
docker-compose -f docker-compose-spark.yml up -d

# 4. Run ETL pipeline
./scripts/build-and-submit.sh local

# 5. Query results
docker exec -it financial-postgres psql -U airflow -d financial_dw \
    -c "SELECT symbol, trade_date, close_price, sma_20, volatility_20d FROM fact_stock_prices LIMIT 10;"
```

**One-command demo:**
```bash
./scripts/quick-demo.sh
```

### Option 2: AWS Deployment

```bash
# Deploy infrastructure via CloudFormation
./scripts/deploy-to-aws.sh
```

---

## Technology Stack

| Component | Technology | Purpose |
|-----------|------------|---------|
| **Data Processing** | Scala 2.12 + Spark 3.4.1 | Type-safe ETL transformations |
| **Orchestration** | Apache Airflow 2.8.1 | Workflow scheduling (MWAA compatible) |
| **Data Warehouse** | PostgreSQL 15 / Redshift Serverless | Analytics storage |
| **Data Quality** | Great Expectations 0.18+ | Validation with 17 rules |
| **Data Generation** | Python + NumPy | Geometric Brownian Motion simulation |
| **Infrastructure** | CloudFormation | AWS resource provisioning |
| **CI/CD** | GitHub Actions | Automated testing pipeline |
| **Containerization** | Docker Compose | Local development environment |

---

## Architecture

```
                    +------------------+
                    |  Data Ingestion  |
                    | (API / Generator)|
                    +--------+---------+
                             |
                             v
                    +------------------+
                    | Great Expectations|
                    |  Data Validation |
                    +--------+---------+
                             |
                             v
+------------------+   +------------------+   +------------------+
|   Spark Master   |-->| Scala ETL Job   |-->|  Curated Data    |
|                  |   | (Window Funcs)  |   | (Parquet/S3)     |
+------------------+   +------------------+   +------------------+
                             |
                             v
                    +------------------+
                    |  Data Warehouse  |
                    | (PostgreSQL/     |
                    |  Redshift)       |
                    +------------------+
```

---

## Technical Highlights

### Window Functions for Time-Series Analytics

```scala
// Simple Moving Average (SMA)
val windowSpec = Window
  .partitionBy("symbol")
  .orderBy("trade_date")
  .rowsBetween(-period + 1, 0)

df.withColumn(s"sma_$period", avg("close_price").over(windowSpec))

// Exponential Moving Average (EMA)
// Volatility (20-day rolling standard deviation)
// Daily returns and price ranges
```

### Adaptive Shuffle Partition Configuration

```scala
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.shuffle.partitions", "200")
```

### Data Skew Handling

The ETL implements salting strategy for high-volume symbols:
- Analyzes symbol distribution to detect skew
- Applies salt keys for hot symbols (e.g., AAPL, TSLA)
- Redistributes data evenly across partitions

### Great Expectations Validation Rules

17 validation rules including:
- Schema validation (required columns)
- NULL checks for critical fields
- Value range validation (prices, volumes)
- OHLC integrity (high >= low)
- Data freshness checks

---

## Project Structure

```
.
├── src/
│   ├── transformation/
│   │   └── scala/
│   │       └── FinancialDataTransform.scala  # Main Scala ETL
│   └── validation/
│       └── data_validator.py                 # Great Expectations
├── tests/
│   ├── test_data_validator.py               # Validation tests
│   ├── test_data_generator.py               # Generator tests
│   └── test_transformations.py              # Transform tests
├── dags/
│   └── financial_data_pipeline_scala.py     # Airflow DAG
├── scripts/
│   ├── data_generator.py                    # Synthetic data
│   ├── quick-demo.sh                        # One-command demo
│   └── build-and-submit.sh                  # Spark submit
├── infrastructure/
│   └── cloudformation/
│       └── main.yaml                        # AWS resources
├── config/
│   ├── settings.py                          # Configuration
│   └── expectations/
│       └── stock_data_suite.json            # Validation rules
├── .github/
│   └── workflows/
│       └── ci.yml                           # CI/CD pipeline
├── docker-compose-spark.yml                 # Local environment
├── build.sbt                                # Scala build config
├── requirements.txt                         # Python dependencies
└── pytest.ini                               # Test configuration
```

---

## Running Tests

```bash
# Run all Python tests
pytest tests/ -v

# Run with coverage
pytest tests/ --cov=src --cov-report=html

# Run specific test file
pytest tests/test_data_validator.py -v

# Run Scala tests
sbt test
```

---

## Technical Indicators Calculated

| Indicator | Description | Window |
|-----------|-------------|--------|
| SMA_5 | Simple Moving Average | 5 days |
| SMA_20 | Simple Moving Average | 20 days |
| SMA_50 | Simple Moving Average | 50 days |
| EMA_12 | Exponential Moving Average | 12 days |
| EMA_26 | Exponential Moving Average | 26 days |
| Volatility_20d | Rolling Standard Deviation | 20 days |
| Daily_Return | Close-to-close % change | 1 day |
| Daily_Range | High - Low | 1 day |
| Volume_Change_Pct | Volume % change | 1 day |

---

## Interview Talking Points

> "This ETL platform processes financial time-series data using **Scala + Spark** with the Dataset API for type-safe transformations. I implemented **window functions** for calculating technical indicators like moving averages and volatility.
>
> For performance optimization, I configured **adaptive shuffle partitions** and implemented a **data skew handling strategy** using salting for high-volume symbols like AAPL and TSLA.
>
> Data quality is enforced through **Great Expectations** with 17 validation rules that act as a gate before data enters the warehouse.
>
> The infrastructure is automated via **CloudFormation**, and the pipeline runs on **AWS MWAA** (managed Airflow) in production, with a Docker Compose setup for local development parity.
>
> The project includes a **CI/CD pipeline** with GitHub Actions that runs Python tests, builds the Scala JAR, validates CloudFormation templates, and checks data quality."

---

## Cost Analysis

| Environment | Monthly Cost | Notes |
|-------------|--------------|-------|
| **Local Docker** | $0 | Development & demos |
| **AWS Free Tier** | $0-3 | Portfolio hosting |
| **AWS Production** | $26K+ | Enterprise scale |

---

## Documentation

- [Getting Started Guide](GETTING_STARTED.md)
- [Scala + Spark Guide](SCALA_SPARK_GUIDE.md)
- [AWS Deployment](AWS_FREE_TIER_DEPLOYMENT.md)
- [Data Generation Guide](DATA_GENERATION_GUIDE.md)
- [Cost Analysis](COST_ANALYSIS_SCALA.md)

---

## Contributing

1. Fork the repository
2. Create feature branch (`git checkout -b feature/AmazingFeature`)
3. Run tests (`pytest tests/ && sbt test`)
4. Commit changes (`git commit -m 'Add AmazingFeature'`)
5. Push to branch (`git push origin feature/AmazingFeature`)
6. Open Pull Request

---

## License

MIT License - see [LICENSE](LICENSE) for details.

---

**Built for Data Engineering interviews** | Scala + Spark + Airflow + AWS
