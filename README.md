# Financial Data Platform

[![CI Pipeline](https://github.com/your-username/aws-mwaa-financial-data-platform/actions/workflows/ci.yml/badge.svg)](https://github.com/your-username/aws-mwaa-financial-data-platform/actions/workflows/ci.yml)
[![Scala 2.12](https://img.shields.io/badge/scala-2.12-red.svg)](https://www.scala-lang.org/)
[![Spark 3.4](https://img.shields.io/badge/spark-3.4-orange.svg)](https://spark.apache.org/)

A distributed Spark ETL platform in Scala for financial time-series analytics, orchestrated by AWS MWAA (Airflow) with idempotent task design, automated via CloudFormation and Docker.

## Architecture

```
Data Source (API / GBM Simulator)
        |
  Great Expectations (fail-fast validation, 17 rules)
        |
  Scala Spark ETL (typed Dataset API + AQE)
  - Dataset[StockRecord] -> Dataset[CleansedRecord] -> Dataset[EnrichedStockRecord]
  - Window functions: SMA, EMA, volatility, daily returns
  - Skew handling via salting for hot symbols
        |
  Curated Parquet (partitioned by year/month)
        |
  PostgreSQL / Redshift (idempotent upsert)
```

## Tech Stack

| Component | Technology |
|-----------|-----------|
| ETL Engine | Scala 2.12 + Spark 3.4.1 (typed Dataset API, AQE) |
| Orchestration | Airflow 2.8.1 (MWAA-compatible, idempotent tasks) |
| Data Quality | Great Expectations (17 validation rules) |
| Data Simulator | Geometric Brownian Motion (log-normal returns) |
| Infrastructure | CloudFormation (VPC, S3, MWAA, Glue, Redshift) |
| Local Dev | Docker Compose (Spark cluster, Airflow, PostgreSQL, Grafana) |
| CI/CD | GitHub Actions (lint, test, build, stress-test) |

## Quick Start

```bash
# Generate synthetic data (GBM simulation)
python scripts/data_generator.py --preset demo

# Build Scala fat JAR
sbt assembly

# Start local environment
docker-compose -f docker-compose-spark.yml up -d

# Run pipeline
./scripts/build-and-submit.sh local
```

One-command demo: `./scripts/quick-demo.sh`

## Key Design Decisions

**Typed Dataset API** - The ETL pipeline uses `Dataset[StockRecord]`, `Dataset[CleansedRecord]`, and `Dataset[EnrichedStockRecord]` with case classes for compile-time type safety. Cleansing uses type-safe `.map()` and `.filter()` lambdas instead of untyped column expressions.

**Adaptive Query Execution** - AQE is enabled with dynamic partition coalescing and automatic skew join handling. Skewed symbols (AAPL, TSLA, etc.) are salted across partitions to prevent bottlenecks during window function computations.

**Geometric Brownian Motion** - The data simulator implements the GBM closed-form solution `S(t+dt) = S(t) * exp((mu - sigma^2/2)*dt + sigma*sqrt(dt)*Z)` with annualized drift and volatility parameters per stock, producing log-normally distributed returns.

**Idempotent Tasks** - Every Airflow task is safe to retry/backfill. Ingestion cleans the target partition before writing, Spark writes with `SaveMode.Overwrite`, and warehouse loading uses DELETE+INSERT in a single transaction keyed on `trade_date`.

**CI/CD Stress Testing** - The pipeline includes a stress-test job that generates large-scale data (8 symbols x 365 days), validates OHLC integrity and GBM properties across 1000+ records, and runs Great Expectations against the full volume.

## Technical Indicators

| Indicator | Window |
|-----------|--------|
| SMA (5, 20, 50) | Rolling average |
| EMA (12, 26) | Exponential weighted |
| Volatility | 20-day rolling stddev |
| Daily Return | Close-to-close % change |
| Daily Range | High - Low |
| Volume Change | Volume % change |

## Project Structure

```
src/transformation/scala/FinancialDataTransform.scala   # Typed Dataset ETL
src/validation/data_validator.py                         # Great Expectations
src/ingestion/alpha_vantage_client.py                    # API client
scripts/data_generator.py                                # GBM simulator
dags/financial_data_pipeline_scala.py                    # Idempotent Airflow DAG
infrastructure/cloudformation/main.yaml                  # AWS IaC (858 lines)
config/expectations/stock_data_suite.json                # 17 validation rules
docker-compose-spark.yml                                 # Local dev environment
.github/workflows/ci.yml                                 # CI/CD + stress test
build.sbt                                                # Scala build config
```

## Deployment

| Mode | Cost | How |
|------|------|-----|
| Local Docker | $0 | `docker-compose -f docker-compose-spark.yml up -d` |
| AWS Free Tier | $0-3/mo | `./scripts/deploy-to-aws.sh` |
| AWS Production | CloudFormation | See [Deployment Guide](AWS_FREE_TIER_DEPLOYMENT.md) |

## Tests

```bash
pytest tests/ -v                    # Python unit tests
pytest tests/ --cov=src             # With coverage
sbt test                            # Scala tests
```

## License

MIT
