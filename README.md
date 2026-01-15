# Financial Data Platform - Scala + Spark ETL Pipeline

🚀 **Enterprise-Grade Financial Data ETL Pipeline** - Scala + Spark for Large-Scale Data Processing

**🎯 Perfect Portfolio Project for Job Seekers** - Run locally at zero cost, scalable to AWS production

---

## 🌟 Project Highlights

- **✅ Scala + Spark 3.4.1** - Type-safe distributed data processing
- **✅ Zero-Cost Execution** - Docker local environment, no AWS required
- **✅ No API Keys Needed** - Built-in data generator, 100% stable
- **✅ 5-Minute Demo** - One-command full pipeline execution
- **✅ Production-Ready** - Switch to AWS (Glue + Redshift) when needed

---

## 🚀 Quick Start

### Option 1: Local Docker (Default - $0/month)

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
    -c "SELECT * FROM fact_stock_prices LIMIT 10;"
```

**Or use the one-command demo:**
```bash
./scripts/quick-demo.sh
```

### Option 2: AWS Free Tier ($0-3/month)

```bash
# 1. Configure AWS CLI
aws configure

# 2. Deploy to AWS with one command
./scripts/deploy-to-aws.sh

# 3. Access via public URL
# Airflow: http://<PUBLIC_IP>:8080
# Spark UI: http://<PUBLIC_IP>:8081
```

📖 **First time setup?** See [GETTING_STARTED.md](GETTING_STARTED.md) for local deployment or [AWS_FREE_TIER_DEPLOYMENT.md](AWS_FREE_TIER_DEPLOYMENT.md) for AWS deployment.

---

## 💰 Cost Comparison

| Solution | Monthly Cost | Deployment | Notes |
|----------|--------------|------------|-------|
| **Docker Local** | **$0** | Local machine | ✅ Best for interview demos |
| **AWS Free Tier** | **$0-3** | EC2 + S3 | ✅ Best for online portfolio |
| AWS Optimized | $26,150 | Full AWS | Production-ready |
| AWS Original | $174,323 | Full AWS | ❌ Too expensive |

**Savings: 99.998%** 💰 (AWS Free Tier vs AWS Original)

Detailed analysis: [COST_ANALYSIS_SCALA.md](COST_ANALYSIS_SCALA.md)

---

## 🛠️ Technology Stack

### Core Technologies
- **Scala 2.12** + **Spark 3.4.1** - Data processing engine
- **Airflow 2.8.1** - Workflow orchestration
- **PostgreSQL 15** - Data warehouse
- **Docker** - Containerized deployment

### AWS Technologies (Optional)
- **EC2** - Compute instances (t2.micro Free Tier)
- **S3** - Object storage for raw/processed data
- **IAM** - Access management with instance profiles
- **CloudFormation** - Infrastructure as Code
- **CloudWatch** - Monitoring and logging

---

## 📊 Data Acquisition

### Method 1: Simulated Data Generation ✅ (Default)

```bash
python scripts/data_generator.py --preset demo
```

**Advantages:**
- ✅ No API Key required
- ✅ 100% stable
- ✅ No rate limits
- ✅ Realistic price movements using Geometric Brownian Motion

### Method 2: Real API (Optional)

```bash
export ALPHA_VANTAGE_API_KEY="your_key"
# Modify DAG to use fetch_stock_data_from_api()
```

---

## 📚 Documentation

### Setup Guides
- [Getting Started Guide](GETTING_STARTED.md) - Local Docker setup (step-by-step)
- [AWS Free Tier Deployment](AWS_FREE_TIER_DEPLOYMENT.md) - AWS deployment guide
- [AWS Deployment Summary](AWS_DEPLOYMENT_SUMMARY.md) - Quick AWS overview

### Technical Documentation
- [Scala + Spark Guide](SCALA_SPARK_GUIDE.md) - Implementation details
- [Data Generation Guide](DATA_GENERATION_GUIDE.md) - Data generator usage
- [Cost Analysis Report](COST_ANALYSIS_SCALA.md) - Detailed cost breakdown
- [Environment Configuration](.env.example) - Configuration template

---

## 🎯 Interview Presentation

### 5-Minute Demo Flow

1. Run `./scripts/quick-demo.sh`
2. Show Spark UI (localhost:8081)
3. Query PostgreSQL results
4. Explain Scala code highlights

### Key Talking Points

> "This is an enterprise-grade ETL pipeline designed with a **dual-deployment architecture**. The same codebase supports both local Docker ($0/month) and AWS Free Tier ($0-3/month) deployments through environment-based configuration.
>
> For portfolio demonstration, I primarily use the local version for instant demos. But I can also deploy to AWS with a single command to showcase cloud skills - the complete AWS infrastructure would cost $174K/month in production, but my optimized Free Tier implementation costs essentially nothing.
>
> The architecture uses Scala + Spark for type-safe distributed processing, with Airflow orchestrating the workflow. Data paths automatically switch between local filesystem and S3 based on deployment mode, demonstrating infrastructure portability and cost optimization strategies."

**Technical Highlights to Mention:**
- Dual-deployment architecture (local + AWS) with environment-based switching
- Infrastructure as Code with CloudFormation
- Type-safe data transformations with compile-time checks
- Distributed processing with Spark partitioning
- S3 integration for cloud-native data storage
- Window functions for time-series analytics (SMA, EMA, volatility)
- Data quality validation with configurable thresholds
- IAM roles and security best practices
- 99.998% cost reduction compared to full AWS production setup

---

## 📁 Project Structure

```
├── src/transformation/scala/      # Scala ETL logic
│   └── FinancialDataTransform.scala
├── scripts/                       # Data generation and deployment
│   ├── data_generator.py         # Stock data generator
│   ├── quick-demo.sh             # One-command demo
│   └── build-and-submit.sh       # Build and submit Spark jobs
├── dags/                          # Airflow DAG definitions
│   └── financial_data_pipeline_scala.py
├── docker-compose-spark.yml       # Docker environment
├── build.sbt                      # Scala project configuration
├── data/                          # Data storage
│   ├── raw/                      # Raw JSON data
│   ├── curated/                  # Processed Parquet data
│   └── validation_reports/       # Data quality reports
└── docs/                          # Additional documentation
```

---

## 🔧 Key Features

### Data Processing
- **Type-Safe Transformations** - Scala case classes with schema enforcement
- **Window Functions** - Moving averages, volatility calculations
- **Data Quality** - Automated validation with Great Expectations
- **Partitioning** - Date and symbol partitioning for efficient queries
- **Compression** - Snappy-compressed Parquet for optimal storage

### Orchestration
- **Task Dependencies** - DAG-based workflow management
- **Error Handling** - Automatic retries with exponential backoff
- **Monitoring** - Task duration tracking and alerting
- **Scalability** - Easily add new stocks or metrics

### Data Generation
- **Realistic Simulation** - Geometric Brownian Motion price modeling
- **Corporate Actions** - Dividends and stock splits
- **Market Conditions** - Configurable volatility and trends
- **Multiple Formats** - JSON, CSV, Parquet output

---

## 🚢 Deployment Options

### Option 1: Local Docker (Recommended for Portfolio)
```bash
# Set deployment mode
export DEPLOYMENT_MODE=local

# Quick demo
./scripts/quick-demo.sh
```

**Advantages:**
- ✅ $0 cost
- ✅ No internet required
- ✅ Instant startup
- ✅ Full control

### Option 2: AWS Free Tier (For Online Portfolio)
```bash
# Set deployment mode
export DEPLOYMENT_MODE=aws

# One-command deployment
./scripts/deploy-to-aws.sh
```

**Advantages:**
- ✅ $0-3/month cost
- ✅ 24/7 availability
- ✅ Public URL for sharing
- ✅ Real cloud environment

See [AWS_FREE_TIER_DEPLOYMENT.md](AWS_FREE_TIER_DEPLOYMENT.md) for detailed instructions.

### Option 3: AWS Production (Optional)
```bash
# Full production deployment with Glue + Redshift
cd infrastructure/cloudformation
aws cloudformation create-stack --template-body file://main.yaml ...
```

**Cost:** $26K-174K/month (not recommended for portfolio)

See [SCALA_SPARK_GUIDE.md](SCALA_SPARK_GUIDE.md) for detailed AWS production instructions.

---

## 📊 Sample Output

### Data Quality Report
```
✅ AAPL: 65 records validated
✅ GOOGL: 65 records validated
✅ MSFT: 65 records validated
✅ AMZN: 65 records validated
✅ TSLA: 65 records validated

Total: 325 records, 0 errors (100% pass rate)
```

### Fact Table Query
```sql
SELECT
    symbol,
    trade_date,
    close_price,
    sma_20,
    daily_return,
    volatility_20d
FROM fact_stock_prices
WHERE trade_date >= '2024-01-01'
ORDER BY trade_date DESC, symbol
LIMIT 5;
```

---

## 🤝 Contributing

This is a portfolio project, but suggestions and improvements are welcome!

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

---

## 📞 Contact

- **GitHub**: [@your-username](https://github.com/your-username)
- **Email**: your.email@example.com
- **LinkedIn**: [Your Name](https://linkedin.com/in/your-profile)

---

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

## 🙏 Acknowledgments

- Alpha Vantage API for financial data structure reference
- Apache Spark community for excellent documentation
- Bitnami for Docker images

---

⭐ **If this helps you land your dream job, please give it a star!**

Made with ❤️ for Data Engineering Job Seekers
