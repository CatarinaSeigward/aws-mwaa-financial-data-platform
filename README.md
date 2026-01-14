# Financial Data Pipeline with AWS MWAA

A production-ready ETL pipeline that ingests financial market data from Alpha Vantage API, validates data quality, transforms to analytical format, and loads into Amazon Redshift for analysis.

## Tech Stack

**Orchestration**: Apache Airflow (AWS MWAA 2.8+)
**Data Processing**: AWS Glue (PySpark), Python 3.9
**Storage**: Amazon S3 (Raw/Curated layers), Amazon Redshift Serverless
**Data Quality**: Great Expectations
**Monitoring**: CloudWatch, AWS Lambda, Slack API
**IaC**: Terraform / CloudFormation
**Security**: AWS KMS, Secrets Manager, IAM

## Architecture

```
Alpha Vantage API → Airflow DAG → S3 Raw (JSON)
                       ↓
                 Great Expectations → Validation Reports
                       ↓
                  AWS Glue Job → S3 Curated (Parquet)
                       ↓
                 Redshift COPY → Redshift Serverless
                       ↓
                CloudWatch Alarms → Lambda → Slack
```

## Technical Implementation

### 1. Data Ingestion
- Built custom Python client with retry logic and rate limiting for Alpha Vantage REST API
- Implemented exponential backoff for API failures
- Raw JSON stored in S3 with date partitioning: `s3://bucket/raw/year=2024/month=01/day=15/`

### 2. Data Validation
- Great Expectations validations run before transformation:
  - Column type checks (timestamp, float prices, integer volume)
  - Null checks on critical fields
  - Range validation (price > 0, volume >= 0)
  - Row count thresholds
- Validation results stored as JSON in S3 for audit trail

### 3. Data Transformation
- AWS Glue PySpark job converts JSON to Parquet with schema enforcement
- Deduplication based on symbol + timestamp composite key
- Derived metrics: daily returns, moving averages
- Columnar format reduces Redshift query costs by 80%

### 4. Data Loading
- Redshift COPY command with IAM role authentication
- UPSERT logic using staging table merge pattern
- Automatic compression encoding (AZ64 for timestamps, LZO for text)

### 5. Orchestration
- Airflow DAG with task dependencies and SLA monitoring
- Retry policy: 3 attempts with 5-minute delays
- Data quality gate: pipeline fails if validation error rate > 5%

### 6. Monitoring
- CloudWatch custom metrics: API success rate, validation pass rate, pipeline duration
- Lambda function parses CloudWatch alarms and sends formatted Slack notifications
- Airflow task logs streamed to CloudWatch Logs

## Deployment

### Prerequisites
```bash
AWS CLI configured with appropriate credentials
Python 3.9+
Terraform 1.5+ or AWS CLI for CloudFormation
```

### Deploy Infrastructure
```bash
# Option 1: Terraform
cd infrastructure/terraform
terraform init
terraform apply -var="alpha_vantage_api_key=YOUR_KEY"

# Option 2: CloudFormation
aws cloudformation create-stack \
  --stack-name financial-data-pipeline \
  --template-body file://infrastructure/cloudformation/stack.yaml \
  --parameters ParameterKey=ApiKey,ParameterValue=YOUR_KEY \
  --capabilities CAPABILITY_IAM
```

### Deploy Airflow DAGs
```bash
# Upload to MWAA S3 bucket
aws s3 sync dags/ s3://mwaa-bucket-name/dags/
aws s3 sync src/ s3://mwaa-bucket-name/dags/src/

# Trigger DAG via Airflow CLI
aws mwaa create-cli-token --name mwaa-environment-name
# Use token to access Airflow UI and trigger DAG
```

### Deploy Lambda Notifier
```bash
cd lambda/slack_notifier
pip install -r requirements.txt -t package/
cd package && zip -r ../function.zip .
cd .. && zip -g function.zip handler.py

aws lambda update-function-code \
  --function-name slack-notifier \
  --zip-file fileb://function.zip
```

## Project Structure
```
├── dags/financial_data_pipeline.py    # Airflow DAG definition
├── src/
│   ├── ingestion/alpha_vantage_client.py     # API client with retry
│   ├── validation/data_validator.py          # Great Expectations wrapper
│   ├── transformation/glue_transform.py      # PySpark ETL script
│   └── loading/redshift_loader.py            # COPY command executor
├── lambda/slack_notifier/handler.py   # CloudWatch to Slack bridge
├── infrastructure/
│   ├── terraform/                     # IaC for all AWS resources
│   └── cloudformation/                # Alternative IaC option
└── config/expectations/               # Data quality rules
```

## Security & Cost Optimization

**Security**
- IAM roles with least privilege access (separate roles for ingestion, transformation, loading)
- S3 encryption with AWS KMS
- API keys stored in Secrets Manager
- VPC endpoints for private AWS service access

**Cost Optimization**
- S3 lifecycle policies: Raw data → Glacier after 30 days
- Redshift Serverless: Auto-pause during idle periods
- Glue Flex execution class for non-time-sensitive jobs
- Parquet compression reduces storage by 60%
