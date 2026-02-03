# AWS Deployment Summary

## Deployment Modes

The project supports two deployment modes, switched via the `DEPLOYMENT_MODE` environment variable.

| | Local Docker | AWS |
|---|---|---|
| Cost | $0/mo | $0-3/mo (Free Tier) |
| Config | `DEPLOYMENT_MODE=local` | `DEPLOYMENT_MODE=aws` |
| Storage | Local filesystem | S3 |
| Compute | Docker Spark cluster | EC2 / Glue |
| Database | PostgreSQL (Docker) | PostgreSQL (EC2) / Redshift |

## How It Works

The Airflow DAG dynamically resolves data paths based on deployment mode:

```python
if DEPLOYMENT_MODE == 'aws':
    RAW_DATA_PATH = os.getenv('S3_RAW_BUCKET')
    CURATED_DATA_PATH = os.getenv('S3_CURATED_BUCKET')
else:
    RAW_DATA_PATH = PROJECT_ROOT / 'data' / 'raw'
    CURATED_DATA_PATH = PROJECT_ROOT / 'data' / 'curated'
```

The Scala Spark code requires no changes -- Spark natively reads/writes S3 paths. Only the `--source-path` and `--target-path` arguments change.

## Local Docker

```bash
cp .env.example .env            # DEPLOYMENT_MODE=local
python scripts/data_generator.py --preset demo
sbt assembly
docker-compose -f docker-compose-spark.yml up -d
./scripts/build-and-submit.sh local
```

## AWS Free Tier

```bash
aws configure
./scripts/deploy-to-aws.sh      # Creates S3, EC2, IAM, uploads code
ssh -i key.pem ec2-user@<IP>
cd aws-mwaa-financial-data-platform
docker-compose -f docker-compose-spark.yml up -d
```

Access points:
- Airflow UI: `http://<IP>:8080` (admin/admin)
- Spark UI: `http://<IP>:8081`

## AWS Production (CloudFormation)

The `infrastructure/cloudformation/main.yaml` template provisions the full production stack:

- VPC with public/private subnets + NAT Gateway
- S3 buckets (raw, curated, validation, MWAA) with KMS encryption
- AWS MWAA (managed Airflow) with auto-scaling workers
- AWS Glue ETL job (configurable 2-10 workers)
- Redshift Serverless (8-32 RPU capacity)
- IAM roles (least-privilege per service)
- Lambda for Slack notifications
- CloudWatch dashboard (task success rates, job duration, S3 sizes)

Deploy:
```bash
aws cloudformation deploy \
  --template-file infrastructure/cloudformation/main.yaml \
  --stack-name financial-platform \
  --parameter-overrides EnvironmentName=prod \
  --capabilities CAPABILITY_NAMED_IAM
```

## Files Modified for AWS Support

```
.env.example                            # DEPLOYMENT_MODE, S3 bucket config
dags/financial_data_pipeline_scala.py   # Dynamic path resolution, S3 upload task
scripts/deploy-to-aws.sh               # One-command AWS deployment
infrastructure/cloudformation/main.yaml # Full production stack (858 lines)
```

## Cleanup

```bash
# Stop EC2 (keep data)
aws ec2 stop-instances --instance-ids <ID>

# Full teardown
aws s3 rm s3://financial-raw-<ACCOUNT_ID> --recursive
aws s3 rb s3://financial-raw-<ACCOUNT_ID>
aws s3 rm s3://financial-processed-<ACCOUNT_ID> --recursive
aws s3 rb s3://financial-processed-<ACCOUNT_ID>
aws cloudformation delete-stack --stack-name financial-platform-free-tier
```
