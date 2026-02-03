# AWS Free Tier Deployment Guide

Deploy the Financial Data Platform on AWS Free Tier for $0-3/month.

## Prerequisites

- AWS account with Free Tier eligibility
- AWS CLI installed and configured (`aws configure`)
- SSH key pair for EC2 access

## Free Tier Resource Usage

| Service | Free Tier Limit | Our Usage |
|---------|----------------|-----------|
| EC2 | 750 hrs/mo (t2.micro) | ~720 hrs |
| S3 | 5 GB + 20K GET + 2K PUT | ~2 GB |
| CloudWatch | 5 GB logs | ~1 GB |
| Data Transfer | 100 GB outbound | ~5 GB |

## Step 1: Deploy Infrastructure

```bash
aws cloudformation create-stack \
  --stack-name financial-platform-free-tier \
  --template-body file://aws-free-tier-stack.yaml \
  --parameters \
    ParameterKey=KeyName,ParameterValue=your-key-name \
    ParameterKey=MyIP,ParameterValue=YOUR_IP/32 \
  --capabilities CAPABILITY_IAM

aws cloudformation wait stack-create-complete \
  --stack-name financial-platform-free-tier

# Get outputs (public IP, S3 buckets, SSH command)
aws cloudformation describe-stacks \
  --stack-name financial-platform-free-tier \
  --query 'Stacks[0].Outputs'
```

The stack creates: EC2 t2.micro (30 GB EBS), 2 S3 buckets, IAM role, security group.

## Step 2: Setup on EC2

```bash
ssh -i your-key.pem ec2-user@<PUBLIC_IP>

# Wait for cloud-init to finish
tail -f /var/log/cloud-init-output.log

cd aws-mwaa-financial-data-platform
pip3.9 install -r requirements.txt

# Configure
cp .env.example .env
# Set DEPLOYMENT_MODE=aws and S3 bucket names in .env

# Generate and upload data
python3.9 scripts/data_generator.py --preset demo
aws s3 sync data/raw/ s3://financial-raw-<ACCOUNT_ID>/raw/

# Start services
sudo docker-compose -f docker-compose-spark.yml up -d
```

## Step 3: Access Services

| Service | URL |
|---------|-----|
| Airflow | `http://<PUBLIC_IP>:8080` (admin/admin) |
| Spark Master | `http://<PUBLIC_IP>:8081` |

Trigger the pipeline via Airflow UI or:
```bash
./scripts/build-and-submit.sh local
```

## Architecture

```
EC2 t2.micro
  - Airflow (LocalExecutor)
  - Spark Master + 2 Workers
  - PostgreSQL 15
  |
  +-- S3: financial-raw-{account-id}        (raw JSON)
  +-- S3: financial-processed-{account-id}  (curated Parquet)
```

Differences from production CloudFormation:
- No MWAA ($300+/mo) -- Airflow runs on EC2
- No Redshift ($500+/mo) -- PostgreSQL on EC2
- No Glue -- Spark runs locally on EC2
- No NAT Gateway ($32/mo) -- public subnet with security groups
- No KMS custom keys -- S3 default encryption

## Security

- SSH restricted to your IP via security group
- S3 access via IAM instance profile (no hardcoded credentials)
- All S3 buckets block public access

## Cost Monitoring

```bash
# Check current month charges
aws ce get-cost-and-usage \
  --time-period Start=$(date +%Y-%m-01),End=$(date +%Y-%m-%d) \
  --granularity MONTHLY \
  --metrics BlendedCost
```

Set a $5 billing alarm:
```bash
aws sns create-topic --name billing-alerts
aws sns subscribe --topic-arn <ARN> --protocol email --notification-endpoint you@email.com
aws cloudwatch put-metric-alarm \
  --alarm-name billing-5-dollars \
  --metric-name EstimatedCharges \
  --namespace AWS/Billing \
  --statistic Maximum \
  --period 21600 \
  --evaluation-periods 1 \
  --threshold 5 \
  --comparison-operator GreaterThanThreshold \
  --alarm-actions <ARN>
```

## Cleanup

```bash
# Stop EC2 (keep data, stop billing)
aws ec2 stop-instances --instance-ids <ID>

# Full teardown
aws s3 rm s3://financial-raw-<ACCOUNT_ID> --recursive
aws s3 rb s3://financial-raw-<ACCOUNT_ID>
aws s3 rm s3://financial-processed-<ACCOUNT_ID> --recursive
aws s3 rb s3://financial-processed-<ACCOUNT_ID>
aws cloudformation delete-stack --stack-name financial-platform-free-tier
```

## After Free Tier Expires (12 months)

- **Continue on AWS**: ~$15-25/mo (EC2 t2.micro + S3 + transfer)
- **Switch to local Docker**: `aws s3 sync s3://... ./data/` then `docker-compose up -d`
