#!/bin/bash
# AWS Deployment Script for Financial Data Platform
# ===================================================
# This script helps deploy the project to AWS Free Tier

set -e

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}AWS Free Tier Deployment Script${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

# Check if AWS CLI is installed
if ! command -v aws &> /dev/null; then
    echo -e "${RED}❌ AWS CLI is not installed${NC}"
    echo "Install it from: https://aws.amazon.com/cli/"
    exit 1
fi

# Check if AWS credentials are configured
if ! aws sts get-caller-identity &> /dev/null; then
    echo -e "${RED}❌ AWS credentials not configured${NC}"
    echo "Run: aws configure"
    exit 1
fi

AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
AWS_REGION=$(aws configure get region || echo "us-east-1")

echo -e "${GREEN}✅ AWS Account ID: ${AWS_ACCOUNT_ID}${NC}"
echo -e "${GREEN}✅ AWS Region: ${AWS_REGION}${NC}"
echo ""

# Step 1: Create S3 Buckets
echo -e "${YELLOW}📦 Step 1: Creating S3 Buckets...${NC}"

S3_RAW_BUCKET="financial-raw-${AWS_ACCOUNT_ID}"
S3_PROCESSED_BUCKET="financial-processed-${AWS_ACCOUNT_ID}"
S3_VALIDATION_BUCKET="financial-validation-${AWS_ACCOUNT_ID}"

for BUCKET in $S3_RAW_BUCKET $S3_PROCESSED_BUCKET $S3_VALIDATION_BUCKET; do
    if aws s3 ls "s3://${BUCKET}" 2>&1 | grep -q 'NoSuchBucket'; then
        aws s3 mb "s3://${BUCKET}" --region "${AWS_REGION}"
        echo -e "${GREEN}  ✅ Created bucket: ${BUCKET}${NC}"
    else
        echo -e "${BLUE}  ℹ️  Bucket already exists: ${BUCKET}${NC}"
    fi
done

# Step 2: Upload Scala JAR
echo ""
echo -e "${YELLOW}📤 Step 2: Uploading Scala JAR...${NC}"

JAR_PATH="target/scala-2.12/financial-etl-assembly-1.0.0.jar"

if [ ! -f "$JAR_PATH" ]; then
    echo -e "${RED}❌ JAR not found: ${JAR_PATH}${NC}"
    echo "Run: sbt assembly"
    exit 1
fi

aws s3 cp "$JAR_PATH" "s3://${S3_PROCESSED_BUCKET}/jars/financial-etl.jar"
echo -e "${GREEN}  ✅ Uploaded JAR to S3${NC}"

# Step 3: Upload DAGs and scripts
echo ""
echo -e "${YELLOW}📤 Step 3: Uploading Airflow DAGs and scripts...${NC}"

aws s3 sync dags/ "s3://${S3_PROCESSED_BUCKET}/dags/"
aws s3 sync scripts/ "s3://${S3_PROCESSED_BUCKET}/scripts/"
echo -e "${GREEN}  ✅ Uploaded DAGs and scripts${NC}"

# Step 4: Generate sample data and upload
echo ""
echo -e "${YELLOW}🎲 Step 4: Generating and uploading sample data...${NC}"

python scripts/data_generator.py --preset demo --output data/raw/
aws s3 sync data/raw/ "s3://${S3_RAW_BUCKET}/data/raw/"
echo -e "${GREEN}  ✅ Uploaded sample data${NC}"

# Step 5: Deploy CloudFormation Stack
echo ""
echo -e "${YELLOW}☁️  Step 5: Deploying CloudFormation Stack...${NC}"

read -p "Enter your SSH key pair name: " KEY_NAME
read -p "Enter your public IP (for SSH access, e.g., 1.2.3.4/32): " MY_IP

STACK_NAME="financial-platform-free-tier"

echo "Deploying stack: ${STACK_NAME}"

aws cloudformation create-stack \
    --stack-name "${STACK_NAME}" \
    --template-body file://infrastructure/cloudformation/aws-free-tier-stack.yaml \
    --parameters \
        ParameterKey=KeyName,ParameterValue="${KEY_NAME}" \
        ParameterKey=MyIP,ParameterValue="${MY_IP}" \
    --capabilities CAPABILITY_IAM \
    --region "${AWS_REGION}"

echo ""
echo -e "${BLUE}⏳ Waiting for stack creation (this may take 5-10 minutes)...${NC}"

aws cloudformation wait stack-create-complete \
    --stack-name "${STACK_NAME}" \
    --region "${AWS_REGION}"

# Step 6: Get outputs
echo ""
echo -e "${GREEN}✅ Stack created successfully!${NC}"
echo ""
echo -e "${YELLOW}📋 Stack Outputs:${NC}"

aws cloudformation describe-stacks \
    --stack-name "${STACK_NAME}" \
    --region "${AWS_REGION}" \
    --query 'Stacks[0].Outputs[*].[OutputKey,OutputValue]' \
    --output table

# Get instance details
INSTANCE_ID=$(aws cloudformation describe-stacks \
    --stack-name "${STACK_NAME}" \
    --region "${AWS_REGION}" \
    --query 'Stacks[0].Outputs[?OutputKey==`InstanceId`].OutputValue' \
    --output text)

PUBLIC_IP=$(aws cloudformation describe-stacks \
    --stack-name "${STACK_NAME}" \
    --region "${AWS_REGION}" \
    --query 'Stacks[0].Outputs[?OutputKey==`PublicIP`].OutputValue' \
    --output text)

# Step 7: Update .env file
echo ""
echo -e "${YELLOW}⚙️  Step 7: Updating .env file...${NC}"

cat > .env <<EOF
# AWS Deployment Configuration
DEPLOYMENT_MODE=aws
AWS_DEFAULT_REGION=${AWS_REGION}
S3_RAW_BUCKET=s3://${S3_RAW_BUCKET}
S3_CURATED_BUCKET=s3://${S3_PROCESSED_BUCKET}
S3_VALIDATION_BUCKET=s3://${S3_VALIDATION_BUCKET}

# Data Source
DATA_SOURCE=simulated
DATA_GENERATOR_PRESET=demo

# PostgreSQL (on EC2)
POSTGRES_HOST=${PUBLIC_IP}
POSTGRES_PORT=5432
POSTGRES_DB=financial_dw
POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow

# Spark Master (on EC2)
SPARK_MASTER_URL=spark://localhost:7077
EOF

echo -e "${GREEN}  ✅ Updated .env file${NC}"

# Step 8: Print next steps
echo ""
echo -e "${BLUE}========================================${NC}"
echo -e "${GREEN}✅ Deployment Complete!${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""
echo -e "${YELLOW}📝 Next Steps:${NC}"
echo ""
echo "1. Connect to EC2 instance:"
echo -e "   ${GREEN}ssh -i ${KEY_NAME}.pem ec2-user@${PUBLIC_IP}${NC}"
echo ""
echo "2. Wait for setup to complete (~5 minutes):"
echo -e "   ${GREEN}tail -f /var/log/cloud-init-output.log${NC}"
echo ""
echo "3. Start Docker containers on EC2:"
echo -e "   ${GREEN}cd aws-mwaa-financial-data-platform${NC}"
echo -e "   ${GREEN}sudo docker-compose -f docker-compose-spark.yml up -d${NC}"
echo ""
echo "4. Access services:"
echo -e "   Airflow UI: ${BLUE}http://${PUBLIC_IP}:8080${NC} (admin/admin)"
echo -e "   Spark UI:   ${BLUE}http://${PUBLIC_IP}:8081${NC}"
echo ""
echo "5. Monitor costs:"
echo -e "   ${GREEN}aws ce get-cost-and-usage --time-period Start=$(date +%Y-%m-01),End=$(date +%Y-%m-%d) --granularity MONTHLY --metrics BlendedCost${NC}"
echo ""
echo -e "${YELLOW}⚠️  Remember:${NC}"
echo "- Stop EC2 when not in use: aws ec2 stop-instances --instance-ids ${INSTANCE_ID}"
echo "- Delete stack when done: aws cloudformation delete-stack --stack-name ${STACK_NAME}"
echo "- Monitor your AWS bill: https://console.aws.amazon.com/billing/"
echo ""
