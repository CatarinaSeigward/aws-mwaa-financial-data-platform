# AWS Free Tier Deployment Guide

## Overview

This guide shows how to deploy the Financial Data Platform on AWS using **Free Tier resources** to minimize costs while maintaining core functionality.

**Estimated Cost**: **$0-3/month** (within AWS Free Tier limits for first 12 months)

---

## 💰 Cost Comparison

| Deployment Option | Monthly Cost | Use Case |
|-------------------|--------------|----------|
| **Full AWS Production** (CloudFormation) | $174,323 | ❌ Too expensive for portfolio |
| **AWS Free Tier** (This Guide) | **$0-3** | ✅ Online portfolio demo |
| **Local Docker** (Default) | $0 | ✅ Best for interviews |

---

## 📋 AWS Free Tier Limits (First 12 Months)

| Service | Free Tier Allowance | Our Usage | Status |
|---------|---------------------|-----------|--------|
| **EC2** | 750 hours/month (t2.micro) | ~720 hours | ✅ Within limit |
| **S3** | 5GB storage + 20K GET + 2K PUT | ~2GB + 10K GET + 500 PUT | ✅ Within limit |
| **RDS** | 750 hours/month (db.t2.micro) | N/A (use PostgreSQL on EC2) | ✅ Free |
| **Lambda** | 1M requests + 400K GB-seconds | ~100K requests | ✅ Within limit |
| **CloudWatch** | 5GB logs + 10 custom metrics | ~1GB logs | ✅ Within limit |
| **Data Transfer** | 100GB outbound | ~5GB | ✅ Within limit |

**Total Estimated Cost**: **$0-3/month** (mostly data transfer overages)

---

## 🏗️ Architecture: AWS Free Tier Version

```
┌─────────────────────────────────────────────────────────┐
│                     AWS Free Tier                       │
├─────────────────────────────────────────────────────────┤
│                                                         │
│  ┌──────────────┐    ┌──────────────┐                 │
│  │  EC2 t2.micro│    │  S3 Bucket   │                 │
│  │              │───▶│  (Raw Data)  │                 │
│  │ - Airflow    │    └──────────────┘                 │
│  │ - Spark      │                                      │
│  │ - PostgreSQL │    ┌──────────────┐                 │
│  │ - Python     │───▶│  S3 Bucket   │                 │
│  └──────────────┘    │ (Processed)  │                 │
│                      └──────────────┘                 │
│                                                         │
│  ┌──────────────┐    ┌──────────────┐                 │
│  │  Lambda      │───▶│ CloudWatch   │                 │
│  │  (Optional)  │    │   Logs       │                 │
│  └──────────────┘    └──────────────┘                 │
│                                                         │
└─────────────────────────────────────────────────────────┘
```

**Key Changes from Production**:
- ❌ No AWS MWAA (expensive) → ✅ Airflow on EC2
- ❌ No Redshift Serverless ($99K/month) → ✅ PostgreSQL on EC2
- ❌ No AWS Glue ($15K/month) → ✅ Spark on EC2
- ❌ No NAT Gateway ($32/month) → ✅ Public subnet with security groups
- ❌ No KMS custom keys ($80/month) → ✅ S3 default encryption

---

## 🚀 Step-by-Step Deployment

### Prerequisites

1. **AWS Account** with Free Tier eligibility
2. **AWS CLI** installed and configured
3. **SSH key pair** for EC2 access
4. **Basic AWS knowledge** (EC2, S3, IAM)

### Step 1: Create EC2 Instance

Create a CloudFormation template for a single EC2 instance:

```yaml
# save as: aws-free-tier-stack.yaml
AWSTemplateFormatVersion: '2010-09-09'
Description: Financial Data Platform - Free Tier Version

Parameters:
  KeyName:
    Type: AWS::EC2::KeyPair::KeyName
    Description: EC2 key pair name for SSH access

  MyIP:
    Type: String
    Description: Your IP address for SSH access (e.g., 1.2.3.4/32)
    AllowedPattern: '^(\d{1,3}\.){3}\d{1,3}/\d{1,2}$'

Resources:
  # S3 Buckets
  RawDataBucket:
    Type: AWS::S3::Bucket
    Properties:
      BucketName: !Sub 'financial-raw-${AWS::AccountId}'
      PublicAccessBlockConfiguration:
        BlockPublicAcls: true
        BlockPublicPolicy: true
        IgnorePublicAcls: true
        RestrictPublicBuckets: true
      LifecycleConfiguration:
        Rules:
          - Id: DeleteOldData
            Status: Enabled
            ExpirationInDays: 90

  ProcessedDataBucket:
    Type: AWS::S3::Bucket
    Properties:
      BucketName: !Sub 'financial-processed-${AWS::AccountId}'
      PublicAccessBlockConfiguration:
        BlockPublicAcls: true
        BlockPublicPolicy: true
        IgnorePublicAcls: true
        RestrictPublicBuckets: true

  # IAM Role for EC2
  EC2Role:
    Type: AWS::IAM::Role
    Properties:
      AssumeRolePolicyDocument:
        Version: '2012-10-17'
        Statement:
          - Effect: Allow
            Principal:
              Service: ec2.amazonaws.com
            Action: 'sts:AssumeRole'
      ManagedPolicyArns:
        - arn:aws:iam::aws:policy/CloudWatchAgentServerPolicy
      Policies:
        - PolicyName: S3Access
          PolicyDocument:
            Version: '2012-10-17'
            Statement:
              - Effect: Allow
                Action:
                  - 's3:GetObject'
                  - 's3:PutObject'
                  - 's3:ListBucket'
                Resource:
                  - !GetAtt RawDataBucket.Arn
                  - !Sub '${RawDataBucket.Arn}/*'
                  - !GetAtt ProcessedDataBucket.Arn
                  - !Sub '${ProcessedDataBucket.Arn}/*'

  EC2InstanceProfile:
    Type: AWS::IAM::InstanceProfile
    Properties:
      Roles:
        - !Ref EC2Role

  # Security Group
  EC2SecurityGroup:
    Type: AWS::EC2::SecurityGroup
    Properties:
      GroupDescription: Security group for Financial Data Platform
      SecurityGroupIngress:
        - IpProtocol: tcp
          FromPort: 22
          ToPort: 22
          CidrIp: !Ref MyIP
          Description: SSH access
        - IpProtocol: tcp
          FromPort: 8080
          ToPort: 8080
          CidrIp: !Ref MyIP
          Description: Airflow Web UI
        - IpProtocol: tcp
          FromPort: 8081
          ToPort: 8081
          CidrIp: !Ref MyIP
          Description: Spark UI
      SecurityGroupEgress:
        - IpProtocol: '-1'
          CidrIp: 0.0.0.0/0

  # EC2 Instance
  EC2Instance:
    Type: AWS::EC2::Instance
    Properties:
      InstanceType: t2.micro  # Free tier eligible
      ImageId: !Sub '{{resolve:ssm:/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64}}'
      KeyName: !Ref KeyName
      IamInstanceProfile: !Ref EC2InstanceProfile
      SecurityGroupIds:
        - !Ref EC2SecurityGroup
      BlockDeviceMappings:
        - DeviceName: /dev/xvda
          Ebs:
            VolumeSize: 30  # Free tier: 30GB
            VolumeType: gp2
      UserData:
        Fn::Base64: !Sub |
          #!/bin/bash
          # Update system
          yum update -y

          # Install Docker
          yum install -y docker
          systemctl start docker
          systemctl enable docker
          usermod -aG docker ec2-user

          # Install Docker Compose
          curl -L "https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
          chmod +x /usr/local/bin/docker-compose

          # Install Git
          yum install -y git

          # Install Java 11 (for Spark)
          yum install -y java-11-amazon-corretto-headless

          # Install Python 3.9
          yum install -y python3.9 python3-pip

          # Clone project (replace with your repo)
          cd /home/ec2-user
          git clone https://github.com/your-username/aws-mwaa-financial-data-platform.git
          chown -R ec2-user:ec2-user aws-mwaa-financial-data-platform

          # Set environment variables
          echo "export RAW_BUCKET=${RawDataBucket}" >> /home/ec2-user/.bashrc
          echo "export PROCESSED_BUCKET=${ProcessedDataBucket}" >> /home/ec2-user/.bashrc

          # Signal completion
          echo "Setup complete" > /home/ec2-user/setup-complete.txt
      Tags:
        - Key: Name
          Value: financial-data-platform

Outputs:
  InstanceId:
    Description: EC2 Instance ID
    Value: !Ref EC2Instance

  PublicIP:
    Description: EC2 Public IP Address
    Value: !GetAtt EC2Instance.PublicIp

  SSHCommand:
    Description: SSH command to connect
    Value: !Sub 'ssh -i ${KeyName}.pem ec2-user@${EC2Instance.PublicIp}'

  AirflowURL:
    Description: Airflow Web UI
    Value: !Sub 'http://${EC2Instance.PublicIp}:8080'

  SparkURL:
    Description: Spark UI
    Value: !Sub 'http://${EC2Instance.PublicIp}:8081'

  RawBucket:
    Description: Raw Data S3 Bucket
    Value: !Ref RawDataBucket

  ProcessedBucket:
    Description: Processed Data S3 Bucket
    Value: !Ref ProcessedDataBucket
```

### Step 2: Deploy the Stack

```bash
# Create the stack
aws cloudformation create-stack \
  --stack-name financial-platform-free-tier \
  --template-body file://aws-free-tier-stack.yaml \
  --parameters \
    ParameterKey=KeyName,ParameterValue=your-key-name \
    ParameterKey=MyIP,ParameterValue=YOUR_IP/32 \
  --capabilities CAPABILITY_IAM

# Wait for stack creation (5-10 minutes)
aws cloudformation wait stack-create-complete \
  --stack-name financial-platform-free-tier

# Get outputs
aws cloudformation describe-stacks \
  --stack-name financial-platform-free-tier \
  --query 'Stacks[0].Outputs'
```

### Step 3: Connect to EC2 and Setup

```bash
# SSH into the instance (use the SSHCommand from outputs)
ssh -i your-key.pem ec2-user@<PUBLIC_IP>

# Wait for user data script to complete
tail -f /var/log/cloud-init-output.log

# Navigate to project
cd aws-mwaa-financial-data-platform

# Install Python dependencies
pip3.9 install -r requirements.txt

# Configure environment
cp .env.example .env
nano .env  # Edit with your S3 bucket names

# Generate sample data
python3.9 scripts/data_generator.py --preset demo

# Upload data to S3
aws s3 sync data/raw/ s3://financial-raw-<ACCOUNT_ID>/raw/

# Start Docker Compose
sudo docker-compose -f docker-compose-spark.yml up -d

# Check container status
sudo docker-compose -f docker-compose-spark.yml ps
```

### Step 4: Access the Services

Open your browser and navigate to:

- **Airflow**: `http://<PUBLIC_IP>:8080`
  - Username: `admin`
  - Password: `admin`

- **Spark UI**: `http://<PUBLIC_IP>:8081`

### Step 5: Run the Pipeline

```bash
# Build Scala JAR
./scripts/build-and-submit.sh local

# Or trigger via Airflow UI
# Navigate to http://<PUBLIC_IP>:8080
# Enable and trigger: financial_data_pipeline_scala
```

---

## 📊 Data Flow on AWS Free Tier

1. **Data Generation**: Python script generates stock data
2. **Upload to S3**: Raw JSON data uploaded to `s3://financial-raw-{account-id}/`
3. **Spark Processing**: Spark job on EC2 reads from S3, transforms data
4. **Write to S3**: Processed Parquet files written to `s3://financial-processed-{account-id}/`
5. **Load to PostgreSQL**: Data loaded into PostgreSQL on EC2
6. **Query**: Access via Airflow UI or direct PostgreSQL connection

---

## 🔒 Security Best Practices

### 1. Restrict SSH Access

```bash
# Update security group to only allow your IP
aws ec2 authorize-security-group-ingress \
  --group-id sg-xxxxx \
  --protocol tcp \
  --port 22 \
  --cidr YOUR_IP/32
```

### 2. Use IAM Roles (Not Access Keys)

The EC2 instance uses an IAM role for S3 access - no hardcoded credentials needed.

### 3. Enable CloudWatch Logs

```bash
# Install CloudWatch agent on EC2
sudo yum install -y amazon-cloudwatch-agent

# Configure log shipping
sudo /opt/aws/amazon-cloudwatch-agent/bin/amazon-cloudwatch-agent-ctl \
  -a fetch-config \
  -m ec2 \
  -s \
  -c file:/opt/aws/amazon-cloudwatch-agent/config.json
```

### 4. Regular Backups

```bash
# Backup PostgreSQL to S3
pg_dump -U airflow financial_dw | gzip | \
  aws s3 cp - s3://financial-processed-<ACCOUNT_ID>/backups/db-$(date +%Y%m%d).sql.gz
```

---

## 💰 Cost Monitoring

### Set Up Billing Alerts

```bash
# Create SNS topic for alerts
aws sns create-topic --name billing-alerts

# Subscribe your email
aws sns subscribe \
  --topic-arn arn:aws:sns:us-east-1:<ACCOUNT_ID>:billing-alerts \
  --protocol email \
  --notification-endpoint your-email@example.com

# Create billing alarm (alert if cost > $5/month)
aws cloudwatch put-metric-alarm \
  --alarm-name billing-alert-5-dollars \
  --alarm-description "Alert if monthly cost exceeds $5" \
  --metric-name EstimatedCharges \
  --namespace AWS/Billing \
  --statistic Maximum \
  --period 21600 \
  --evaluation-periods 1 \
  --threshold 5 \
  --comparison-operator GreaterThanThreshold \
  --alarm-actions arn:aws:sns:us-east-1:<ACCOUNT_ID>:billing-alerts
```

### Check Current Costs

```bash
# View current month charges
aws ce get-cost-and-usage \
  --time-period Start=$(date +%Y-%m-01),End=$(date +%Y-%m-%d) \
  --granularity MONTHLY \
  --metrics BlendedCost
```

---

## 🔧 Troubleshooting

### Issue: EC2 Instance Not Starting

**Solution**: Check instance status
```bash
aws ec2 describe-instance-status --instance-ids i-xxxxx
```

### Issue: Cannot Connect to Airflow UI

**Solution**: Check security group and instance state
```bash
# Ensure port 8080 is open
aws ec2 describe-security-groups --group-ids sg-xxxxx

# Check if Airflow is running
ssh ec2-user@<PUBLIC_IP> "sudo docker ps | grep airflow"
```

### Issue: S3 Access Denied

**Solution**: Verify IAM role permissions
```bash
# Check instance profile
aws ec2 describe-instances --instance-ids i-xxxxx \
  --query 'Reservations[0].Instances[0].IamInstanceProfile'
```

### Issue: Exceeding Free Tier Limits

**Solution**: Monitor usage in AWS Cost Explorer
```bash
# Stop EC2 when not in use
aws ec2 stop-instances --instance-ids i-xxxxx

# Delete old S3 data
aws s3 rm s3://financial-raw-<ACCOUNT_ID>/raw/ --recursive --exclude "*" --include "2024-01-*"
```

---

## 🎯 After 12 Months (Free Tier Expires)

### Option 1: Continue on AWS (Paid)

**Estimated Cost**: $15-25/month
- EC2 t2.micro: ~$10/month
- S3 storage: ~$3/month
- Data transfer: ~$5/month
- CloudWatch: ~$2/month

### Option 2: Migrate Back to Local Docker

```bash
# Export data from S3
aws s3 sync s3://financial-processed-<ACCOUNT_ID>/ ./data/curated/

# Delete AWS resources
aws cloudformation delete-stack --stack-name financial-platform-free-tier

# Run locally
docker-compose -f docker-compose-spark.yml up -d
```

---

## 📋 Cleanup (Delete All Resources)

```bash
# Delete CloudFormation stack (this deletes EC2, Security Group, IAM Role)
aws cloudformation delete-stack --stack-name financial-platform-free-tier

# Delete S3 buckets (must be empty first)
aws s3 rm s3://financial-raw-<ACCOUNT_ID> --recursive
aws s3 rb s3://financial-raw-<ACCOUNT_ID>

aws s3 rm s3://financial-processed-<ACCOUNT_ID> --recursive
aws s3 rb s3://financial-processed-<ACCOUNT_ID>

# Verify deletion
aws cloudformation describe-stacks --stack-name financial-platform-free-tier
# Should return: Stack with id financial-platform-free-tier does not exist
```

---

## 📊 Comparison: Local vs AWS Free Tier

| Feature | Local Docker | AWS Free Tier |
|---------|--------------|---------------|
| **Cost** | $0/month | $0-3/month |
| **Internet Required** | No | Yes |
| **Public URL** | No | Yes (http://x.x.x.x:8080) |
| **24/7 Availability** | No (laptop sleep) | Yes |
| **Setup Time** | 10 minutes | 30 minutes |
| **Maintenance** | Low | Medium |
| **Scalability** | Limited (laptop RAM) | Limited (t2.micro) |
| **Best For** | Interview demos | Online portfolio |

---

## 🎉 Conclusion

You can now choose between:

1. **Local Docker** ($0/month) - Best for interview demos
2. **AWS Free Tier** ($0-3/month) - Best for online portfolio
3. **Full AWS Production** ($174K/month) - Enterprise production (not recommended for portfolio)

**Recommendation**: Start with **Local Docker**, deploy to **AWS Free Tier** if you want a public URL.

---

## 📞 Support

- AWS Free Tier FAQ: https://aws.amazon.com/free/
- AWS Cost Calculator: https://calculator.aws/
- Project Issues: GitHub Issues

---

**Happy Cloud Computing! ☁️**
