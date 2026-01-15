"""
Financial Data Pipeline DAG - Scala + Spark Version
====================================================
Airflow DAG that orchestrates Scala Spark ETL jobs

Features:
- Scala + Spark data transformation (替代 PySpark)
- 可在本地 Docker Spark 集群运行
- 可提交到 AWS Glue (生产环境)
- 完整的错误处理和重试策略

Author: Financial Data Platform Team
"""

import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup

# Configuration
PROJECT_ROOT = Path(os.getenv('AIRFLOW_HOME', '/opt/airflow'))
SPARK_MASTER = os.getenv('SPARK_MASTER_URL', 'spark://spark-master:7077')
JAR_PATH = PROJECT_ROOT / 'jars' / 'financial-etl-1.0.0.jar'
MAIN_CLASS = 'com.financial.etl.transform.FinancialDataTransform'

# Deployment mode
DEPLOYMENT_MODE = os.getenv('DEPLOYMENT_MODE', 'local')  # 'local' or 'aws'

# Data paths - dynamically switch between local and S3
if DEPLOYMENT_MODE == 'aws':
    RAW_DATA_PATH = os.getenv('S3_RAW_BUCKET', 's3://financial-raw/data/raw')
    CURATED_DATA_PATH = os.getenv('S3_CURATED_BUCKET', 's3://financial-processed/data/curated')
    VALIDATION_PATH = os.getenv('S3_VALIDATION_BUCKET', 's3://financial-validation/reports')
else:
    RAW_DATA_PATH = PROJECT_ROOT / 'data' / 'raw'
    CURATED_DATA_PATH = PROJECT_ROOT / 'data' / 'curated'
    VALIDATION_PATH = PROJECT_ROOT / 'data' / 'validation_reports'

# DAG configuration
DEFAULT_ARGS = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=30),
    'execution_timeout': timedelta(hours=2),
}

# SLA configuration
SLA = timedelta(hours=3)


def check_spark_cluster(**context):
    """
    验证 Spark 集群是否可用
    """
    import socket

    master_host = SPARK_MASTER.split('://')[1].split(':')[0]
    master_port = int(SPARK_MASTER.split(':')[-1])

    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)
        result = sock.connect_ex((master_host, master_port))
        sock.close()

        if result == 0:
            print(f"✅ Spark cluster is reachable: {SPARK_MASTER}")
            return True
        else:
            raise ConnectionError(f"Cannot connect to Spark master: {SPARK_MASTER}")

    except Exception as e:
        print(f"❌ Spark cluster check failed: {e}")
        raise


def verify_jar_exists(**context):
    """
    验证 Scala JAR 文件存在
    """
    if not JAR_PATH.exists():
        raise FileNotFoundError(
            f"JAR file not found: {JAR_PATH}\n"
            f"Build it with: sbt assembly"
        )

    jar_size = JAR_PATH.stat().st_size / (1024 * 1024)
    print(f"✅ JAR found: {JAR_PATH} ({jar_size:.2f} MB)")

    return str(JAR_PATH)


def upload_to_s3_if_needed(**context):
    """
    如果是 AWS 部署模式,上传生成的数据到 S3
    """
    if DEPLOYMENT_MODE != 'aws':
        print("✅ Local mode - skip S3 upload")
        return

    import boto3
    from pathlib import Path

    print(f"☁️  Uploading data to S3...")

    s3_client = boto3.client('s3')
    local_data_dir = PROJECT_ROOT / 'data' / 'raw'

    # Extract bucket name from S3 URL
    bucket_name = RAW_DATA_PATH.replace('s3://', '').split('/')[0]
    s3_prefix = '/'.join(RAW_DATA_PATH.replace('s3://', '').split('/')[1:])

    uploaded_count = 0
    for file_path in local_data_dir.rglob('*.json'):
        s3_key = f"{s3_prefix}/{file_path.relative_to(local_data_dir)}"
        s3_client.upload_file(str(file_path), bucket_name, s3_key)
        uploaded_count += 1

    print(f"✅ Uploaded {uploaded_count} files to {RAW_DATA_PATH}")


def generate_sample_data(**context):
    """
    生成模拟股票数据（默认方式）

    优势：
    - 无需 API Key
    - 无网络依赖
    - 无速率限制
    - 100% 可靠
    - 适合演示和测试

    如需使用真实 API，请使用 fetch_stock_data_from_api() 函数
    """
    import subprocess

    execution_date = context['ds']

    print(f"🎲 Generating sample data for {execution_date}...")
    print(f"   Deployment mode: {DEPLOYMENT_MODE}")
    print("   (Using simulated data for stable demo)")

    # 生成到本地临时目录
    local_output = PROJECT_ROOT / 'data' / 'raw' if DEPLOYMENT_MODE == 'aws' else RAW_DATA_PATH

    # 调用数据生成脚本
    cmd = [
        'python',
        'scripts/data_generator.py',
        '--preset', 'demo',  # 默认使用 demo 预设
        '--execution-date', execution_date,
        '--output', str(local_output),
    ]

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=True,
        )

        print(result.stdout)

        # 统计生成的数据
        output_dir = RAW_DATA_PATH / f"date={execution_date}"
        json_files = list(output_dir.glob('**/*.json'))

        total_records = 0
        for json_file in json_files:
            import json
            with open(json_file) as f:
                data = json.load(f)
                total_records += len(data.get('data', []))

        print(f"\n✅ Generated {total_records} records from {len(json_files)} files")

        # Push to XCom
        context['task_instance'].xcom_push(key='total_records', value=total_records)
        context['task_instance'].xcom_push(key='output_path', value=str(output_dir))
        context['task_instance'].xcom_push(key='data_source', value='simulated')

    except subprocess.CalledProcessError as e:
        print(f"❌ Error generating data: {e}")
        print(f"STDOUT: {e.stdout}")
        print(f"STDERR: {e.stderr}")
        raise


def fetch_stock_data_from_api(**context):
    """
    从 Alpha Vantage API 获取真实股票数据（备用方式）

    使用场景：
    - 需要真实历史数据
    - 生产环境
    - 回测验证

    要求：
    - 设置 ALPHA_VANTAGE_API_KEY 环境变量
    - 注意 API 限额（5 req/min, 500 req/day）
    """
    import sys
    import json
    from pathlib import Path

    # Add src to path
    sys.path.insert(0, str(PROJECT_ROOT / 'src'))

    from ingestion.alpha_vantage_client import AlphaVantageClient

    # Initialize client
    api_key = os.getenv('ALPHA_VANTAGE_API_KEY')
    if not api_key:
        raise ValueError(
            "ALPHA_VANTAGE_API_KEY not set.\n"
            "Get free API key at: https://www.alphavantage.co/support/#api-key\n"
            "Or use generate_sample_data() for demo purposes."
        )

    client = AlphaVantageClient(api_key=api_key)

    # Symbols to fetch
    symbols = ['AAPL', 'GOOGL', 'MSFT', 'AMZN', 'TSLA']

    execution_date = context['ds']
    output_dir = RAW_DATA_PATH / f"date={execution_date}"
    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"📥 Fetching REAL data from Alpha Vantage API...")
    print(f"   Symbols: {', '.join(symbols)}")

    total_records = 0
    for symbol in symbols:
        try:
            print(f"  Fetching {symbol}...")
            data = client.get_daily_adjusted(symbol, outputsize='compact')

            if not data:
                print(f"  ⚠️  No data for {symbol}")
                continue

            # Save to JSON
            symbol_dir = output_dir / f"symbol={symbol}"
            symbol_dir.mkdir(parents=True, exist_ok=True)

            output_file = symbol_dir / f"{symbol}_{execution_date}.json"
            with open(output_file, 'w') as f:
                json.dump(data, f, indent=2)

            records_count = len(data.get('data', []))
            total_records += records_count
            print(f"  ✅ {symbol}: {records_count} records")

        except Exception as e:
            print(f"  ❌ Error fetching {symbol}: {e}")
            continue

    if total_records == 0:
        raise ValueError("No data fetched from API")

    print(f"\n✅ Total fetched: {total_records} records from Alpha Vantage API")

    # Push to XCom
    context['task_instance'].xcom_push(key='total_records', value=total_records)
    context['task_instance'].xcom_push(key='output_path', value=str(output_dir))
    context['task_instance'].xcom_push(key='data_source', value='alpha_vantage_api')


def verify_ingestion(**context):
    """
    验证数据摄取完成
    """
    execution_date = context['ds']
    output_dir = RAW_DATA_PATH / f"date={execution_date}"

    if not output_dir.exists():
        raise FileNotFoundError(f"Output directory not found: {output_dir}")

    json_files = list(output_dir.glob('**/*.json'))

    if not json_files:
        raise ValueError(f"No JSON files found in {output_dir}")

    total_size = sum(f.stat().st_size for f in json_files) / (1024 * 1024)

    print(f"✅ Ingestion verified:")
    print(f"   Files: {len(json_files)}")
    print(f"   Total size: {total_size:.2f} MB")
    print(f"   Location: {output_dir}")


def verify_transformation(**context):
    """
    验证 Spark 转换输出
    """
    execution_date = context['ds']

    # Check processed directory
    processed_dir = CURATED_DATA_PATH / 'processed'
    if not processed_dir.exists():
        raise FileNotFoundError(f"Processed directory not found: {processed_dir}")

    # Check for parquet files
    parquet_files = list(processed_dir.glob('**/*.parquet'))
    if not parquet_files:
        raise ValueError(f"No parquet files found in {processed_dir}")

    total_size = sum(f.stat().st_size for f in parquet_files) / (1024 * 1024)

    print(f"✅ Transformation verified:")
    print(f"   Parquet files: {len(parquet_files)}")
    print(f"   Total size: {total_size:.2f} MB")

    # Check daily snapshot
    snapshot_path = CURATED_DATA_PATH / 'daily_snapshots' / f"date={execution_date}"
    if snapshot_path.exists():
        snapshot_files = list(snapshot_path.glob('*.parquet'))
        print(f"   Snapshot files: {len(snapshot_files)}")


def load_to_postgres(**context):
    """
    从 Parquet 加载到 PostgreSQL (替代 Redshift)
    """
    import sys
    sys.path.insert(0, str(PROJECT_ROOT / 'src'))

    from loading.postgres_loader import load_parquet_to_postgres

    execution_date = context['ds']
    snapshot_path = CURATED_DATA_PATH / 'daily_snapshots' / f"date={execution_date}"

    # Find parquet file
    parquet_files = list(snapshot_path.glob('*.parquet'))
    if not parquet_files:
        raise FileNotFoundError(f"No parquet files in {snapshot_path}")

    parquet_file = parquet_files[0]

    print(f"📥 Loading from: {parquet_file}")

    # Load to PostgreSQL
    rows_loaded = load_parquet_to_postgres(
        parquet_path=str(parquet_file),
        table_name='fact_stock_prices',
        connection_params={
            'host': 'postgres',
            'database': 'financial_dw',
            'user': 'airflow',
            'password': 'airflow'
        }
    )

    print(f"✅ Loaded {rows_loaded} rows to PostgreSQL")

    context['task_instance'].xcom_push(key='rows_loaded', value=rows_loaded)


def send_success_notification(**context):
    """
    发送成功通知
    """
    execution_date = context['ds']
    total_records = context['task_instance'].xcom_pull(
        task_ids='fetch_stock_data',
        key='total_records'
    )
    rows_loaded = context['task_instance'].xcom_pull(
        task_ids='load_to_postgres',
        key='rows_loaded'
    )

    message = f"""
✅ Financial Data Pipeline Success

Execution Date: {execution_date}
Records Fetched: {total_records}
Records Loaded: {rows_loaded}

Scala + Spark transformation completed successfully!
    """

    print(message)

    # Optional: Send to Slack
    # slack_webhook = os.getenv('SLACK_WEBHOOK_URL')
    # if slack_webhook:
    #     import requests
    #     requests.post(slack_webhook, json={'text': message})


# ============================================================================
# DAG Definition
# ============================================================================

with DAG(
    dag_id='financial_data_pipeline_scala',
    default_args=DEFAULT_ARGS,
    description='Financial data ETL pipeline with Scala + Spark',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['scala', 'spark', 'etl', 'financial'],
    max_active_runs=1,
    sla_miss_callback=None,  # Add custom SLA callback if needed
) as dag:

    # Start marker
    start = DummyOperator(task_id='start')

    # Pre-flight checks
    with TaskGroup('pre_flight_checks', tooltip='验证环境配置') as pre_flight:

        check_spark = PythonOperator(
            task_id='check_spark_cluster',
            python_callable=check_spark_cluster,
            doc_md="""
            ### Check Spark Cluster
            验证 Spark 集群是否可用
            """
        )

        check_jar = PythonOperator(
            task_id='verify_jar_exists',
            python_callable=verify_jar_exists,
            doc_md="""
            ### Verify JAR
            验证 Scala JAR 文件存在
            """
        )

        check_spark >> check_jar

    # Data ingestion - 默认使用模拟数据
    fetch = PythonOperator(
        task_id='generate_sample_data',
        python_callable=generate_sample_data,  # 改为使用模拟数据生成
        sla=timedelta(minutes=10),
        doc_md="""
        ### Generate Sample Data
        生成模拟股票数据（推荐用于演示）

        - Symbols: AAPL, GOOGL, MSFT, AMZN, TSLA
        - Data: 模拟真实市场行为（价格波动、分红、拆股）
        - Output: JSON files in data/raw/
        - 优势: 无需 API Key, 100% 稳定, 无速率限制

        如需使用真实 API 数据，请替换为 fetch_stock_data_from_api
        """
    )

    # Upload to S3 if AWS mode
    upload_s3 = PythonOperator(
        task_id='upload_to_s3',
        python_callable=upload_to_s3_if_needed,
        doc_md="""
        ### Upload to S3 (AWS mode only)
        如果 DEPLOYMENT_MODE=aws，上传数据到 S3
        """
    )

    verify_fetch = PythonOperator(
        task_id='verify_ingestion',
        python_callable=verify_ingestion,
        doc_md="""
        ### Verify Ingestion
        验证数据摄取完成
        """
    )

    # Scala Spark transformation
    spark_transform = SparkSubmitOperator(
        task_id='scala_spark_transform',
        application=str(JAR_PATH),
        java_class=MAIN_CLASS,
        conn_id='spark_default',
        conf={
            'spark.sql.shuffle.partitions': '8',
            'spark.sql.adaptive.enabled': 'true',
            'spark.sql.adaptive.coalescePartitions.enabled': 'true',
        },
        driver_memory='1g',
        executor_memory='2g',
        executor_cores=2,
        num_executors=2,
        application_args=[
            '--source-path', str(RAW_DATA_PATH),
            '--target-path', str(CURATED_DATA_PATH),
            '--execution-date', '{{ ds }}',
        ],
        sla=timedelta(hours=1),
        doc_md="""
        ### Scala Spark Transformation

        运行 Scala + Spark ETL 作业:
        - 读取 JSON 原始数据
        - 数据清洗和验证
        - 计算技术指标 (SMA, EMA, 波动率)
        - 写入 Parquet 文件

        **Language:** Scala 2.12
        **Framework:** Apache Spark 3.4.1
        """
    )

    verify_transform = PythonOperator(
        task_id='verify_transformation',
        python_callable=verify_transformation,
        doc_md="""
        ### Verify Transformation
        验证 Spark 转换输出
        """
    )

    # Load to data warehouse
    load = PythonOperator(
        task_id='load_to_postgres',
        python_callable=load_to_postgres,
        sla=timedelta(minutes=30),
        doc_md="""
        ### Load to PostgreSQL
        将 Parquet 数据加载到 PostgreSQL
        """
    )

    # Success notification
    notify = PythonOperator(
        task_id='send_success_notification',
        python_callable=send_success_notification,
        trigger_rule='all_success',
        doc_md="""
        ### Success Notification
        发送成功通知
        """
    )

    # End marker
    end = DummyOperator(task_id='end')

    # Define task dependencies
    start >> pre_flight >> fetch >> upload_s3 >> verify_fetch
    verify_fetch >> spark_transform >> verify_transform
    verify_transform >> load >> notify >> end


# ============================================================================
# DAG Documentation
# ============================================================================

dag.doc_md = """
# Financial Data Pipeline - Scala + Spark Version

## 概述

使用 **Scala + Spark** 的生产级金融数据 ETL 管道。

## 架构

```
Alpha Vantage API
      ↓
  [Python] fetch_stock_data
      ↓
  JSON → data/raw/
      ↓
  [Scala + Spark] spark_transform
      ↓
  Parquet → data/curated/
      ↓
  [Python] load_to_postgres
      ↓
  PostgreSQL
```

## 技术栈

- **编排**: Apache Airflow
- **数据处理**: Scala + Apache Spark 3.4.1
- **存储格式**: Parquet (Snappy 压缩)
- **数据仓库**: PostgreSQL (本地) / Redshift (生产)

## 核心功能

### Scala Spark 转换
- ✅ 类型安全的数据处理
- ✅ 高性能并行计算
- ✅ 完整的技术指标计算
- ✅ 优化的 Parquet 写入

### 技术指标
- Simple Moving Averages (SMA): 5, 20, 50 天
- Exponential Moving Averages (EMA): 12, 26 天
- 波动率 (Volatility): 20 天标准差
- 日收益率、价格区间、成交量变化

## 运行环境

### 本地 Docker
```bash
# 启动 Spark 集群
docker-compose -f docker-compose-spark.yml up -d

# 构建 Scala JAR
sbt assembly

# 触发 DAG
airflow dags trigger financial_data_pipeline_scala
```

### AWS 生产环境
- 替换 PostgreSQL → Redshift
- 替换本地 Spark → AWS Glue
- 添加 S3 作为数据存储

## 监控

- Airflow UI: http://localhost:8080
- Spark Master UI: http://localhost:8081
- Spark Application UI: http://localhost:4040

## 成本优化

**本地开发**: $0/月
- Docker Compose 运行全部服务
- 完全免费

**AWS 生产**: 可通过以下优化降低成本
- 使用 Glue Flex 执行类
- Redshift 自动暂停
- S3 生命周期策略

## 维护

- 定期检查 Spark 日志
- 监控 Parquet 文件大小
- 验证数据质量指标
"""
