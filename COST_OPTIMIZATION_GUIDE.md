# 求职项目成本优化指南 - 降至接近 $0/月

## 📊 当前成本 vs 优化后成本

| 组件 | 当前月成本 | 优化后月成本 | 节省 |
|------|-----------|-------------|------|
| AWS MWAA | $3,000-21,000 | **$0** (本地Airflow) | 100% |
| Redshift Serverless | $30,000-99,000 | **$0** (PostgreSQL/DuckDB) | 100% |
| AWS Glue | $3,000-15,000 | **$0** (本地PySpark/Pandas) | 100% |
| NAT Gateway | $32/月 | **$0** (删除VPC) | 100% |
| KMS | $50-80 | **$0** (S3默认加密) | 100% |
| S3 | $50-200 | **$0.23-5** (仅免费层) | 95%+ |
| CloudWatch | $20-50 | **$0** (基础免费) | 100% |
| Lambda | $0 (免费层) | **$0** | - |
| Secrets Manager | $0.80 | **$0** (环境变量) | 100% |
| **总计** | **$36,000-135,000** | **< $5/月** | **99.9%** |

---

## 🚀 架构改造方案

### 方案 1: 完全本地 + AWS S3 (推荐用于演示)
**月成本: $0-1**

```
本地开发机
├── Docker Compose
│   ├── Airflow (standalone)
│   ├── PostgreSQL (数据仓库)
│   ├── Grafana (监控)
│   └── MinIO (本地S3模拟 - 可选)
└── Python Scripts
    ├── Pandas/DuckDB (替代Glue)
    └── API调用 (Alpha Vantage免费层)

↓ (可选) 上传结果到 AWS S3 (免费层: 5GB)
```

**优势:**
- ✅ 完全免费运行
- ✅ 可以在笔记本上演示
- ✅ 启动快速 (docker-compose up)
- ✅ 保留完整技术栈展示
- ✅ 可选择性上传到AWS S3展示云集成

---

### 方案 2: AWS 免费层架构
**月成本: $0-5**

```
[Alpha Vantage API - 免费层]
        ↓
[EC2 t2.micro - 免费层 750小时/月]
  ├── Airflow Standalone
  ├── PostgreSQL
  └── Python ETL脚本
        ↓
[S3 免费层 - 5GB存储, 20,000 GET, 2,000 PUT]
        ↓
[DuckDB on EC2 - 查询S3直接]
        ↓
[CloudWatch 免费层 - 5GB日志]
```

**优势:**
- ✅ 在真实AWS环境运行
- ✅ 几乎免费 (< $5/月)
- ✅ 适合面试时展示
- ✅ 可扩展到付费层

---

### 方案 3: GitHub Actions + 免费服务
**月成本: $0**

```
[GitHub Actions - 2000分钟/月免费]
  ├── 定时触发 (cron)
  ├── Python脚本 (Pandas处理)
  └── 单元测试
        ↓
[GitHub Releases / Artifacts]
  └── 存储Parquet文件
        ↓
[DuckDB查询本地]
  └── 或上传到 Kaggle Datasets (免费)
```

**优势:**
- ✅ 100% 免费
- ✅ 展示CI/CD能力
- ✅ 公开可见 (开源项目)
- ✅ 自动化文档生成

---

## 🛠️ 详细实现方案

### 实施方案 1: 本地 Docker 架构 (最推荐)

#### 新的 `docker-compose.yml`

```yaml
version: '3.8'

services:
  # PostgreSQL - 替代 Redshift
  postgres:
    image: postgres:15-alpine
    environment:
      POSTGRES_USER: airflow
      POSTGRES_PASSWORD: airflow
      POSTGRES_DB: financial_dw
    volumes:
      - postgres_data:/var/lib/postgresql/data
      - ./sql/init.sql:/docker-entrypoint-initdb.d/init.sql
    ports:
      - "5432:5432"

  # Airflow - 替代 AWS MWAA
  airflow:
    image: apache/airflow:2.8.1-python3.11
    environment:
      AIRFLOW__CORE__EXECUTOR: LocalExecutor
      AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres/financial_dw
      AIRFLOW__CORE__LOAD_EXAMPLES: 'false'
      ALPHA_VANTAGE_API_KEY: ${ALPHA_VANTAGE_API_KEY}
      AWS_ACCESS_KEY_ID: ${AWS_ACCESS_KEY_ID:-none}
      AWS_SECRET_ACCESS_KEY: ${AWS_SECRET_ACCESS_KEY:-none}
    volumes:
      - ./dags:/opt/airflow/dags
      - ./src:/opt/airflow/src
      - ./config:/opt/airflow/config
      - ./data:/opt/airflow/data  # 本地数据存储
      - airflow_logs:/opt/airflow/logs
    ports:
      - "8080:8080"
    command: >
      bash -c "airflow db init &&
               airflow users create --username admin --password admin --firstname Admin --lastname User --role Admin --email admin@example.com &&
               airflow webserver & airflow scheduler"
    depends_on:
      - postgres

  # Grafana - 监控可视化 (替代 CloudWatch Dashboard)
  grafana:
    image: grafana/grafana:latest
    environment:
      GF_SECURITY_ADMIN_PASSWORD: admin
      GF_INSTALL_PLUGINS: grafana-postgresql-datasource
    volumes:
      - grafana_data:/var/lib/grafana
      - ./monitoring/grafana:/etc/grafana/provisioning
    ports:
      - "3000:3000"
    depends_on:
      - postgres

  # MinIO - 本地S3替代 (可选)
  minio:
    image: minio/minio:latest
    command: server /data --console-address ":9001"
    environment:
      MINIO_ROOT_USER: minioadmin
      MINIO_ROOT_PASSWORD: minioadmin
    volumes:
      - minio_data:/data
    ports:
      - "9000:9000"
      - "9001:9001"

volumes:
  postgres_data:
  airflow_logs:
  grafana_data:
  minio_data:
```

#### 成本: **$0/月**

---

### 技术栈映射

| 原AWS服务 | 免费替代 | 功能保留 |
|----------|---------|---------|
| AWS MWAA | **Apache Airflow (Docker)** | ✅ 100% |
| Redshift Serverless | **PostgreSQL 15** | ✅ 95% (无大规模并行) |
| AWS Glue | **Pandas + DuckDB** | ✅ 90% (小数据集) |
| S3 | **本地文件系统 / MinIO** | ✅ 100% (开发) |
| KMS | **不加密 / GPG** | ⚠️ (演示环境可接受) |
| CloudWatch | **Grafana + Loki** | ✅ 95% |
| Secrets Manager | **.env 文件** | ⚠️ (演示环境可接受) |
| Lambda | **Airflow PythonOperator** | ✅ 100% |
| NAT Gateway | **删除 (本地无需)** | N/A |

---

## 📝 迁移步骤

### Step 1: 创建 Docker Compose 环境

```bash
# 1. 创建目录结构
mkdir -p data/{raw,curated,validation}
mkdir -p sql monitoring/grafana

# 2. 创建 .env 文件
cat > .env << 'EOF'
ALPHA_VANTAGE_API_KEY=your_free_api_key_here
POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow
POSTGRES_DB=financial_dw
EOF

# 3. 启动服务
docker-compose up -d

# 4. 访问服务
# Airflow UI: http://localhost:8080 (admin/admin)
# Grafana: http://localhost:3000 (admin/admin)
# MinIO: http://localhost:9001 (minioadmin/minioadmin)
```

---

### Step 2: 修改 Python ETL 代码

**替换 AWS Glue 为 Pandas/DuckDB:**

创建 `src/transformation/local_transform.py`:

```python
import pandas as pd
import duckdb
from pathlib import Path

def transform_stock_data(input_path: str, output_path: str):
    """
    本地替代 AWS Glue PySpark 作业
    使用 Pandas + DuckDB (零成本)
    """
    # 读取 JSON (替代 Glue 的 S3 读取)
    df = pd.read_json(input_path)

    # 数据清洗
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df = df.dropna(subset=['close', 'volume'])

    # 技术指标计算
    df['sma_5'] = df.groupby('symbol')['close'].rolling(5).mean().reset_index(0, drop=True)
    df['sma_20'] = df.groupby('symbol')['close'].rolling(20).mean().reset_index(0, drop=True)
    df['daily_return'] = df.groupby('symbol')['close'].pct_change()

    # 写入 Parquet (替代 Glue 的 S3 写入)
    df.to_parquet(output_path, compression='snappy', index=False)

    return len(df)

# DuckDB 直接查询 Parquet
def query_parquet(parquet_path: str):
    """
    DuckDB 可以直接查询 Parquet (替代 Redshift)
    """
    conn = duckdb.connect(':memory:')
    result = conn.execute(f"""
        SELECT
            symbol,
            date_trunc('day', timestamp) as date,
            AVG(close) as avg_close,
            SUM(volume) as total_volume
        FROM read_parquet('{parquet_path}')
        GROUP BY symbol, date
        ORDER BY date DESC
    """).df()
    return result
```

**更新 Airflow DAG:**

```python
# dags/financial_data_pipeline_local.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys
sys.path.insert(0, '/opt/airflow/src')

from transformation.local_transform import transform_stock_data
from ingestion.alpha_vantage_client import fetch_stock_data

with DAG(
    'financial_data_pipeline_local',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False
) as dag:

    fetch = PythonOperator(
        task_id='fetch_stock_data',
        python_callable=fetch_stock_data,
        op_kwargs={'symbols': ['AAPL', 'GOOGL']}
    )

    transform = PythonOperator(
        task_id='transform_data',
        python_callable=transform_stock_data,
        op_kwargs={
            'input_path': '/opt/airflow/data/raw/{{ ds }}.json',
            'output_path': '/opt/airflow/data/curated/{{ ds }}.parquet'
        }
    )

    fetch >> transform
```

---

### Step 3: 数据库迁移 (Redshift → PostgreSQL)

**创建 `sql/init.sql`:**

```sql
-- 创建与 Redshift 相同的表结构
CREATE TABLE IF NOT EXISTS fact_stock_prices (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(10) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    open_price DECIMAL(18,4),
    high_price DECIMAL(18,4),
    low_price DECIMAL(18,4),
    close_price DECIMAL(18,4) NOT NULL,
    volume BIGINT,
    sma_5 DECIMAL(18,4),
    sma_20 DECIMAL(18,4),
    sma_50 DECIMAL(18,4),
    daily_return DECIMAL(10,6),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(symbol, timestamp)
);

CREATE INDEX idx_symbol_timestamp ON fact_stock_prices(symbol, timestamp DESC);

-- 创建监控表 (替代 CloudWatch Metrics)
CREATE TABLE IF NOT EXISTS pipeline_metrics (
    id SERIAL PRIMARY KEY,
    metric_name VARCHAR(100),
    metric_value DECIMAL(18,4),
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

**COPY 替代方案:**

```python
# src/loading/postgres_loader.py
import psycopg2
import pandas as pd

def load_to_postgres(parquet_path: str):
    """
    替代 Redshift COPY 命令
    """
    df = pd.read_parquet(parquet_path)

    conn = psycopg2.connect(
        host='postgres',
        database='financial_dw',
        user='airflow',
        password='airflow'
    )

    # 批量插入 (使用 ON CONFLICT 处理重复)
    with conn.cursor() as cur:
        for _, row in df.iterrows():
            cur.execute("""
                INSERT INTO fact_stock_prices
                (symbol, timestamp, close_price, volume, sma_5, sma_20, daily_return)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (symbol, timestamp) DO NOTHING
            """, (row['symbol'], row['timestamp'], row['close'],
                  row['volume'], row['sma_5'], row['sma_20'], row['daily_return']))

    conn.commit()
    conn.close()
```

---

### Step 4: 监控替代方案

**Grafana 仪表板配置:**

创建 `monitoring/grafana/dashboards/pipeline.json`:

```json
{
  "dashboard": {
    "title": "Financial Data Pipeline",
    "panels": [
      {
        "title": "Daily Records Ingested",
        "targets": [{
          "rawSql": "SELECT date_trunc('day', created_at) as time, COUNT(*) FROM fact_stock_prices GROUP BY 1 ORDER BY 1"
        }]
      },
      {
        "title": "Pipeline Success Rate",
        "targets": [{
          "rawSql": "SELECT metric_name, AVG(metric_value) FROM pipeline_metrics WHERE metric_name='success_rate' GROUP BY 1"
        }]
      }
    ]
  }
}
```

**Slack 通知替代:**

```python
# 使用 Airflow Email 或简单的 HTTP 请求
from airflow.operators.python import PythonOperator
import requests

def send_notification(context):
    message = f"Pipeline {context['dag'].dag_id} completed"
    # 可选: 使用免费的 Slack incoming webhook
    # 或者仅打印到日志
    print(message)
```

---

## 🎯 Alpha Vantage API 免费层限制

**免费计划:**
- ✅ 5 请求/分钟
- ✅ 500 请求/天
- ✅ 所有端点访问

**应对策略:**
```python
# src/ingestion/alpha_vantage_client.py
import time

def fetch_with_rate_limit(symbols):
    """
    符合免费层限制: 5 req/min
    """
    for symbol in symbols:
        data = fetch_stock_data(symbol)
        time.sleep(12)  # 12秒 = 5次/分钟
        yield data
```

**建议:**
- 只跟踪 3-5 个股票符号
- 每天运行 1 次 (而非实时)
- 使用缓存避免重复请求

---

## 💰 实际成本分析

### 完全本地方案 (Docker)

| 资源 | 成本 |
|------|------|
| 计算 (本地笔记本) | $0 (已有设备) |
| 存储 (1GB数据) | $0 (本地磁盘) |
| 网络 | $0 (少量API调用) |
| **月度总计** | **$0** |

**额外硬件需求:**
- 最低: 4GB RAM, 20GB 磁盘
- 推荐: 8GB RAM, 50GB 磁盘

---

### AWS 免费层方案 (可选)

| 服务 | 免费层 | 预计使用 | 超额成本 |
|------|-------|---------|---------|
| EC2 t2.micro | 750小时/月 | 730小时 | $0 |
| S3 | 5GB, 20K GET, 2K PUT | 2GB, 10K GET, 500 PUT | $0 |
| CloudWatch | 5GB 日志 | 1GB | $0 |
| 数据传输 | 100GB 出站 | 5GB | $0 |
| **月度总计** | | | **$0-2** |

**注意:** 免费层有效期 **12个月** (从注册AWS账号起)

---

## 📊 功能对比矩阵

| 功能 | AWS生产版 | 本地版 | 保留度 |
|------|----------|-------|-------|
| 工作流编排 | MWAA | Airflow Docker | ✅ 100% |
| 数据质量 | Great Expectations | Great Expectations | ✅ 100% |
| ETL处理 | Glue PySpark | Pandas/DuckDB | ✅ 90% |
| 数据仓库 | Redshift | PostgreSQL | ✅ 85% |
| 对象存储 | S3 | 本地文件系统 | ✅ 95% |
| 监控 | CloudWatch | Grafana | ✅ 90% |
| 告警通知 | Lambda → Slack | Airflow Email | ✅ 80% |
| 加密 | KMS | 无/GPG | ⚠️ 50% |
| 可扩展性 | 高 | 低 | ⚠️ 30% |
| **总体技能展示** | | | **✅ 90%+** |

---

## 🚀 快速开始

### 1. 一键启动本地环境

```bash
# 克隆项目
git clone <your-repo>
cd aws-mwaa-financial-data-platform

# 创建免费 Alpha Vantage API Key
# 访问: https://www.alphavantage.co/support/#api-key
# 添加到 .env
echo "ALPHA_VANTAGE_API_KEY=YOUR_KEY" > .env

# 启动 Docker 环境
docker-compose up -d

# 等待 30秒 服务启动
sleep 30

# 访问 Airflow
open http://localhost:8080
# 登录: admin / admin

# 触发 DAG
curl -X POST http://localhost:8080/api/v1/dags/financial_data_pipeline_local/dagRuns \
  -u admin:admin \
  -H "Content-Type: application/json" \
  -d '{"conf":{}}'
```

### 2. 验证数据

```bash
# 连接 PostgreSQL
docker exec -it postgres psql -U airflow -d financial_dw

# 查询数据
SELECT symbol, COUNT(*) as record_count
FROM fact_stock_prices
GROUP BY symbol;

# 查看最新价格
SELECT * FROM fact_stock_prices
ORDER BY timestamp DESC
LIMIT 10;
```

### 3. 查看监控

```bash
# Grafana
open http://localhost:3000
# 登录: admin / admin

# 导入仪表板
# 使用 monitoring/grafana/dashboards/pipeline.json
```

---

## 📝 面试展示策略

### 演示流程 (5分钟)

1. **展示架构图** (1分钟)
   - "这是生产级AWS架构，但我创建了成本优化版本..."

2. **运行 Docker Compose** (30秒)
   ```bash
   docker-compose up -d
   ```

3. **触发 Airflow DAG** (1分钟)
   - 打开 Airflow UI
   - 手动触发 DAG
   - 展示任务依赖图

4. **查询数据** (1分钟)
   - PostgreSQL 查询技术指标
   - DuckDB 查询 Parquet 文件

5. **展示监控** (1分钟)
   - Grafana 仪表板
   - 数据质量报告

6. **代码讲解** (1分钟)
   - 数据验证规则
   - ETL 转换逻辑

### 关键话术

> "为了展示技术能力而不产生高额费用，我设计了两个版本："
>
> 1. **生产版 (AWS)**: 使用 MWAA、Glue、Redshift - 展示企业级架构设计能力
> 2. **演示版 (Docker)**: 完全本地运行 - 展示成本意识和架构灵活性
>
> "演示版保留了 90%+ 的核心功能，月成本从 $36K 降至 $0，同时：
> - ✅ 保留相同的 Airflow DAG 逻辑
> - ✅ 相同的数据质量验证
> - ✅ 相同的 ETL 处理流程
> - ✅ 可一键切换回 AWS"

---

## 🎓 技能展示清单

通过这个项目，你可以展示:

### 数据工程
- ✅ ETL 管道设计
- ✅ 数据质量验证 (Great Expectations)
- ✅ 列式存储优化 (Parquet)
- ✅ 增量数据处理
- ✅ SQL 查询优化

### 云架构
- ✅ AWS 服务选型 (IaC: CloudFormation)
- ✅ 成本优化意识
- ✅ 安全最佳实践 (IAM, 加密)
- ✅ 监控和告警

### DevOps
- ✅ Docker / Docker Compose
- ✅ 基础设施即代码 (IaC)
- ✅ CI/CD (可选: GitHub Actions)
- ✅ 日志管理

### Python 开发
- ✅ API 客户端 (重试、限流)
- ✅ Pandas / PySpark
- ✅ 测试 (pytest)
- ✅ 代码质量 (black, flake8)

---

## 🔄 升级路径

### 如果需要实际部署 AWS (面试后)

可以提供一个"预算意识版 AWS 配置":

```yaml
# infrastructure/cloudformation/budget-version.yaml
Parameters:
  Environment:
    Default: demo  # 而非 production

Mappings:
  EnvironmentConfig:
    demo:
      MWAAClass: mw1.small
      MWAAMaxWorkers: 2       # 从 10 降至 2
      RedshiftRPU: 8          # 从 32 降至 8
      RedshiftAutoPause: 1800 # 30分钟无活动后暂停
      GlueWorkers: 2          # 从 10 降至 2
      GlueExecution: FLEX     # 节省 60%
```

**预计月成本: $500-800** (仍然昂贵但可接受)

---

## 📚 附加资源

### 配置文件生成

我可以帮你生成:
1. ✅ 完整的 `docker-compose.yml`
2. ✅ 简化版 Airflow DAG
3. ✅ Pandas 替代 Glue 脚本
4. ✅ PostgreSQL 初始化 SQL
5. ✅ Grafana 仪表板 JSON
6. ✅ README 演示说明

### 文档建议

在项目 README 中添加:

```markdown
## 💰 成本说明

本项目提供两种部署模式:

### 🏢 生产模式 (AWS)
- 适合: 企业环境，大规模数据
- 成本: $36,000-135,000/月
- 架构: 见 `infrastructure/cloudformation/main.yaml`

### 💻 演示模式 (Docker)
- 适合: 本地开发，求职展示
- 成本: $0/月
- 架构: 见 `docker-compose.yml`

**演示模式提供 90%+ 功能，适合技术面试展示**
```

---

## ✅ 总结

### 行动计划

1. **立即可做:**
   - ✅ 创建 `docker-compose.yml` (我可以帮你生成)
   - ✅ 添加 `COST_OPTIMIZATION_GUIDE.md` (本文档)
   - ✅ 修改 DAG 支持本地模式
   - ✅ 更新 README 说明两种模式

2. **可选 (增强简历):**
   - ✅ 添加 GitHub Actions CI/CD
   - ✅ 创建 Jupyter Notebook 数据分析示例
   - ✅ 录制 5 分钟演示视频
   - ✅ 部署静态网站展示架构图

3. **避免:**
   - ❌ 部署完整 AWS 版本 (除非面试官要求)
   - ❌ 长时间运行 MWAA/Redshift
   - ❌ 启用不必要的 AWS 服务

### 面试时的回答模板

**面试官**: "这个项目的 AWS 成本是多少?"

**你**: "生产级部署约 $3.6万-13.5万/月。但作为求职项目，我设计了本地 Docker 版本，成本为 $0，同时保留 90%+ 核心功能。这展示了我的成本意识和架构灵活性。需要的话我可以在 2分钟内演示完整数据管道。"

---

需要我帮你生成任何配置文件吗? 我可以创建:
- [ ] docker-compose.yml (完整版)
- [ ] 简化的 Airflow DAG
- [ ] Pandas ETL 脚本
- [ ] PostgreSQL 初始化脚本
- [ ] Grafana 仪表板配置
- [ ] 一键启动脚本 (setup.sh)

告诉我你需要哪些! 🚀
