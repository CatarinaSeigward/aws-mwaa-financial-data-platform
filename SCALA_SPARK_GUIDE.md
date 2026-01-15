# Scala + Spark 实施指南

## 🎯 改造总结

项目已从 **PySpark** 改为 **纯 Scala + Spark**，具备以下优势：

### ✅ 技术优势

| 特性 | PySpark | Scala + Spark |
|------|---------|--------------|
| **类型安全** | ❌ 运行时检查 | ✅ **编译时检查** |
| **性能** | 中等 (Python → JVM) | ✅ **高性能 (原生 JVM)** |
| **代码质量** | 动态类型 | ✅ **静态类型 + 模式匹配** |
| **部署** | 依赖 Python | ✅ **单个 JAR 文件** |
| **面试展示** | 常见 | ✅ **更显专业度** |
| **企业采用** | 数据科学 | ✅ **大数据工程** |

### 💰 成本对比

| 环境 | 成本 | 说明 |
|------|------|------|
| **本地 Docker** | **$0/月** | ✅ 推荐求职演示 |
| **AWS Glue (优化)** | $500-800/月 | 生产环境 |
| **AWS 原配置** | $36K-135K/月 | ❌ 太贵 |

---

## 📦 项目结构

```
financial-data-platform/
├── src/
│   └── transformation/
│       └── scala/
│           └── FinancialDataTransform.scala  ← 主 ETL 逻辑
│
├── build.sbt                                  ← Scala 项目配置
├── project/
│   ├── build.properties                       ← SBT 版本
│   └── plugins.sbt                            ← SBT 插件
│
├── dags/
│   └── financial_data_pipeline_scala.py      ← Airflow DAG
│
├── docker-compose-spark.yml                   ← Spark 集群配置
├── scripts/
│   └── build-and-submit.sh                    ← 构建+提交脚本
│
└── data/
    ├── raw/                                   ← JSON 原始数据
    └── curated/                               ← Parquet 输出
```

---

## 🚀 快速开始

### 前置要求

#### 必需 (本地运行)
```bash
# 1. Docker + Docker Compose
docker --version  # 20.10+
docker-compose --version  # 1.29+

# 2. Java JDK
java -version  # Java 11 或 17

# 3. Scala Build Tool (SBT)
sbt --version  # 1.9+
```

#### 安装 SBT

**macOS:**
```bash
brew install sbt
```

**Ubuntu/Debian:**
```bash
echo "deb https://repo.scala-sbt.org/scalasbt/debian all main" | sudo tee /etc/apt/sources.list.d/sbt.list
curl -sL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x2EE0EA64E40A89B84B2DF73499E82A75642AC823" | sudo apt-key add
sudo apt-get update
sudo apt-get install sbt
```

**Windows:**
```bash
choco install sbt
# 或下载安装: https://www.scala-sbt.org/download.html
```

---

## 🏗️ 步骤 1: 构建 Scala JAR

### 方法 A: 使用脚本 (推荐)

```bash
# 进入项目目录
cd aws-mwaa-financial-data-platform

# 构建 JAR
./scripts/build-and-submit.sh local --rebuild
```

### 方法 B: 手动构建

```bash
# 清理
sbt clean

# 编译
sbt compile

# 运行测试
sbt test

# 创建 fat JAR (包含所有依赖)
sbt assembly

# 输出: target/scala-2.12/financial-etl-1.0.0.jar
```

**预期输出:**
```
[info] Strategy 'discard' was applied to 2 files
[info] Strategy 'concat' was applied to 2 files
[success] Total time: 45 s
[success] Built JAR: target/scala-2.12/financial-etl-1.0.0.jar (35.2 MB)
```

---

## 🐳 步骤 2: 启动 Docker 环境

### 启动所有服务

```bash
# 启动 Spark 集群 + Airflow + PostgreSQL
docker-compose -f docker-compose-spark.yml up -d

# 查看服务状态
docker-compose -f docker-compose-spark.yml ps
```

**预期服务:**
```
NAME                      STATUS    PORTS
financial-spark-master    Up        0.0.0.0:7077->7077/tcp, 0.0.0.0:8081->8081/tcp
financial-spark-worker-1  Up
financial-spark-worker-2  Up
financial-airflow         Up        0.0.0.0:8080->8080/tcp
financial-postgres        Up        0.0.0.0:5432->5432/tcp
financial-grafana         Up        0.0.0.0:3000->3000/tcp
financial-minio           Up        0.0.0.0:9000-9001->9000-9001/tcp
```

### 访问 Web UI

| 服务 | URL | 用户名 | 密码 |
|------|-----|--------|------|
| **Airflow** | http://localhost:8080 | admin | admin |
| **Spark Master** | http://localhost:8081 | - | - |
| **Grafana** | http://localhost:3000 | admin | admin |
| **MinIO** | http://localhost:9001 | minioadmin | minioadmin |

---

## 🎬 步骤 3: 运行 ETL 流程

### 方法 A: 通过 Airflow DAG (推荐)

1. **打开 Airflow UI**
   ```
   http://localhost:8080
   登录: admin / admin
   ```

2. **找到 DAG**
   - DAG ID: `financial_data_pipeline_scala`
   - 标签: `scala`, `spark`, `etl`, `financial`

3. **配置 Alpha Vantage API Key**
   ```bash
   # 编辑 .env 文件
   echo "ALPHA_VANTAGE_API_KEY=your_key_here" >> .env

   # 重启 Airflow
   docker-compose -f docker-compose-spark.yml restart airflow
   ```

   获取免费 API Key: https://www.alphavantage.co/support/#api-key

4. **手动触发 DAG**
   - 点击 DAG 名称
   - 点击右上角 "Trigger DAG" ▶️ 按钮
   - 等待执行完成 (~5-10分钟)

5. **查看执行结果**
   - 任务状态: 绿色 = 成功
   - 点击任务查看日志
   - 检查 Spark UI: http://localhost:8081

### 方法 B: 直接运行 Spark Job

```bash
# 本地模式
./scripts/build-and-submit.sh local

# 集群模式
./scripts/build-and-submit.sh cluster
```

### 方法 C: 手动 spark-submit

```bash
# 本地模式
spark-submit \
  --class com.financial.etl.transform.FinancialDataTransform \
  --master "local[*]" \
  --driver-memory 2g \
  target/scala-2.12/financial-etl-1.0.0.jar \
  --source-path ./data/raw \
  --target-path ./data/curated \
  --execution-date 2024-01-15

# Docker 集群模式
docker exec financial-spark-master \
  spark-submit \
  --class com.financial.etl.transform.FinancialDataTransform \
  --master "spark://spark-master:7077" \
  --driver-memory 1g \
  --executor-memory 2g \
  --total-executor-cores 4 \
  /jars/financial-etl-1.0.0.jar \
  --source-path /data/raw \
  --target-path /data/curated \
  --execution-date 2024-01-15
```

---

## 📊 步骤 4: 验证输出

### 检查 Parquet 文件

```bash
# 查看输出文件
find data/curated/processed -name "*.parquet"

# 输出示例:
# data/curated/processed/year=2024/month=1/part-00000.snappy.parquet
# data/curated/processed/year=2024/month=1/part-00001.snappy.parquet
```

### 查询 PostgreSQL

```bash
# 连接数据库
docker exec -it financial-postgres psql -U airflow -d financial_dw

# 查询数据
SELECT
  symbol,
  COUNT(*) as record_count,
  MIN(trade_date) as earliest_date,
  MAX(trade_date) as latest_date,
  AVG(close_price) as avg_close
FROM fact_stock_prices
GROUP BY symbol;

# 查看技术指标
SELECT
  symbol,
  trade_date,
  close_price,
  sma_5,
  sma_20,
  daily_return,
  volatility_20d
FROM fact_stock_prices
ORDER BY trade_date DESC
LIMIT 10;
```

### 使用 Pandas 读取 Parquet

```python
import pandas as pd

# 读取 Parquet
df = pd.read_parquet('data/curated/processed/')

print(f"Total records: {len(df)}")
print(f"Columns: {df.columns.tolist()}")
print(f"\nSample data:")
print(df.head())

# 技术指标统计
print(f"\nTechnical Indicators:")
print(df[['symbol', 'sma_5', 'sma_20', 'ema_12', 'volatility_20d']].describe())
```

---

## 🔍 代码详解

### Scala 核心转换逻辑

**文件**: `src/transformation/scala/FinancialDataTransform.scala`

#### 主要功能模块

```scala
// 1. 数据读取
def readRawData(spark: SparkSession, path: String): DataFrame = {
  spark.read
    .option("multiLine", "true")
    .json(path)
    .withColumn("source_file", input_file_name())
}

// 2. 数据清洗
def cleanseData(df: DataFrame): DataFrame = {
  df.filter(col("symbol").isNotNull && col("timestamp").isNotNull)
    .withColumn("symbol", upper(trim(col("symbol"))))
    .withColumn("volume", coalesce(col("volume"), lit(0L)))
    .dropDuplicates("symbol", "timestamp")
}

// 3. 技术指标计算 (SMA)
def calculateMovingAverages(df: DataFrame, periods: Seq[Int]): DataFrame = {
  var result = df
  periods.foreach { period =>
    val windowSpec = Window
      .partitionBy("symbol")
      .orderBy("trade_date")
      .rowsBetween(-period + 1, 0)

    result = result.withColumn(
      s"sma_$period",
      round(avg("close_price").over(windowSpec), 4)
    )
  }
  result
}

// 4. Parquet 写入
def writeToCurated(df: DataFrame, path: String, partitionCols: Seq[String]): Unit = {
  df.write
    .mode(SaveMode.Overwrite)
    .partitionBy(partitionCols: _*)
    .parquet(path)
}
```

#### 类型安全的好处

```scala
// ❌ PySpark - 运行时才报错
df.withColumn("typo_column", col("clse_price") * 2)  // 编译通过，运行报错

// ✅ Scala - 编译时检查
df.withColumn("new_column", col("close_price") * 2)  // 类型检查
```

---

## 🎓 技能展示要点

### 对比 PySpark

在面试中，你可以这样说明：

> "最初使用 PySpark 实现原型，但为了提升性能和类型安全，重构为纯 Scala + Spark。主要改进包括："
>
> 1. **类型安全**: 编译时捕获错误，减少运行时异常
> 2. **性能**: 原生 JVM 执行，无 Python ↔ JVM 序列化开销
> 3. **代码质量**: 函数式编程、模式匹配、不可变数据结构
> 4. **部署简化**: 单个 fat JAR，无 Python 环境依赖
> 5. **企业标准**: 大数据工程团队普遍使用 Scala

### 核心实现亮点

**1. 窗口函数优化**
```scala
// 使用 Window 函数计算移动平均
val windowSpec = Window
  .partitionBy("symbol")
  .orderBy("trade_date")
  .rowsBetween(-19, 0)  // 滚动 20 天窗口

df.withColumn("sma_20", avg("close_price").over(windowSpec))
```

**2. 分区写入策略**
```scala
// 按年月分区，优化查询性能
df.write
  .partitionBy("year", "month")
  .parquet(outputPath)

// 目录结构:
// curated/processed/year=2024/month=1/*.parquet
// curated/processed/year=2024/month=2/*.parquet
```

**3. Adaptive Query Execution**
```scala
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

// 自动优化:
// - 动态调整 shuffle 分区数
// - 合并小文件
// - 倾斜连接处理
```

---

## 🐛 常见问题

### 1. SBT 构建失败

**问题**: `java.lang.OutOfMemoryError: Java heap space`

**解决**:
```bash
# 增加 SBT 内存
export SBT_OPTS="-Xmx4G -XX:+UseG1GC"
sbt assembly
```

### 2. Spark 连接失败

**问题**: `Cannot connect to spark://spark-master:7077`

**检查**:
```bash
# 验证 Spark Master 是否运行
docker ps | grep spark-master

# 查看日志
docker logs financial-spark-master

# 重启服务
docker-compose -f docker-compose-spark.yml restart
```

### 3. JAR 文件找不到

**问题**: `FileNotFoundError: JAR file not found`

**解决**:
```bash
# 重新构建 JAR
sbt clean assembly

# 复制到 Docker volume
docker cp target/scala-2.12/financial-etl-1.0.0.jar \
  financial-spark-master:/jars/
```

### 4. Alpha Vantage API 限流

**问题**: `API rate limit exceeded (5 calls/min)`

**解决**:
```python
# 在 fetch_stock_data 中添加延迟
import time
for symbol in symbols:
    data = client.get_daily_adjusted(symbol)
    time.sleep(12)  # 等待 12 秒 = 5 次/分钟
```

---

## 📈 性能优化

### 本地开发优化

```scala
// build.sbt 优化
Test / fork := true
Test / javaOptions ++= Seq(
  "-Xmx2G",
  "-XX:+UseG1GC",
  "-XX:MaxMetaspaceSize=512m"
)
```

### Spark 配置优化

```bash
spark-submit \
  --conf spark.sql.shuffle.partitions=8 \        # 减少 shuffle 分区
  --conf spark.default.parallelism=4 \           # 并行度
  --conf spark.sql.files.maxPartitionBytes=128m \  # 文件分区大小
  --conf spark.sql.adaptive.enabled=true \       # 自适应执行
  ...
```

### Parquet 压缩优化

```scala
// 在 FinancialDataTransform.scala 中已配置
spark.conf.set("spark.sql.parquet.compression.codec", "snappy")

// 压缩率对比:
// JSON: 100 MB
// Parquet (Snappy): 30 MB (节省 70%)
// Parquet (Gzip): 20 MB (更慢，更小)
```

---

## 🚀 部署到 AWS

### 上传 JAR 到 S3

```bash
# 构建生产 JAR
sbt clean assembly

# 上传到 S3
aws s3 cp target/scala-2.12/financial-etl-1.0.0.jar \
  s3://your-bucket/glue-jars/
```

### 创建 AWS Glue Job

```bash
aws glue create-job \
  --name financial-data-transform \
  --role arn:aws:iam::123456789012:role/GlueServiceRole \
  --command '{
    "Name": "glueetl",
    "ScriptLocation": "s3://your-bucket/glue-jars/financial-etl-1.0.0.jar",
    "PythonVersion": "3"
  }' \
  --default-arguments '{
    "--job-language": "scala",
    "--class": "com.financial.etl.transform.FinancialDataTransform",
    "--source-path": "s3://your-bucket/raw/",
    "--target-path": "s3://your-bucket/curated/"
  }' \
  --glue-version "4.0" \
  --worker-type "G.1X" \
  --number-of-workers 2
```

### 从 Airflow 触发 Glue

```python
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator

glue_task = GlueJobOperator(
    task_id='run_scala_spark_glue',
    job_name='financial-data-transform',
    script_args={
        '--execution-date': '{{ ds }}'
    },
    aws_conn_id='aws_default'
)
```

---

## 📚 学习资源

### Scala + Spark

- **官方文档**: https://spark.apache.org/docs/latest/api/scala/
- **Scala 学习**: https://docs.scala-lang.org/tour/tour-of-scala.html
- **Spark By Examples**: https://sparkbyexamples.com/

### 推荐书籍

- *Learning Spark* (O'Reilly) - Spark 基础
- *High Performance Spark* - 性能优化
- *Scala for the Impatient* - Scala 快速入门

---

## ✅ 检查清单

### 本地开发环境

- [ ] Java 11+ 已安装
- [ ] SBT 1.9+ 已安装
- [ ] Docker + Docker Compose 已安装
- [ ] Alpha Vantage API Key 已获取
- [ ] 成功构建 JAR (`sbt assembly`)
- [ ] Docker 服务已启动
- [ ] 可以访问 Airflow UI (localhost:8080)
- [ ] 可以访问 Spark UI (localhost:8081)

### 功能验证

- [ ] Scala 代码编译通过
- [ ] 单元测试通过 (`sbt test`)
- [ ] 本地 Spark 运行成功
- [ ] Docker Spark 集群运行成功
- [ ] Airflow DAG 触发成功
- [ ] Parquet 文件已生成
- [ ] PostgreSQL 数据已加载
- [ ] 技术指标计算正确

### 求职准备

- [ ] 可以流畅演示整个流程 (< 5 分钟)
- [ ] 可以解释 Scala vs PySpark 优势
- [ ] 可以解释窗口函数实现
- [ ] 可以解释成本优化策略
- [ ] 准备好架构图和代码讲解
- [ ] GitHub README 已更新

---

## 🎯 总结

通过这次改造，项目现在展示了：

### 技术深度
✅ **Scala** - 函数式编程、类型安全
✅ **Spark** - 分布式计算、窗口函数
✅ **Parquet** - 列式存储优化
✅ **Docker** - 容器化部署
✅ **Airflow** - 工作流编排

### 成本意识
✅ 从 **$36K-135K/月** 降至 **$0/月** (本地)
✅ AWS 生产环境可优化至 **$500-800/月**

### 专业度
✅ 企业级代码质量
✅ 完整的错误处理
✅ 生产就绪的架构
✅ 详细的文档

**完美的求职作品集！** 🚀

---

## 📞 需要帮助？

遇到问题请检查:
1. Docker 日志: `docker logs financial-spark-master`
2. Airflow 日志: Airflow UI → DAG → Task → Logs
3. Spark UI: http://localhost:8081

祝你面试成功！🎉
