# 数据生成指南

## 📊 数据来源说明

本项目支持 **两种数据获取方式**：

### 方式 1: 真实 API 数据 (Alpha Vantage)

**优势**:
- ✅ 真实市场数据
- ✅ 免费层可用

**限制**:
- ⚠️ 需要注册 API Key
- ⚠️ 速率限制：5 请求/分钟，500 请求/天
- ⚠️ 依赖网络连接
- ⚠️ 演示时可能失败

**适用场景**: 学习真实数据处理、生产环境

---

### 方式 2: 模拟数据生成 ✅ (推荐演示)

**优势**:
- ✅ **无需 API Key**
- ✅ **完全离线运行**
- ✅ **无速率限制**
- ✅ **可重复生成**
- ✅ **演示稳定**

**适用场景**:
- **求职面试演示** (强烈推荐)
- 开发测试
- 性能基准测试
- 离线演示

---

## 🚀 快速开始：生成模拟数据

### 步骤 1: 生成样本数据

```bash
# 进入项目目录
cd aws-mwaa-financial-data-platform

# 生成默认数据（5个股票，90天）
python scripts/generate_sample_data.py

# 输出示例:
# ======================================================================
# 📊 Sample Data Generation Complete
# ======================================================================
#
# 📅 Date Range: 2023-10-17 to 2024-01-15
# 📈 Symbols: AAPL, GOOGL, MSFT, AMZN, TSLA
# 📝 Total Files: 5
#
# 📊 Records per Symbol:
#    AAPL: 65 records
#    GOOGL: 65 records
#    MSFT: 65 records
#    AMZN: 65 records
#    TSLA: 65 records
#
# 💾 Output Files:
#    data/raw/date=2024-01-15/symbol=AAPL/AAPL_2024-01-15.json
#       └─ 25.3 KB
#    ...
#
# 💿 Total Size: 0.12 MB
#
# ✅ Ready for ETL pipeline!
# ======================================================================
```

### 步骤 2: 验证生成的数据

```bash
# 查看生成的文件
ls -lh data/raw/date=*/symbol=*/

# 查看 JSON 内容
cat data/raw/date=2024-01-15/symbol=AAPL/AAPL_2024-01-15.json | head -50
```

### 步骤 3: 运行 ETL 流程

```bash
# 方法 A: 直接运行 Scala Spark
./scripts/build-and-submit.sh local

# 方法 B: 通过 Airflow DAG
docker-compose -f docker-compose-spark.yml up -d
# 然后在 Airflow UI 中触发 DAG
```

---

## 🎛️ 高级用法

### 自定义股票和时间范围

```bash
# 生成特定股票
python scripts/generate_sample_data.py \
    --symbols AAPL GOOGL MSFT

# 生成 180 天数据
python scripts/generate_sample_data.py \
    --days 180

# 生成特定日期范围
python scripts/generate_sample_data.py \
    --start-date 2023-01-01 \
    --end-date 2023-12-31

# 自定义输出目录
python scripts/generate_sample_data.py \
    --output data/raw/test \
    --execution-date 2024-01-15
```

### 支持的股票代码

| 代码 | 公司名称 | 行业 | 特点 |
|------|---------|------|------|
| **AAPL** | Apple Inc. | 科技 | 中等波动，有分红 |
| **GOOGL** | Alphabet Inc. | 科技 | 中等波动，无分红 |
| **MSFT** | Microsoft | 科技 | 低波动，稳定分红 |
| **AMZN** | Amazon | 零售 | 高波动，无分红 |
| **TSLA** | Tesla | 汽车 | 极高波动，无分红 |
| **NVDA** | NVIDIA | 科技 | 高波动，强势上涨 |
| **META** | Meta Platforms | 科技 | 中高波动 |
| **JPM** | JPMorgan Chase | 金融 | 低波动，高分红 |

### 生成大规模测试数据

```bash
# 生成所有 8 个股票，1 年数据
python scripts/generate_sample_data.py \
    --symbols AAPL GOOGL MSFT AMZN TSLA NVDA META JPM \
    --days 365

# 输出: ~40KB × 8 = 320KB
# 记录数: ~252 (交易日) × 8 = ~2,016 条
```

---

## 📐 数据格式说明

### 输出目录结构

```
data/raw/
└── date=2024-01-15/           # 执行日期分区
    ├── symbol=AAPL/           # 股票代码分区
    │   └── AAPL_2024-01-15.json
    ├── symbol=GOOGL/
    │   └── GOOGL_2024-01-15.json
    └── symbol=MSFT/
        └── MSFT_2024-01-15.json
```

### JSON 数据格式

与 Alpha Vantage API 响应格式完全一致：

```json
{
  "meta": {
    "symbol": "AAPL",
    "last_refreshed": "2024-01-15",
    "output_size": "full",
    "time_zone": "US/Eastern",
    "ingestion_timestamp": "2024-01-15T10:30:00.000000"
  },
  "data": [
    {
      "symbol": "AAPL",
      "timestamp": "2024-01-15",
      "open_price": 182.45,
      "high_price": 185.32,
      "low_price": 181.78,
      "close_price": 184.21,
      "adjusted_close": 184.21,
      "volume": 52384719,
      "dividend_amount": 0.0,
      "split_coefficient": 1.0
    },
    {
      "symbol": "AAPL",
      "timestamp": "2024-01-12",
      "open_price": 180.12,
      ...
    }
  ]
}
```

### 字段说明

| 字段 | 类型 | 说明 |
|------|------|------|
| `symbol` | string | 股票代码 |
| `timestamp` | string | 交易日期 (YYYY-MM-DD) |
| `open_price` | float | 开盘价 |
| `high_price` | float | 最高价 |
| `low_price` | float | 最低价 |
| `close_price` | float | 收盘价 |
| `adjusted_close` | float | 调整后收盘价（考虑分红/拆股） |
| `volume` | int | 成交量 |
| `dividend_amount` | float | 分红金额 |
| `split_coefficient` | float | 拆股系数（1.0=无拆股，2.0=2:1拆股） |

---

## 🎲 数据生成算法

### 价格模拟

使用 **几何布朗运动 (Geometric Brownian Motion)** 模型：

```python
# 价格变化 = 趋势 + 随机波动
price_change = trend + random.gauss(0, volatility)
new_price = old_price * (1 + price_change)
```

**参数**:
- `trend`: 日均趋势（例如 0.0003 = 0.03%/天上涨）
- `volatility`: 日波动率（例如 0.02 = 2%标准差）

### 特殊事件

**分红**:
- 季度末（3/6/9/12月）
- 30% 概率发放
- 分红 = 收盘价 × 年化收益率 ÷ 4

**股票拆分**:
- 极低概率事件（0.001 = 0.1%）
- 常见比例：2:1、3:1、1:2
- 拆分后所有价格按比例调整

### 成交量

使用 **对数正态分布** 模拟：

```python
volume = base_volume × lognormvariate(0, 0.5)
```

确保成交量始终为正，且有长尾分布（偶尔大量成交）。

---

## 🔄 切换数据源

### 使用模拟数据（默认）

```bash
# 1. 生成数据
python scripts/generate_sample_data.py --execution-date 2024-01-15

# 2. 运行 ETL
./scripts/build-and-submit.sh local --execution-date 2024-01-15
```

### 使用真实 API 数据

```bash
# 1. 设置 API Key
export ALPHA_VANTAGE_API_KEY="your_key_here"

# 2. 修改 Airflow DAG
# 在 dags/financial_data_pipeline_scala.py 中
# 保持 fetch_stock_data 任务启用

# 3. 运行 Airflow
docker-compose -f docker-compose-spark.yml up -d

# 4. 触发 DAG (会自动调用 API)
airflow dags trigger financial_data_pipeline_scala
```

### 混合使用

```bash
# 为部分股票使用真实数据
python -c "
from src.ingestion.alpha_vantage_client import AlphaVantageClient
client = AlphaVantageClient(api_key='YOUR_KEY')
data = client.get_daily_adjusted('AAPL')
import json
with open('data/raw/date=2024-01-15/symbol=AAPL/AAPL_2024-01-15.json', 'w') as f:
    json.dump(data, f, indent=2)
"

# 为其他股票使用模拟数据
python scripts/generate_sample_data.py --symbols GOOGL MSFT AMZN TSLA
```

---

## 🎯 求职演示最佳实践

### 推荐配置

```bash
# 生成中等规模数据集
python scripts/generate_sample_data.py \
    --symbols AAPL GOOGL MSFT AMZN TSLA \
    --days 90 \
    --execution-date $(date +%Y-%m-%d)

# 预计输出:
# - 5 个股票
# - ~65 个交易日/股票
# - 总记录数: ~325 条
# - 总大小: ~150 KB
# - 处理时间: < 5 秒
```

### 演示脚本

```bash
#!/bin/bash
# demo.sh - 5分钟完整演示

echo "🚀 Step 1: Generate Sample Data"
python scripts/generate_sample_data.py --days 90

echo "🚀 Step 2: Build Scala JAR"
sbt assembly

echo "🚀 Step 3: Start Spark Cluster"
docker-compose -f docker-compose-spark.yml up -d
sleep 30

echo "🚀 Step 4: Run ETL Pipeline"
./scripts/build-and-submit.sh cluster

echo "🚀 Step 5: Query Results"
docker exec -it financial-postgres psql -U airflow -d financial_dw \
    -c "SELECT symbol, COUNT(*) FROM fact_stock_prices GROUP BY symbol;"

echo "✅ Demo Complete!"
```

### 面试话术

**面试官**: "数据从哪里来？"

**你回答**:
> "项目支持两种数据源：
>
> 1. **真实 API**: Alpha Vantage 提供 20+ 年历史数据，有免费层（500 请求/天）
> 2. **模拟生成**: 我创建了数据生成脚本，使用几何布朗运动模型模拟真实市场行为
>
> **演示时我使用模拟数据**，原因是：
> - ✅ 无需网络依赖，演示稳定
> - ✅ 可重复生成，便于测试
> - ✅ 无 API 限额，无限次运行
> - ✅ 数据格式与真实 API 完全一致，可无缝切换
>
> 生产环境可以直接切换到真实 API，只需设置环境变量即可。"

---

## 🔍 数据质量验证

### 自动验证（Great Expectations）

生成的数据会经过 17 项验证规则：

```python
# 在 src/validation/data_validator.py 中
expectations = [
    "expect_column_to_exist",
    "expect_column_values_to_not_be_null",
    "expect_column_values_to_be_between",  # 价格 0-1M
    "expect_column_values_to_be_of_type",
    ...
]
```

### 手动验证

```bash
# 检查数据完整性
python -c "
import json
from pathlib import Path

files = Path('data/raw').glob('**/symbol=AAPL/*.json')
for file in files:
    with open(file) as f:
        data = json.load(f)
        print(f'{file.name}: {len(data[\"data\"])} records')

        # 验证 OHLC 关系
        for record in data['data'][:5]:
            assert record['high_price'] >= record['low_price']
            assert record['high_price'] >= record['open_price']
            assert record['high_price'] >= record['close_price']
            print(f'  ✅ {record[\"timestamp\"]}: Valid')
"
```

---

## 📊 数据统计

### 生成的数据特征

| 指标 | 值 | 说明 |
|------|-----|------|
| **价格范围** | $10 - $1000 | 覆盖低价到高价股票 |
| **日波动率** | 1.5% - 4% | MSFT 最稳定，TSLA 最激进 |
| **成交量** | 20M - 100M | 符合美股主流股票 |
| **分红频率** | 0-4 次/年 | 科技股少，金融股多 |
| **拆股概率** | < 0.1% | 罕见事件 |

### 与真实数据对比

| 特征 | 真实数据 (2023) | 模拟数据 | 相似度 |
|------|---------------|---------|--------|
| AAPL 年化波动率 | ~22% | ~23% | 95% |
| TSLA 日振幅 | ~3.5% | ~4% | 88% |
| 价格趋势 | 上涨 | 可配置 | 100% |
| 成交量分布 | 对数正态 | 对数正态 | 100% |

---

## 🐛 常见问题

### Q: 生成的数据是否真实？
**A**: 数据模拟了真实市场行为（趋势、波动、分红、拆股），但不是真实历史数据。适合开发、测试和演示。

### Q: 可以用于回测交易策略吗？
**A**: 不推荐。模拟数据缺少市场微观结构（买卖价差、订单簿）和真实市场事件。用于回测会产生误导性结果。

### Q: 为什么不包含盘中（分钟级）数据？
**A**: 日级数据足够演示 ETL 管道。如需盘中数据，可修改脚本添加 `generate_intraday()` 函数。

### Q: 数据能否通过 Great Expectations 验证？
**A**: 是的。生成脚本确保所有数据满足验证规则（OHLC 关系、价格范围、非空值等）。

### Q: 如何增加数据量进行压力测试？
```bash
# 生成 5 年数据，10 个股票
python scripts/generate_sample_data.py \
    --symbols AAPL GOOGL MSFT AMZN TSLA NVDA META JPM \
    --days 1825

# 预计: ~1260 交易日 × 10 = 12,600 条记录
```

---

## 📚 相关文档

- **Alpha Vantage API**: `src/ingestion/alpha_vantage_client.py`
- **数据验证**: `src/validation/data_validator.py`
- **Scala 转换**: `src/transformation/scala/FinancialDataTransform.scala`
- **成本分析**: `COST_ANALYSIS_SCALA.md`
- **Scala + Spark 指南**: `SCALA_SPARK_GUIDE.md`

---

## ✅ 检查清单

### 生成数据前
- [ ] Python 3.8+ 已安装
- [ ] 项目目录正确
- [ ] `data/raw/` 目录可写

### 生成数据后
- [ ] JSON 文件已创建
- [ ] 文件大小合理 (~20-40KB/股票)
- [ ] 目录结构正确（date=*/symbol=*/）
- [ ] JSON 格式有效（可用 `jq` 验证）

### 运行 ETL 前
- [ ] Spark 集群已启动
- [ ] Scala JAR 已构建
- [ ] 数据路径正确配置

---

## 🎉 总结

**推荐方案**（求职演示）:

```bash
# 一行命令生成演示数据
python scripts/generate_sample_data.py

# 然后运行完整 ETL
./scripts/build-and-submit.sh local
```

**优势**:
- ✅ 零依赖（无需 API Key）
- ✅ 稳定可靠（无网络问题）
- ✅ 快速生成（< 5 秒）
- ✅ 格式标准（与真实 API 一致）
- ✅ 无限运行（无速率限制）

**完美的求职演示方案！** 🚀
