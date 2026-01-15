"""
Local Pandas ETL Transform (替代 AWS Glue PySpark)
=================================================
完全基于 Pandas/DuckDB 的数据转换，无需 Spark/Scala

特点:
- ✅ 零 Spark 依赖
- ✅ 零 JVM 依赖
- ✅ 零成本运行
- ✅ 保留所有技术指标计算
- ✅ 可在笔记本上运行

Usage:
    python local_pandas_transform.py \
        --source_path ./data/raw/2024-01-15 \
        --target_path ./data/curated/2024-01-15.parquet
"""

import os
import json
import argparse
from pathlib import Path
from datetime import datetime
from typing import List, Dict
import warnings
warnings.filterwarnings('ignore')

import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

# Optional: DuckDB for SQL-like operations (完全免费)
try:
    import duckdb
    DUCKDB_AVAILABLE = True
except ImportError:
    DUCKDB_AVAILABLE = False
    print("DuckDB not installed. Using Pandas only.")


# ==============================================================================
# Configuration
# ==============================================================================
class Config:
    """转换配置"""
    SMA_PERIODS = [5, 20, 50]
    EMA_PERIODS = [12, 26]
    VOLATILITY_WINDOW = 20
    PARQUET_COMPRESSION = 'snappy'
    CHUNK_SIZE = 10000  # 分块处理大文件


# ==============================================================================
# Data Reading Functions
# ==============================================================================
def read_json_files(source_path: str) -> pd.DataFrame:
    """
    从目录读取所有 JSON 文件

    Args:
        source_path: 包含 JSON 文件的目录路径

    Returns:
        合并后的 DataFrame
    """
    print(f"📖 Reading JSON files from: {source_path}")

    source_path = Path(source_path)
    json_files = list(source_path.glob('**/*.json'))

    if not json_files:
        raise FileNotFoundError(f"No JSON files found in {source_path}")

    print(f"Found {len(json_files)} JSON files")

    dfs = []
    for json_file in json_files:
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)

            # 处理嵌套结构
            if isinstance(data, dict) and 'data' in data:
                df = pd.DataFrame(data['data'])
            elif isinstance(data, list):
                df = pd.DataFrame(data)
            else:
                df = pd.DataFrame([data])

            df['source_file'] = str(json_file)
            dfs.append(df)

        except Exception as e:
            print(f"⚠️  Error reading {json_file}: {e}")
            continue

    if not dfs:
        raise ValueError("No valid data found in JSON files")

    combined_df = pd.concat(dfs, ignore_index=True)
    print(f"✅ Loaded {len(combined_df)} records from {len(json_files)} files")

    return combined_df


def read_from_s3_to_local(s3_path: str, local_path: str):
    """
    可选: 从 S3 下载到本地处理

    Args:
        s3_path: S3 URI (s3://bucket/prefix/)
        local_path: 本地目标路径
    """
    try:
        import boto3
        s3 = boto3.client('s3')

        # 解析 S3 URI
        bucket = s3_path.replace('s3://', '').split('/')[0]
        prefix = '/'.join(s3_path.replace('s3://', '').split('/')[1:])

        # 列出对象
        paginator = s3.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get('Contents', []):
                key = obj['Key']
                if key.endswith('.json'):
                    local_file = Path(local_path) / Path(key).name
                    local_file.parent.mkdir(parents=True, exist_ok=True)
                    s3.download_file(bucket, key, str(local_file))
                    print(f"Downloaded: {key}")

    except ImportError:
        print("boto3 not installed. Cannot download from S3.")
    except Exception as e:
        print(f"Error downloading from S3: {e}")


# ==============================================================================
# Data Cleansing Functions (替代 PySpark cleanse_data)
# ==============================================================================
def cleanse_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    数据清洗 (纯 Pandas 实现)

    Rules:
    1. 移除空值
    2. 标准化符号格式
    3. 处理负价格
    4. 填充缺失值
    """
    print("🧹 Applying data cleansing rules...")

    initial_count = len(df)

    # 1. 移除必需字段的空值
    df = df.dropna(subset=['symbol', 'timestamp'])

    # 2. 标准化 symbol (大写)
    df['symbol'] = df['symbol'].astype(str).str.upper().str.strip()

    # 3. 处理价格列
    price_columns = ['open_price', 'high_price', 'low_price', 'close_price', 'adjusted_close']
    for col in price_columns:
        if col in df.columns:
            # 替换负值为 NaN
            df[col] = pd.to_numeric(df[col], errors='coerce')
            df[col] = df[col].where(df[col] >= 0, np.nan)

    # 4. 填充缺失值
    df['volume'] = pd.to_numeric(df.get('volume', 0), errors='coerce').fillna(0)
    df['dividend_amount'] = pd.to_numeric(df.get('dividend_amount', 0), errors='coerce').fillna(0.0)
    df['split_coefficient'] = pd.to_numeric(df.get('split_coefficient', 1.0), errors='coerce').fillna(1.0)

    # 5. 验证 OHLC 完整性: high >= low
    df = df[
        (df['high_price'].isna()) |
        (df['low_price'].isna()) |
        (df['high_price'] >= df['low_price'])
    ]

    # 6. 移除重复记录 (symbol + timestamp)
    df = df.drop_duplicates(subset=['symbol', 'timestamp'], keep='first')

    final_count = len(df)
    removed = initial_count - final_count

    print(f"✅ Cleansed: {initial_count} → {final_count} records ({removed} removed)")

    return df


def standardize_dates(df: pd.DataFrame) -> pd.DataFrame:
    """
    标准化日期格式 (替代 PySpark to_date/to_timestamp)
    """
    print("📅 Standardizing dates...")

    # 转换时间戳
    df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
    df['trade_date'] = df['timestamp'].dt.date
    df['trade_timestamp'] = df['timestamp']

    # 移除无效日期
    df = df.dropna(subset=['trade_date'])

    return df


# ==============================================================================
# Technical Indicator Functions (纯 Pandas 实现)
# ==============================================================================
def calculate_derived_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """
    计算衍生指标 (替代 PySpark Window 函数)

    使用 groupby + shift 实现
    """
    print("📊 Calculating derived metrics...")

    # 确保按 symbol 和 date 排序
    df = df.sort_values(['symbol', 'trade_date']).reset_index(drop=True)

    # 1. 日内波动
    df['daily_range'] = df['high_price'] - df['low_price']
    df['daily_range_pct'] = (df['daily_range'] / df['close_price']) * 100

    # 2. 日收益率 (close-to-close)
    df['prev_close'] = df.groupby('symbol')['close_price'].shift(1)
    df['daily_return'] = ((df['close_price'] - df['prev_close']) / df['prev_close']) * 100

    # 3. 成交量变化
    df['prev_volume'] = df.groupby('symbol')['volume'].shift(1)
    df['volume_change_pct'] = ((df['volume'] - df['prev_volume']) / df['prev_volume']) * 100

    # 清理临时列
    df = df.drop(columns=['prev_close', 'prev_volume'], errors='ignore')

    return df


def calculate_moving_averages(df: pd.DataFrame, periods: List[int] = None) -> pd.DataFrame:
    """
    计算简单移动平均 (SMA)

    使用 pandas rolling 实现
    """
    print(f"📈 Calculating SMA for periods: {periods or Config.SMA_PERIODS}...")

    periods = periods or Config.SMA_PERIODS

    for period in periods:
        df[f'sma_{period}'] = (
            df.groupby('symbol')['close_price']
            .transform(lambda x: x.rolling(window=period, min_periods=1).mean())
            .round(4)
        )

    return df


def calculate_ema(df: pd.DataFrame, periods: List[int] = None) -> pd.DataFrame:
    """
    计算指数移动平均 (EMA)

    使用 pandas ewm 实现 (比 PySpark 更精确)
    """
    print(f"📉 Calculating EMA for periods: {periods or Config.EMA_PERIODS}...")

    periods = periods or Config.EMA_PERIODS

    for period in periods:
        df[f'ema_{period}'] = (
            df.groupby('symbol')['close_price']
            .transform(lambda x: x.ewm(span=period, adjust=False).mean())
            .round(4)
        )

    return df


def calculate_volatility(df: pd.DataFrame, window: int = None) -> pd.DataFrame:
    """
    计算滚动波动率 (标准差)

    使用 pandas rolling std
    """
    window = window or Config.VOLATILITY_WINDOW
    print(f"📊 Calculating {window}d volatility...")

    df['volatility_20d'] = (
        df.groupby('symbol')['daily_return']
        .transform(lambda x: x.rolling(window=window, min_periods=1).std())
        .round(4)
    )

    return df


def add_technical_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """
    添加所有技术指标
    """
    df = calculate_derived_metrics(df)
    df = calculate_moving_averages(df)
    df = calculate_ema(df)
    df = calculate_volatility(df)

    return df


# ==============================================================================
# Dimensional Modeling (替代 PySpark date functions)
# ==============================================================================
def add_date_dimensions(df: pd.DataFrame) -> pd.DataFrame:
    """
    添加日期维度列
    """
    print("📅 Adding date dimensions...")

    df['year'] = pd.to_datetime(df['trade_date']).dt.year
    df['quarter'] = pd.to_datetime(df['trade_date']).dt.quarter
    df['month'] = pd.to_datetime(df['trade_date']).dt.month
    df['day'] = pd.to_datetime(df['trade_date']).dt.day
    df['day_of_week'] = pd.to_datetime(df['trade_date']).dt.dayofweek + 1  # 1=Monday

    return df


def add_metadata(df: pd.DataFrame) -> pd.DataFrame:
    """
    添加元数据
    """
    df['processing_timestamp'] = pd.Timestamp.now()
    df['price_id'] = (
        df['symbol'] + '_' +
        pd.to_datetime(df['trade_date']).dt.strftime('%Y%m%d')
    )

    return df


def select_final_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    选择最终列
    """
    final_columns = [
        'price_id',
        'symbol',
        'trade_date',
        'trade_timestamp',
        'open_price',
        'high_price',
        'low_price',
        'close_price',
        'adjusted_close',
        'volume',
        'dividend_amount',
        'split_coefficient',
        'daily_return',
        'daily_range',
        'daily_range_pct',
        'volume_change_pct',
        'sma_5',
        'sma_20',
        'sma_50',
        'ema_12',
        'ema_26',
        'volatility_20d',
        'year',
        'quarter',
        'month',
        'day',
        'day_of_week',
        'processing_timestamp',
    ]

    # 只选择存在的列
    available_columns = [c for c in final_columns if c in df.columns]
    return df[available_columns]


# ==============================================================================
# Data Writing Functions (Parquet)
# ==============================================================================
def write_to_parquet(
    df: pd.DataFrame,
    output_path: str,
    partition_cols: List[str] = None,
    compression: str = 'snappy'
):
    """
    写入 Parquet 文件 (替代 Spark write.parquet)

    使用 PyArrow 实现高效压缩
    """
    print(f"💾 Writing to Parquet: {output_path}")

    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # 确保日期列是正确类型
    if 'trade_date' in df.columns:
        df['trade_date'] = pd.to_datetime(df['trade_date'])

    if partition_cols:
        # 分区写入 (模拟 Spark partitionBy)
        print(f"Partitioning by: {partition_cols}")

        for partition_values, group_df in df.groupby(partition_cols):
            if not isinstance(partition_values, tuple):
                partition_values = (partition_values,)

            # 构建分区路径
            partition_path = output_path
            for col, val in zip(partition_cols, partition_values):
                partition_path = partition_path / f"{col}={val}"

            partition_path.mkdir(parents=True, exist_ok=True)
            file_path = partition_path / "data.parquet"

            # 写入分区文件
            group_df.to_parquet(
                file_path,
                compression=compression,
                index=False,
                engine='pyarrow'
            )

        print(f"✅ Written {len(df)} records to {output_path} (partitioned)")

    else:
        # 单文件写入
        df.to_parquet(
            output_path,
            compression=compression,
            index=False,
            engine='pyarrow'
        )

        print(f"✅ Written {len(df)} records to {output_path}")

    # 显示文件大小
    if output_path.is_file():
        size_mb = output_path.stat().st_size / (1024 * 1024)
        print(f"📦 File size: {size_mb:.2f} MB")


def write_to_csv(df: pd.DataFrame, output_path: str):
    """
    可选: 写入 CSV (便于调试)
    """
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    df.to_csv(output_path, index=False)
    print(f"✅ Written CSV to {output_path}")


# ==============================================================================
# DuckDB Query Examples (可选)
# ==============================================================================
def query_with_duckdb(parquet_path: str, query: str = None):
    """
    使用 DuckDB 直接查询 Parquet 文件

    DuckDB 是零配置的分析数据库，完全免费
    """
    if not DUCKDB_AVAILABLE:
        print("DuckDB not available")
        return None

    conn = duckdb.connect(':memory:')

    default_query = f"""
    SELECT
        symbol,
        DATE_TRUNC('day', trade_date) as date,
        AVG(close_price) as avg_close,
        SUM(volume) as total_volume,
        COUNT(*) as record_count
    FROM read_parquet('{parquet_path}')
    GROUP BY symbol, date
    ORDER BY date DESC
    LIMIT 10
    """

    query = query or default_query

    result = conn.execute(query).df()
    print("\n📊 Query Result:")
    print(result)

    return result


# ==============================================================================
# Main ETL Pipeline
# ==============================================================================
def run_transformation(
    source_path: str,
    target_path: str,
    partition_by: List[str] = None,
    write_csv: bool = False
):
    """
    执行完整 ETL 流程 (纯 Pandas，零 Spark 依赖)
    """
    print("=" * 70)
    print("🚀 STARTING PANDAS-BASED FINANCIAL DATA TRANSFORMATION")
    print("=" * 70)
    print(f"Source: {source_path}")
    print(f"Target: {target_path}")
    print(f"Partition by: {partition_by or 'None'}")
    print("=" * 70)

    start_time = datetime.now()

    try:
        # Step 1: 读取原始数据
        raw_df = read_json_files(source_path)

        # Step 2: 数据清洗
        cleansed_df = cleanse_data(raw_df)

        # Step 3: 标准化日期
        dated_df = standardize_dates(cleansed_df)

        # Step 4: 添加技术指标
        enriched_df = add_technical_indicators(dated_df)

        # Step 5: 添加日期维度
        dimensional_df = add_date_dimensions(enriched_df)

        # Step 6: 添加元数据
        final_df = add_metadata(dimensional_df)

        # Step 7: 选择最终列
        output_df = select_final_columns(final_df)

        # 显示统计信息
        print("\n" + "=" * 70)
        print("📊 TRANSFORMATION SUMMARY")
        print("=" * 70)
        print(f"Total records: {len(output_df)}")
        print(f"Symbols: {output_df['symbol'].nunique()}")
        print(f"Date range: {output_df['trade_date'].min()} to {output_df['trade_date'].max()}")
        print("\nSample data:")
        print(output_df.head(3).to_string())

        # Step 8: 写入 Parquet
        write_to_parquet(
            output_df,
            target_path,
            partition_cols=partition_by,
            compression=Config.PARQUET_COMPRESSION
        )

        # Step 9: 可选写入 CSV
        if write_csv:
            csv_path = Path(target_path).with_suffix('.csv')
            write_to_csv(output_df, str(csv_path))

        # 执行时间
        elapsed = datetime.now() - start_time
        print("\n" + "=" * 70)
        print(f"✅ TRANSFORMATION COMPLETE in {elapsed.total_seconds():.2f} seconds")
        print("=" * 70)

        # 可选: DuckDB 查询示例
        if DUCKDB_AVAILABLE and Path(target_path).is_file():
            print("\n🔍 Running sample DuckDB query...")
            query_with_duckdb(target_path)

        return output_df

    except Exception as e:
        print(f"\n❌ ERROR: Transformation failed - {e}")
        import traceback
        traceback.print_exc()
        raise


# ==============================================================================
# CLI Entry Point
# ==============================================================================
def main():
    """
    命令行入口
    """
    parser = argparse.ArgumentParser(
        description='Local Pandas ETL Transform (Zero Spark Dependencies)'
    )

    parser.add_argument(
        '--source_path',
        type=str,
        required=True,
        help='Path to source JSON files (local directory or S3 URI)'
    )

    parser.add_argument(
        '--target_path',
        type=str,
        required=True,
        help='Output Parquet file path'
    )

    parser.add_argument(
        '--partition_by',
        type=str,
        nargs='+',
        default=None,
        help='Partition columns (e.g., year month)'
    )

    parser.add_argument(
        '--write_csv',
        action='store_true',
        help='Also write CSV output for debugging'
    )

    args = parser.parse_args()

    run_transformation(
        source_path=args.source_path,
        target_path=args.target_path,
        partition_by=args.partition_by,
        write_csv=args.write_csv
    )


if __name__ == "__main__":
    main()
