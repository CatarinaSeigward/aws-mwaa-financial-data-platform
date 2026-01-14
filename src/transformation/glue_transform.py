"""
AWS Glue ETL Transform Script
=============================
PySpark-based data transformation for financial market data.
Converts raw JSON to optimized Parquet with dimensional modeling.

This script is executed by AWS Glue and performs:
1. Read raw JSON from S3 raw layer
2. Data cleansing and standardization
3. Technical indicator calculations
4. Dimensional modeling (OHLC facts)
5. Write Parquet to S3 curated layer

Usage:
    Triggered by AWS Glue Job with parameters:
    --source_bucket: S3 bucket containing raw data
    --target_bucket: S3 bucket for curated output
    --execution_date: Processing date (YYYY-MM-DD)
"""

import sys
from datetime import datetime
from typing import List

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

from pyspark.context import SparkContext
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, lit, when, coalesce, to_date, to_timestamp,
    year, month, dayofmonth, dayofweek, quarter,
    lag, lead, avg, stddev, sum as spark_sum,
    row_number, dense_rank, percent_rank,
    round as spark_round, abs as spark_abs,
    current_timestamp, date_format, concat_ws,
    explode, from_json, schema_of_json, get_json_object,
)
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    LongType, DateType, TimestampType, IntegerType,
)


# ==============================================================================
# Initialize Glue Context
# ==============================================================================
args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "source_bucket", "target_bucket", "execution_date"]
)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Configuration
SOURCE_BUCKET = args["source_bucket"]
TARGET_BUCKET = args["target_bucket"]
EXECUTION_DATE = args["execution_date"]

print(f"Starting transformation job")
print(f"Source: s3://{SOURCE_BUCKET}")
print(f"Target: s3://{TARGET_BUCKET}")
print(f"Date: {EXECUTION_DATE}")


# ==============================================================================
# Schema Definitions
# ==============================================================================
RAW_SCHEMA = StructType([
    StructField("symbol", StringType(), False),
    StructField("timestamp", StringType(), False),
    StructField("open_price", DoubleType(), True),
    StructField("high_price", DoubleType(), True),
    StructField("low_price", DoubleType(), True),
    StructField("close_price", DoubleType(), True),
    StructField("adjusted_close", DoubleType(), True),
    StructField("volume", LongType(), True),
    StructField("dividend_amount", DoubleType(), True),
    StructField("split_coefficient", DoubleType(), True),
])

FACT_SCHEMA = StructType([
    StructField("price_id", StringType(), False),
    StructField("symbol", StringType(), False),
    StructField("trade_date", DateType(), False),
    StructField("trade_timestamp", TimestampType(), True),
    StructField("open_price", DoubleType(), True),
    StructField("high_price", DoubleType(), True),
    StructField("low_price", DoubleType(), True),
    StructField("close_price", DoubleType(), True),
    StructField("adjusted_close", DoubleType(), True),
    StructField("volume", LongType(), True),
    StructField("dividend_amount", DoubleType(), True),
    StructField("split_coefficient", DoubleType(), True),
    # Derived metrics
    StructField("daily_return", DoubleType(), True),
    StructField("daily_range", DoubleType(), True),
    StructField("daily_range_pct", DoubleType(), True),
    StructField("volume_change_pct", DoubleType(), True),
    # Technical indicators
    StructField("sma_5", DoubleType(), True),
    StructField("sma_20", DoubleType(), True),
    StructField("sma_50", DoubleType(), True),
    StructField("ema_12", DoubleType(), True),
    StructField("ema_26", DoubleType(), True),
    StructField("volatility_20d", DoubleType(), True),
    # Date dimensions
    StructField("year", IntegerType(), True),
    StructField("quarter", IntegerType(), True),
    StructField("month", IntegerType(), True),
    StructField("day", IntegerType(), True),
    StructField("day_of_week", IntegerType(), True),
    # Metadata
    StructField("processing_timestamp", TimestampType(), True),
    StructField("source_file", StringType(), True),
])


# ==============================================================================
# Data Reading Functions
# ==============================================================================
def read_raw_data(source_path: str) -> DataFrame:
    """
    Read raw JSON data from S3 and flatten to DataFrame.
    
    Args:
        source_path: S3 path to raw data (s3://bucket/prefix/)
        
    Returns:
        Flattened DataFrame with raw stock data
    """
    print(f"Reading raw data from: {source_path}")
    
    # Read JSON files
    raw_df = spark.read.option("multiLine", "true").json(source_path)
    
    # Handle nested structure (our standardized format has "data" array)
    if "data" in raw_df.columns:
        # Explode the data array
        from pyspark.sql.functions import explode
        raw_df = raw_df.select(
            explode("data").alias("record"),
            lit(source_path).alias("source_file")
        ).select(
            col("record.*"),
            col("source_file")
        )
    
    print(f"Loaded {raw_df.count()} raw records")
    return raw_df


def read_partitioned_data(bucket: str, prefix: str, date: str) -> DataFrame:
    """
    Read data from date-partitioned S3 structure.
    
    Args:
        bucket: S3 bucket name
        prefix: Path prefix
        date: Partition date (YYYY-MM-DD)
        
    Returns:
        DataFrame with data from specified partition
    """
    partition_path = f"s3://{bucket}/{prefix}/date={date}/"
    
    print(f"Reading from partition: {partition_path}")
    
    try:
        # List all symbol partitions
        df = spark.read.option("multiLine", "true").json(f"{partition_path}*/")
        
        if "data" in df.columns:
            df = df.select(
                explode("data").alias("record"),
                lit(partition_path).alias("source_file")
            ).select(
                col("record.*"),
                col("source_file")
            )
        
        return df
        
    except Exception as e:
        print(f"Error reading partition: {e}")
        # Try reading individual files
        return spark.read.option("multiLine", "true").json(partition_path)


# ==============================================================================
# Data Cleansing Functions
# ==============================================================================
def cleanse_data(df: DataFrame) -> DataFrame:
    """
    Apply data cleansing rules.
    
    Rules:
    1. Remove null symbols and timestamps
    2. Standardize symbol format (uppercase)
    3. Handle negative prices
    4. Cap extreme values
    5. Fill missing volumes with 0
    """
    print("Applying data cleansing rules")
    
    cleansed_df = df.filter(
        col("symbol").isNotNull() & 
        col("timestamp").isNotNull()
    ).withColumn(
        "symbol", 
        col("symbol").cast("string")
    ).withColumn(
        # Ensure prices are non-negative
        "open_price",
        when(col("open_price") < 0, None).otherwise(col("open_price"))
    ).withColumn(
        "high_price",
        when(col("high_price") < 0, None).otherwise(col("high_price"))
    ).withColumn(
        "low_price",
        when(col("low_price") < 0, None).otherwise(col("low_price"))
    ).withColumn(
        "close_price",
        when(col("close_price") < 0, None).otherwise(col("close_price"))
    ).withColumn(
        "adjusted_close",
        when(col("adjusted_close") < 0, None).otherwise(col("adjusted_close"))
    ).withColumn(
        # Fill null volumes
        "volume",
        coalesce(col("volume"), lit(0))
    ).withColumn(
        # Fill null dividend amounts
        "dividend_amount",
        coalesce(col("dividend_amount"), lit(0.0))
    ).withColumn(
        # Fill null split coefficients
        "split_coefficient",
        coalesce(col("split_coefficient"), lit(1.0))
    )
    
    # Validate OHLC integrity: high >= low
    cleansed_df = cleansed_df.filter(
        (col("high_price").isNull()) | 
        (col("low_price").isNull()) | 
        (col("high_price") >= col("low_price"))
    )
    
    print(f"Cleansed records: {cleansed_df.count()}")
    return cleansed_df


def standardize_dates(df: DataFrame) -> DataFrame:
    """
    Standardize date and timestamp columns.
    """
    return df.withColumn(
        "trade_date",
        to_date(col("timestamp"))
    ).withColumn(
        "trade_timestamp",
        to_timestamp(col("timestamp"))
    )


# ==============================================================================
# Technical Indicator Functions
# ==============================================================================
def calculate_derived_metrics(df: DataFrame) -> DataFrame:
    """
    Calculate derived price metrics.
    """
    print("Calculating derived metrics")
    
    # Window for previous day comparison
    window_prev = Window.partitionBy("symbol").orderBy("trade_date")
    
    return df.withColumn(
        # Daily price range
        "daily_range",
        col("high_price") - col("low_price")
    ).withColumn(
        # Daily range as percentage
        "daily_range_pct",
        when(
            col("close_price") > 0,
            (col("high_price") - col("low_price")) / col("close_price") * 100
        ).otherwise(None)
    ).withColumn(
        # Daily return (close to close)
        "prev_close",
        lag("close_price", 1).over(window_prev)
    ).withColumn(
        "daily_return",
        when(
            col("prev_close") > 0,
            (col("close_price") - col("prev_close")) / col("prev_close") * 100
        ).otherwise(None)
    ).withColumn(
        # Volume change percentage
        "prev_volume",
        lag("volume", 1).over(window_prev)
    ).withColumn(
        "volume_change_pct",
        when(
            col("prev_volume") > 0,
            (col("volume") - col("prev_volume")) / col("prev_volume") * 100
        ).otherwise(None)
    ).drop("prev_close", "prev_volume")


def calculate_moving_averages(df: DataFrame) -> DataFrame:
    """
    Calculate Simple Moving Averages (SMA) for various windows.
    """
    print("Calculating moving averages")
    
    # Define windows for different SMA periods
    window_5 = Window.partitionBy("symbol").orderBy("trade_date").rowsBetween(-4, 0)
    window_20 = Window.partitionBy("symbol").orderBy("trade_date").rowsBetween(-19, 0)
    window_50 = Window.partitionBy("symbol").orderBy("trade_date").rowsBetween(-49, 0)
    
    return df.withColumn(
        "sma_5",
        spark_round(avg("close_price").over(window_5), 4)
    ).withColumn(
        "sma_20",
        spark_round(avg("close_price").over(window_20), 4)
    ).withColumn(
        "sma_50",
        spark_round(avg("close_price").over(window_50), 4)
    )


def calculate_ema(df: DataFrame, column: str, periods: int) -> DataFrame:
    """
    Calculate Exponential Moving Average.
    
    Note: True EMA requires iterative calculation. This is a simplified version
    using weighted average approximation suitable for batch processing.
    """
    window = Window.partitionBy("symbol").orderBy("trade_date").rowsBetween(-(periods-1), 0)
    
    # Simplified EMA using weighted average
    smoothing = 2.0 / (periods + 1)
    
    return df.withColumn(
        f"ema_{periods}",
        spark_round(avg(col(column)).over(window) * smoothing + 
                   col(column) * (1 - smoothing), 4)
    )


def calculate_volatility(df: DataFrame) -> DataFrame:
    """
    Calculate rolling volatility (standard deviation of returns).
    """
    print("Calculating volatility")
    
    window_20d = Window.partitionBy("symbol").orderBy("trade_date").rowsBetween(-19, 0)
    
    return df.withColumn(
        "volatility_20d",
        spark_round(stddev("daily_return").over(window_20d), 4)
    )


def add_technical_indicators(df: DataFrame) -> DataFrame:
    """
    Add all technical indicators to the DataFrame.
    """
    df = calculate_derived_metrics(df)
    df = calculate_moving_averages(df)
    df = calculate_ema(df, "close_price", 12)
    df = calculate_ema(df, "close_price", 26)
    df = calculate_volatility(df)
    
    return df


# ==============================================================================
# Dimensional Modeling Functions
# ==============================================================================
def add_date_dimensions(df: DataFrame) -> DataFrame:
    """
    Add date dimension columns for analytical queries.
    """
    return df.withColumn(
        "year", year("trade_date")
    ).withColumn(
        "quarter", quarter("trade_date")
    ).withColumn(
        "month", month("trade_date")
    ).withColumn(
        "day", dayofmonth("trade_date")
    ).withColumn(
        "day_of_week", dayofweek("trade_date")
    )


def add_metadata(df: DataFrame, source_file: str = None) -> DataFrame:
    """
    Add processing metadata.
    """
    return df.withColumn(
        "processing_timestamp",
        current_timestamp()
    ).withColumn(
        "price_id",
        concat_ws("_", col("symbol"), date_format(col("trade_date"), "yyyyMMdd"))
    )


def select_final_columns(df: DataFrame) -> DataFrame:
    """
    Select and order final columns for the fact table.
    """
    final_columns = [
        "price_id",
        "symbol",
        "trade_date",
        "trade_timestamp",
        "open_price",
        "high_price",
        "low_price",
        "close_price",
        "adjusted_close",
        "volume",
        "dividend_amount",
        "split_coefficient",
        "daily_return",
        "daily_range",
        "daily_range_pct",
        "volume_change_pct",
        "sma_5",
        "sma_20",
        "sma_50",
        "ema_12",
        "ema_26",
        "volatility_20d",
        "year",
        "quarter",
        "month",
        "day",
        "day_of_week",
        "processing_timestamp",
    ]
    
    # Select only columns that exist
    available_columns = [c for c in final_columns if c in df.columns]
    return df.select(available_columns)


# ==============================================================================
# Data Writing Functions
# ==============================================================================
def write_to_curated(df: DataFrame, target_path: str, partition_cols: List[str] = None):
    """
    Write transformed data to curated layer in Parquet format.
    
    Args:
        df: Transformed DataFrame
        target_path: S3 path for output
        partition_cols: Columns to partition by
    """
    print(f"Writing to curated layer: {target_path}")
    
    partition_cols = partition_cols or ["year", "month"]
    
    # Repartition for optimal file sizes (target ~128MB per file)
    num_partitions = max(1, df.count() // 100000)
    
    df.repartition(num_partitions).write.mode(
        "overwrite"
    ).partitionBy(
        *partition_cols
    ).parquet(
        target_path
    )
    
    print(f"Successfully wrote {df.count()} records to {target_path}")


def write_daily_snapshot(df: DataFrame, bucket: str, prefix: str, date: str):
    """
    Write a daily snapshot without partitioning (for Redshift COPY).
    """
    snapshot_path = f"s3://{bucket}/{prefix}/date={date}/"
    
    print(f"Writing daily snapshot: {snapshot_path}")
    
    df.coalesce(1).write.mode(
        "overwrite"
    ).parquet(
        snapshot_path
    )
    
    print(f"Daily snapshot complete: {snapshot_path}")


# ==============================================================================
# Main ETL Pipeline
# ==============================================================================
def run_transformation():
    """
    Execute the complete ETL transformation pipeline.
    """
    print("=" * 60)
    print("STARTING FINANCIAL DATA TRANSFORMATION")
    print("=" * 60)
    
    # Step 1: Read raw data
    raw_df = read_partitioned_data(
        bucket=SOURCE_BUCKET,
        prefix="stock_data",
        date=EXECUTION_DATE
    )
    
    if raw_df.count() == 0:
        print("WARNING: No data found for processing")
        return
    
    # Step 2: Cleanse data
    cleansed_df = cleanse_data(raw_df)
    
    # Step 3: Standardize dates
    dated_df = standardize_dates(cleansed_df)
    
    # Step 4: Add technical indicators
    enriched_df = add_technical_indicators(dated_df)
    
    # Step 5: Add date dimensions
    dimensional_df = add_date_dimensions(enriched_df)
    
    # Step 6: Add metadata
    final_df = add_metadata(dimensional_df)
    
    # Step 7: Select final columns
    output_df = select_final_columns(final_df)
    
    # Step 8: Write to curated layer (partitioned)
    write_to_curated(
        df=output_df,
        target_path=f"s3://{TARGET_BUCKET}/processed/",
        partition_cols=["year", "month"]
    )
    
    # Step 9: Write daily snapshot (for Redshift)
    write_daily_snapshot(
        df=output_df,
        bucket=TARGET_BUCKET,
        prefix="daily_snapshots",
        date=EXECUTION_DATE
    )
    
    print("=" * 60)
    print("TRANSFORMATION COMPLETE")
    print("=" * 60)


# ==============================================================================
# Entry Point
# ==============================================================================
if __name__ == "__main__":
    try:
        run_transformation()
        job.commit()
    except Exception as e:
        print(f"ERROR: Transformation failed - {e}")
        raise e
