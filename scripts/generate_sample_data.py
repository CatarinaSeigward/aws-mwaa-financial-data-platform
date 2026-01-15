#!/usr/bin/env python3
"""
Sample Data Generator for Financial Data Pipeline
=================================================
生成模拟的股票市场数据，用于演示和测试

Features:
- 生成真实感的 OHLCV 数据
- 模拟价格波动和趋势
- 支持多个股票代码
- 输出格式与 Alpha Vantage API 一致
- 无需 API Key，完全离线运行

Usage:
    # 生成默认数据（5个股票，90天）
    python scripts/generate_sample_data.py

    # 自定义参数
    python scripts/generate_sample_data.py \
        --symbols AAPL GOOGL MSFT \
        --days 180 \
        --output data/raw/sample

    # 生成特定日期范围
    python scripts/generate_sample_data.py \
        --start-date 2024-01-01 \
        --end-date 2024-03-31
"""

import json
import argparse
import random
from pathlib import Path
from datetime import datetime, timedelta, date
from typing import List, Dict, Any, Tuple
import math

# 设置随机种子以确保可重复性
random.seed(42)


# ==============================================================================
# Stock Configuration
# ==============================================================================

STOCK_CONFIGS = {
    "AAPL": {
        "name": "Apple Inc.",
        "sector": "Technology",
        "initial_price": 180.0,
        "volatility": 0.02,  # 日波动率 2%
        "trend": 0.0003,     # 日趋势 0.03%
        "dividend_yield": 0.005,
        "split_probability": 0.001,
    },
    "GOOGL": {
        "name": "Alphabet Inc.",
        "sector": "Technology",
        "initial_price": 140.0,
        "volatility": 0.025,
        "trend": 0.0004,
        "dividend_yield": 0.0,
        "split_probability": 0.0005,
    },
    "MSFT": {
        "name": "Microsoft Corporation",
        "sector": "Technology",
        "initial_price": 370.0,
        "volatility": 0.018,
        "trend": 0.0005,
        "dividend_yield": 0.008,
        "split_probability": 0.0003,
    },
    "AMZN": {
        "name": "Amazon.com Inc.",
        "sector": "Consumer Cyclical",
        "initial_price": 155.0,
        "volatility": 0.03,
        "trend": 0.0002,
        "dividend_yield": 0.0,
        "split_probability": 0.0008,
    },
    "TSLA": {
        "name": "Tesla Inc.",
        "sector": "Automotive",
        "initial_price": 240.0,
        "volatility": 0.04,  # 高波动率
        "trend": 0.0001,
        "dividend_yield": 0.0,
        "split_probability": 0.001,
    },
    "NVDA": {
        "name": "NVIDIA Corporation",
        "sector": "Technology",
        "initial_price": 500.0,
        "volatility": 0.035,
        "trend": 0.0008,  # 强势上涨
        "dividend_yield": 0.001,
        "split_probability": 0.002,
    },
    "META": {
        "name": "Meta Platforms Inc.",
        "sector": "Technology",
        "initial_price": 350.0,
        "volatility": 0.028,
        "trend": 0.0003,
        "dividend_yield": 0.0,
        "split_probability": 0.0,
    },
    "JPM": {
        "name": "JPMorgan Chase & Co.",
        "sector": "Financial Services",
        "initial_price": 150.0,
        "volatility": 0.015,
        "trend": 0.0002,
        "dividend_yield": 0.025,
        "split_probability": 0.0001,
    },
}


# ==============================================================================
# Price Generation Functions
# ==============================================================================

def generate_daily_ohlc(
    previous_close: float,
    volatility: float,
    trend: float,
    volume_base: int = 50_000_000,
) -> Tuple[float, float, float, float, int]:
    """
    生成单日的 OHLC 数据

    Args:
        previous_close: 前一日收盘价
        volatility: 波动率
        trend: 趋势（正值=上涨，负值=下跌）
        volume_base: 基础成交量

    Returns:
        (open, high, low, close, volume)
    """
    # 开盘价：前收盘 ± 随机跳空
    gap = random.gauss(0, volatility / 2)
    open_price = previous_close * (1 + gap)

    # 日内波动
    intraday_volatility = random.gauss(volatility, volatility / 3)
    intraday_volatility = max(0.001, intraday_volatility)  # 最小波动

    # 收盘价：趋势 + 随机波动
    price_change = random.gauss(trend, volatility)
    close_price = open_price * (1 + price_change)

    # 最高价和最低价
    # 确保: low <= open,close <= high
    intraday_range = abs(random.gauss(0, intraday_volatility))

    high_price = max(open_price, close_price) * (1 + intraday_range)
    low_price = min(open_price, close_price) * (1 - intraday_range)

    # 确保逻辑关系
    high_price = max(high_price, open_price, close_price)
    low_price = min(low_price, open_price, close_price)

    # 成交量：基础量 ± 随机变化
    volume_multiplier = random.lognormvariate(0, 0.5)
    volume = int(volume_base * volume_multiplier)

    return (
        round(open_price, 2),
        round(high_price, 2),
        round(low_price, 2),
        round(close_price, 2),
        volume,
    )


def generate_stock_history(
    symbol: str,
    start_date: date,
    end_date: date,
    config: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """
    生成完整的股票历史数据

    Args:
        symbol: 股票代码
        start_date: 开始日期
        end_date: 结束日期
        config: 股票配置

    Returns:
        按日期倒序排列的数据列表
    """
    records = []
    current_date = start_date
    current_price = config["initial_price"]

    # 累积变量
    cumulative_dividend = 0.0
    split_coefficient = 1.0

    while current_date <= end_date:
        # 跳过周末（简化）
        if current_date.weekday() >= 5:  # 周六、周日
            current_date += timedelta(days=1)
            continue

        # 生成 OHLC 数据
        open_price, high, low, close_price, volume = generate_daily_ohlc(
            previous_close=current_price,
            volatility=config["volatility"],
            trend=config["trend"],
        )

        # 调整后收盘价（考虑分红和拆股）
        adjusted_close = close_price

        # 分红（季度末，概率性）
        dividend = 0.0
        if current_date.month % 3 == 0 and current_date.day >= 28:
            if random.random() < 0.3:  # 30% 概率发放分红
                dividend = close_price * config["dividend_yield"] / 4  # 季度分红
                cumulative_dividend += dividend
                adjusted_close = close_price - dividend

        # 股票拆分（罕见事件）
        split = 1.0
        if random.random() < config["split_probability"]:
            split = random.choice([2.0, 3.0, 0.5])  # 2:1, 3:1 拆分或 1:2 合并
            split_coefficient *= split
            # 拆分后调整所有价格
            open_price /= split
            high /= split
            low /= split
            close_price /= split
            adjusted_close /= split
            volume = int(volume * split)

        # 记录数据
        record = {
            "symbol": symbol,
            "timestamp": current_date.strftime("%Y-%m-%d"),
            "open_price": round(open_price, 4),
            "high_price": round(high, 4),
            "low_price": round(low, 4),
            "close_price": round(close_price, 4),
            "adjusted_close": round(adjusted_close, 4),
            "volume": volume,
            "dividend_amount": round(dividend, 4),
            "split_coefficient": round(split, 2),
        }

        records.append(record)

        # 更新当前价格
        current_price = close_price

        # 下一天
        current_date += timedelta(days=1)

    # Alpha Vantage 格式：最新日期在前
    return list(reversed(records))


# ==============================================================================
# Output Functions
# ==============================================================================

def save_to_json(
    records: List[Dict[str, Any]],
    symbol: str,
    output_dir: Path,
    execution_date: str,
) -> Path:
    """
    保存数据为 JSON 文件（Alpha Vantage 格式）

    Args:
        records: 数据记录列表
        symbol: 股票代码
        output_dir: 输出目录
        execution_date: 执行日期（用于分区）

    Returns:
        输出文件路径
    """
    # 创建分区目录结构: data/raw/date=YYYY-MM-DD/symbol=XXX/
    partition_dir = output_dir / f"date={execution_date}" / f"symbol={symbol}"
    partition_dir.mkdir(parents=True, exist_ok=True)

    # 输出文件
    output_file = partition_dir / f"{symbol}_{execution_date}.json"

    # Alpha Vantage API 响应格式
    data = {
        "meta": {
            "symbol": symbol,
            "last_refreshed": execution_date,
            "output_size": "full",
            "time_zone": "US/Eastern",
            "ingestion_timestamp": datetime.utcnow().isoformat(),
        },
        "data": records,
    }

    # 写入 JSON
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    return output_file


def print_summary(
    symbols: List[str],
    records_per_symbol: Dict[str, int],
    output_files: List[Path],
    start_date: date,
    end_date: date,
):
    """
    打印生成摘要
    """
    print("\n" + "=" * 70)
    print("📊 Sample Data Generation Complete")
    print("=" * 70)

    print(f"\n📅 Date Range: {start_date} to {end_date}")
    print(f"📈 Symbols: {', '.join(symbols)}")
    print(f"📝 Total Files: {len(output_files)}")

    print("\n📊 Records per Symbol:")
    for symbol, count in records_per_symbol.items():
        print(f"   {symbol}: {count:,} records")

    print(f"\n💾 Output Files:")
    for file in output_files[:5]:  # 显示前 5 个
        size_kb = file.stat().st_size / 1024
        print(f"   {file.relative_to(file.parents[3])}")
        print(f"      └─ {size_kb:.1f} KB")

    if len(output_files) > 5:
        print(f"   ... and {len(output_files) - 5} more files")

    total_size = sum(f.stat().st_size for f in output_files) / (1024 * 1024)
    print(f"\n💿 Total Size: {total_size:.2f} MB")

    print("\n✅ Ready for ETL pipeline!")
    print("=" * 70)


# ==============================================================================
# Main Function
# ==============================================================================

def main():
    """
    主函数
    """
    parser = argparse.ArgumentParser(
        description='Generate sample financial data for testing',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Generate default data (5 symbols, 90 days)
  python scripts/generate_sample_data.py

  # Custom symbols and duration
  python scripts/generate_sample_data.py --symbols AAPL GOOGL --days 180

  # Specific date range
  python scripts/generate_sample_data.py --start-date 2024-01-01 --end-date 2024-03-31

  # Custom output directory
  python scripts/generate_sample_data.py --output data/raw/test
        """
    )

    parser.add_argument(
        '--symbols',
        nargs='+',
        default=['AAPL', 'GOOGL', 'MSFT', 'AMZN', 'TSLA'],
        choices=list(STOCK_CONFIGS.keys()),
        help='Stock symbols to generate (default: AAPL GOOGL MSFT AMZN TSLA)'
    )

    parser.add_argument(
        '--days',
        type=int,
        default=90,
        help='Number of days to generate (default: 90)'
    )

    parser.add_argument(
        '--start-date',
        type=str,
        help='Start date (YYYY-MM-DD, default: <days> ago)'
    )

    parser.add_argument(
        '--end-date',
        type=str,
        help='End date (YYYY-MM-DD, default: today)'
    )

    parser.add_argument(
        '--output',
        type=str,
        default='data/raw',
        help='Output directory (default: data/raw)'
    )

    parser.add_argument(
        '--execution-date',
        type=str,
        help='Execution date for partitioning (default: end-date)'
    )

    args = parser.parse_args()

    # 解析日期
    if args.end_date:
        end_date = datetime.strptime(args.end_date, '%Y-%m-%d').date()
    else:
        end_date = date.today()

    if args.start_date:
        start_date = datetime.strptime(args.start_date, '%Y-%m-%d').date()
    else:
        start_date = end_date - timedelta(days=args.days)

    execution_date = args.execution_date or end_date.strftime('%Y-%m-%d')

    # 输出目录
    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)

    print("\n" + "=" * 70)
    print("🚀 Starting Sample Data Generation")
    print("=" * 70)
    print(f"\n📅 Date Range: {start_date} to {end_date} ({(end_date - start_date).days} days)")
    print(f"📈 Symbols: {', '.join(args.symbols)}")
    print(f"💾 Output: {output_dir.absolute()}")
    print("\n" + "=" * 70)

    # 生成数据
    output_files = []
    records_per_symbol = {}

    for symbol in args.symbols:
        print(f"\n📊 Generating data for {symbol}...")

        config = STOCK_CONFIGS[symbol]

        # 生成历史数据
        records = generate_stock_history(
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
            config=config,
        )

        records_per_symbol[symbol] = len(records)

        # 保存 JSON
        output_file = save_to_json(
            records=records,
            symbol=symbol,
            output_dir=output_dir,
            execution_date=execution_date,
        )

        output_files.append(output_file)

        print(f"   ✅ Generated {len(records):,} records")
        print(f"   💾 Saved to: {output_file.relative_to(output_dir.parent)}")

    # 打印摘要
    print_summary(
        symbols=args.symbols,
        records_per_symbol=records_per_symbol,
        output_files=output_files,
        start_date=start_date,
        end_date=end_date,
    )


if __name__ == "__main__":
    main()
