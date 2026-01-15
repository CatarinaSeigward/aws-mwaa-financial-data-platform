#!/usr/bin/env python3
"""
Enhanced Data Generator for Financial Data Pipeline
===================================================
生成高质量、可配置的模拟股票数据

Features:
- 支持自定义股票配置（价格、波动率、趋势）
- 模拟真实市场行为（价格走势、成交量、分红、拆股）
- 支持多种输出格式（JSON、CSV、Parquet）
- 可生成预定义数据集（小、中、大规模）
- 与 Alpha Vantage API 格式完全兼容
- 支持增量数据生成

Usage:
    # 快速生成默认数据集
    python scripts/data_generator.py --preset demo

    # 自定义生成
    python scripts/data_generator.py \
        --symbols AAPL GOOGL MSFT \
        --days 180 \
        --format json \
        --trend bullish

    # 生成大规模测试数据
    python scripts/data_generator.py --preset large

    # 增量生成（追加新数据）
    python scripts/data_generator.py \
        --incremental \
        --days 30
"""

import json
import csv
import argparse
import random
import logging
from pathlib import Path
from datetime import datetime, timedelta, date
from typing import List, Dict, Any, Tuple, Optional
import math

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# 设置随机种子以确保可重复性
random.seed(42)


# ==============================================================================
# 股票配置库
# ==============================================================================

class StockConfig:
    """股票配置类"""

    CONFIGS = {
        "AAPL": {
            "name": "Apple Inc.",
            "sector": "Technology",
            "exchange": "NASDAQ",
            "initial_price": 180.0,
            "volatility": 0.020,      # 2% 日波动率
            "trend": 0.0003,          # 0.03% 日趋势
            "dividend_yield": 0.005,  # 0.5% 年化股息
            "split_probability": 0.001,
            "volume_base": 60_000_000,
        },
        "GOOGL": {
            "name": "Alphabet Inc.",
            "sector": "Technology",
            "exchange": "NASDAQ",
            "initial_price": 140.0,
            "volatility": 0.025,
            "trend": 0.0004,
            "dividend_yield": 0.0,
            "split_probability": 0.0005,
            "volume_base": 25_000_000,
        },
        "MSFT": {
            "name": "Microsoft Corporation",
            "sector": "Technology",
            "exchange": "NASDAQ",
            "initial_price": 370.0,
            "volatility": 0.018,
            "trend": 0.0005,
            "dividend_yield": 0.008,
            "split_probability": 0.0003,
            "volume_base": 30_000_000,
        },
        "AMZN": {
            "name": "Amazon.com Inc.",
            "sector": "Consumer Cyclical",
            "exchange": "NASDAQ",
            "initial_price": 155.0,
            "volatility": 0.030,
            "trend": 0.0002,
            "dividend_yield": 0.0,
            "split_probability": 0.0008,
            "volume_base": 50_000_000,
        },
        "TSLA": {
            "name": "Tesla Inc.",
            "sector": "Automotive",
            "exchange": "NASDAQ",
            "initial_price": 240.0,
            "volatility": 0.040,
            "trend": 0.0001,
            "dividend_yield": 0.0,
            "split_probability": 0.001,
            "volume_base": 100_000_000,
        },
        "NVDA": {
            "name": "NVIDIA Corporation",
            "sector": "Technology",
            "exchange": "NASDAQ",
            "initial_price": 500.0,
            "volatility": 0.035,
            "trend": 0.0008,
            "dividend_yield": 0.001,
            "split_probability": 0.002,
            "volume_base": 40_000_000,
        },
        "META": {
            "name": "Meta Platforms Inc.",
            "sector": "Technology",
            "exchange": "NASDAQ",
            "initial_price": 350.0,
            "volatility": 0.028,
            "trend": 0.0003,
            "dividend_yield": 0.0,
            "split_probability": 0.0,
            "volume_base": 20_000_000,
        },
        "JPM": {
            "name": "JPMorgan Chase & Co.",
            "sector": "Financial Services",
            "exchange": "NYSE",
            "initial_price": 150.0,
            "volatility": 0.015,
            "trend": 0.0002,
            "dividend_yield": 0.025,
            "split_probability": 0.0001,
            "volume_base": 15_000_000,
        },
    }

    @classmethod
    def get(cls, symbol: str) -> Dict[str, Any]:
        """获取股票配置"""
        return cls.CONFIGS.get(symbol.upper(), cls.CONFIGS["AAPL"])

    @classmethod
    def all_symbols(cls) -> List[str]:
        """获取所有支持的股票代码"""
        return list(cls.CONFIGS.keys())

    @classmethod
    def apply_market_condition(cls, symbol: str, condition: str) -> Dict[str, Any]:
        """
        根据市场条件调整配置

        Args:
            symbol: 股票代码
            condition: 市场条件 (bullish, bearish, volatile, stable)
        """
        config = cls.get(symbol).copy()

        if condition == "bullish":
            config["trend"] *= 3  # 强势上涨
            config["volatility"] *= 0.8  # 降低波动
        elif condition == "bearish":
            config["trend"] *= -2  # 下跌
            config["volatility"] *= 1.2  # 增加波动
        elif condition == "volatile":
            config["volatility"] *= 2  # 高波动
            config["trend"] *= 0.5  # 趋势减弱
        elif condition == "stable":
            config["volatility"] *= 0.5  # 低波动
            config["trend"] *= 0.5  # 平稳

        return config


# ==============================================================================
# 数据生成引擎
# ==============================================================================

class StockDataGenerator:
    """股票数据生成器"""

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.current_price = config["initial_price"]
        self.cumulative_dividend = 0.0
        self.split_coefficient = 1.0

    def generate_daily_ohlcv(self) -> Tuple[float, float, float, float, int]:
        """
        生成单日 OHLCV 数据

        Returns:
            (open, high, low, close, volume)
        """
        volatility = self.config["volatility"]
        trend = self.config["trend"]
        volume_base = self.config["volume_base"]

        # 开盘价：前收盘 ± 跳空
        gap = random.gauss(0, volatility / 2)
        open_price = self.current_price * (1 + gap)

        # 日内波动
        intraday_vol = abs(random.gauss(volatility, volatility / 3))
        intraday_vol = max(0.001, intraday_vol)

        # 收盘价：趋势 + 随机
        price_change = random.gauss(trend, volatility)
        close_price = open_price * (1 + price_change)

        # 高低价
        intraday_range = abs(random.gauss(0, intraday_vol))
        high_price = max(open_price, close_price) * (1 + intraday_range)
        low_price = min(open_price, close_price) * (1 - intraday_range)

        # 确保逻辑正确
        high_price = max(high_price, open_price, close_price)
        low_price = min(low_price, open_price, close_price)

        # 成交量
        volume_multiplier = random.lognormvariate(0, 0.5)
        volume = int(volume_base * volume_multiplier)

        return (
            round(open_price, 4),
            round(high_price, 4),
            round(low_price, 4),
            round(close_price, 4),
            volume,
        )

    def generate_dividend(self, current_date: date, close_price: float) -> float:
        """
        生成分红（季度末概率性发放）

        Args:
            current_date: 当前日期
            close_price: 收盘价

        Returns:
            分红金额
        """
        if current_date.month % 3 != 0 or current_date.day < 28:
            return 0.0

        if random.random() < 0.3:  # 30% 概率发放
            dividend = close_price * self.config["dividend_yield"] / 4
            self.cumulative_dividend += dividend
            return round(dividend, 4)

        return 0.0

    def generate_split(self) -> float:
        """
        生成股票拆分（罕见事件）

        Returns:
            拆分系数 (1.0 = 无拆分)
        """
        if random.random() < self.config["split_probability"]:
            split = random.choice([2.0, 3.0, 0.5])  # 2:1, 3:1 或 1:2
            self.split_coefficient *= split
            return split
        return 1.0

    def generate_history(
        self,
        symbol: str,
        start_date: date,
        end_date: date,
    ) -> List[Dict[str, Any]]:
        """
        生成完整历史数据

        Args:
            symbol: 股票代码
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            数据记录列表（最新日期在前）
        """
        records = []
        current_date = start_date

        while current_date <= end_date:
            # 跳过周末
            if current_date.weekday() >= 5:
                current_date += timedelta(days=1)
                continue

            # 生成 OHLC
            open_p, high, low, close_p, volume = self.generate_daily_ohlcv()

            # 分红
            dividend = self.generate_dividend(current_date, close_p)

            # 拆股
            split = self.generate_split()
            if split != 1.0:
                open_p /= split
                high /= split
                low /= split
                close_p /= split
                volume = int(volume * split)

            # 调整后收盘价
            adjusted_close = close_p - dividend

            # 记录
            record = {
                "symbol": symbol,
                "timestamp": current_date.strftime("%Y-%m-%d"),
                "open_price": round(open_p, 4),
                "high_price": round(high, 4),
                "low_price": round(low, 4),
                "close_price": round(close_p, 4),
                "adjusted_close": round(adjusted_close, 4),
                "volume": volume,
                "dividend_amount": round(dividend, 4),
                "split_coefficient": round(split, 2),
            }

            records.append(record)

            # 更新当前价格
            self.current_price = close_p

            current_date += timedelta(days=1)

        # 最新日期在前
        return list(reversed(records))


# ==============================================================================
# 输出格式化器
# ==============================================================================

class OutputFormatter:
    """输出格式化器"""

    @staticmethod
    def to_json(
        records: List[Dict[str, Any]],
        symbol: str,
        output_file: Path,
        metadata: Optional[Dict[str, Any]] = None,
    ):
        """保存为 JSON（Alpha Vantage 格式）"""
        data = {
            "meta": metadata or {
                "symbol": symbol,
                "last_refreshed": datetime.now().strftime("%Y-%m-%d"),
                "output_size": "full",
                "time_zone": "US/Eastern",
                "ingestion_timestamp": datetime.utcnow().isoformat(),
                "data_source": "simulated",
            },
            "data": records,
        }

        output_file.parent.mkdir(parents=True, exist_ok=True)
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

        logger.info(f"✅ JSON saved: {output_file} ({len(records)} records)")

    @staticmethod
    def to_csv(records: List[Dict[str, Any]], output_file: Path):
        """保存为 CSV"""
        if not records:
            return

        output_file.parent.mkdir(parents=True, exist_ok=True)

        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=records[0].keys())
            writer.writeheader()
            writer.writerows(records)

        logger.info(f"✅ CSV saved: {output_file} ({len(records)} records)")

    @staticmethod
    def to_parquet(records: List[Dict[str, Any]], output_file: Path):
        """保存为 Parquet"""
        try:
            import pandas as pd

            df = pd.DataFrame(records)
            df['timestamp'] = pd.to_datetime(df['timestamp'])

            output_file.parent.mkdir(parents=True, exist_ok=True)
            df.to_parquet(output_file, compression='snappy', index=False)

            logger.info(f"✅ Parquet saved: {output_file} ({len(records)} records)")
        except ImportError:
            logger.error("❌ pandas/pyarrow not installed. Cannot save Parquet.")


# ==============================================================================
# 预设数据集
# ==============================================================================

class Presets:
    """预定义数据集"""

    DEMO = {
        "name": "Demo Dataset",
        "symbols": ["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA"],
        "days": 90,
        "market_condition": "normal",
        "description": "适合快速演示的小规模数据集",
    }

    SMALL = {
        "name": "Small Dataset",
        "symbols": ["AAPL", "GOOGL", "MSFT"],
        "days": 30,
        "market_condition": "normal",
        "description": "小规模测试数据集",
    }

    MEDIUM = {
        "name": "Medium Dataset",
        "symbols": ["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA", "NVDA"],
        "days": 180,
        "market_condition": "normal",
        "description": "中等规模开发数据集",
    }

    LARGE = {
        "name": "Large Dataset",
        "symbols": StockConfig.all_symbols(),
        "days": 365,
        "market_condition": "normal",
        "description": "大规模压力测试数据集",
    }

    @classmethod
    def get(cls, preset: str) -> Dict[str, Any]:
        """获取预设配置"""
        presets = {
            "demo": cls.DEMO,
            "small": cls.SMALL,
            "medium": cls.MEDIUM,
            "large": cls.LARGE,
        }
        return presets.get(preset.lower(), cls.DEMO)


# ==============================================================================
# 主生成器
# ==============================================================================

class DataGeneratorOrchestrator:
    """数据生成编排器"""

    def __init__(
        self,
        symbols: List[str],
        start_date: date,
        end_date: date,
        output_dir: Path,
        output_format: str = "json",
        market_condition: str = "normal",
        execution_date: Optional[str] = None,
    ):
        self.symbols = symbols
        self.start_date = start_date
        self.end_date = end_date
        self.output_dir = output_dir
        self.output_format = output_format
        self.market_condition = market_condition
        self.execution_date = execution_date or end_date.strftime("%Y-%m-%d")

    def generate_all(self) -> Dict[str, Any]:
        """生成所有数据"""
        logger.info("=" * 70)
        logger.info("🚀 Starting Data Generation")
        logger.info("=" * 70)
        logger.info(f"📅 Date Range: {self.start_date} to {self.end_date}")
        logger.info(f"📈 Symbols: {', '.join(self.symbols)}")
        logger.info(f"📊 Market Condition: {self.market_condition}")
        logger.info(f"💾 Output: {self.output_dir}")
        logger.info(f"📄 Format: {self.output_format}")
        logger.info("=" * 70)

        results = {
            "symbols": {},
            "total_records": 0,
            "output_files": [],
        }

        for symbol in self.symbols:
            logger.info(f"\n📊 Generating data for {symbol}...")

            # 获取配置
            config = StockConfig.apply_market_condition(symbol, self.market_condition)

            # 生成数据
            generator = StockDataGenerator(config)
            records = generator.generate_history(symbol, self.start_date, self.end_date)

            # 保存数据
            output_file = self._save_data(symbol, records)

            results["symbols"][symbol] = {
                "records": len(records),
                "file": str(output_file),
            }
            results["total_records"] += len(records)
            results["output_files"].append(output_file)

            logger.info(f"   ✅ Generated {len(records):,} records")

        self._print_summary(results)

        return results

    def _save_data(self, symbol: str, records: List[Dict[str, Any]]) -> Path:
        """保存数据到文件"""
        # 创建分区目录
        partition_dir = self.output_dir / f"date={self.execution_date}" / f"symbol={symbol}"

        # 根据格式保存
        if self.output_format == "json":
            output_file = partition_dir / f"{symbol}_{self.execution_date}.json"
            OutputFormatter.to_json(records, symbol, output_file)
        elif self.output_format == "csv":
            output_file = partition_dir / f"{symbol}_{self.execution_date}.csv"
            OutputFormatter.to_csv(records, output_file)
        elif self.output_format == "parquet":
            output_file = partition_dir / f"{symbol}_{self.execution_date}.parquet"
            OutputFormatter.to_parquet(records, output_file)
        else:
            raise ValueError(f"Unsupported format: {self.output_format}")

        return output_file

    def _print_summary(self, results: Dict[str, Any]):
        """打印生成摘要"""
        logger.info("\n" + "=" * 70)
        logger.info("📊 Generation Complete")
        logger.info("=" * 70)

        logger.info(f"\n📈 Records per Symbol:")
        for symbol, data in results["symbols"].items():
            logger.info(f"   {symbol}: {data['records']:,} records")

        logger.info(f"\n💾 Output Files:")
        for file in results["output_files"][:5]:
            size_kb = file.stat().st_size / 1024
            logger.info(f"   {file.name} ({size_kb:.1f} KB)")

        if len(results["output_files"]) > 5:
            logger.info(f"   ... and {len(results['output_files']) - 5} more")

        total_size = sum(f.stat().st_size for f in results["output_files"]) / (1024 * 1024)
        logger.info(f"\n💿 Total: {results['total_records']:,} records, {total_size:.2f} MB")
        logger.info("\n✅ Ready for ETL pipeline!")
        logger.info("=" * 70)


# ==============================================================================
# 命令行接口
# ==============================================================================

def main():
    """主函数"""
    parser = argparse.ArgumentParser(
        description='Enhanced Financial Data Generator',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Quick demo dataset
  python scripts/data_generator.py --preset demo

  # Custom generation
  python scripts/data_generator.py --symbols AAPL GOOGL --days 180

  # Bullish market scenario
  python scripts/data_generator.py --preset medium --market bullish

  # Large-scale testing
  python scripts/data_generator.py --preset large --format parquet

Presets:
  demo   - 5 symbols, 90 days (recommended for interviews)
  small  - 3 symbols, 30 days
  medium - 6 symbols, 180 days
  large  - 8 symbols, 365 days

Market Conditions:
  normal   - Standard market behavior
  bullish  - Strong uptrend with lower volatility
  bearish  - Downtrend with higher volatility
  volatile - High volatility with weak trend
  stable   - Low volatility with weak trend
        """
    )

    # 预设或自定义
    parser.add_argument(
        '--preset',
        choices=['demo', 'small', 'medium', 'large'],
        help='Use predefined dataset configuration'
    )

    # 自定义参数
    parser.add_argument(
        '--symbols',
        nargs='+',
        choices=StockConfig.all_symbols(),
        help='Stock symbols to generate'
    )

    parser.add_argument(
        '--days',
        type=int,
        help='Number of days to generate'
    )

    parser.add_argument(
        '--start-date',
        type=str,
        help='Start date (YYYY-MM-DD)'
    )

    parser.add_argument(
        '--end-date',
        type=str,
        help='End date (YYYY-MM-DD)'
    )

    # 输出选项
    parser.add_argument(
        '--output',
        type=str,
        default='data/raw',
        help='Output directory (default: data/raw)'
    )

    parser.add_argument(
        '--format',
        choices=['json', 'csv', 'parquet'],
        default='json',
        help='Output format (default: json)'
    )

    parser.add_argument(
        '--execution-date',
        type=str,
        help='Execution date for partitioning (default: end-date)'
    )

    # 市场条件
    parser.add_argument(
        '--market',
        choices=['normal', 'bullish', 'bearish', 'volatile', 'stable'],
        default='normal',
        help='Market condition (default: normal)'
    )

    # 其他选项
    parser.add_argument(
        '--seed',
        type=int,
        default=42,
        help='Random seed for reproducibility (default: 42)'
    )

    args = parser.parse_args()

    # 设置随机种子
    random.seed(args.seed)

    # 确定配置
    if args.preset:
        preset_config = Presets.get(args.preset)
        symbols = preset_config["symbols"]
        days = preset_config["days"]
        market_condition = preset_config.get("market_condition", "normal")
        logger.info(f"📦 Using preset: {preset_config['name']}")
        logger.info(f"   {preset_config['description']}")
    else:
        symbols = args.symbols or ["AAPL", "GOOGL", "MSFT"]
        days = args.days or 90
        market_condition = args.market

    # 确定日期范围
    if args.end_date:
        end_date = datetime.strptime(args.end_date, '%Y-%m-%d').date()
    else:
        end_date = date.today()

    if args.start_date:
        start_date = datetime.strptime(args.start_date, '%Y-%m-%d').date()
    else:
        start_date = end_date - timedelta(days=days)

    # 创建生成器
    orchestrator = DataGeneratorOrchestrator(
        symbols=symbols,
        start_date=start_date,
        end_date=end_date,
        output_dir=Path(args.output),
        output_format=args.format,
        market_condition=market_condition,
        execution_date=args.execution_date,
    )

    # 生成数据
    orchestrator.generate_all()


if __name__ == "__main__":
    main()
