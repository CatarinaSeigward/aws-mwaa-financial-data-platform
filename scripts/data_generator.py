#!/usr/bin/env python3
"""
Enhanced Data Generator for Financial Data Pipeline
===================================================
Geometric Brownian Motion (GBM) based market simulator for generating
high-quality, configurable synthetic stock data.

The price process follows the SDE:
    dS = mu * S * dt + sigma * S * dW

With closed-form solution:
    S(t+dt) = S(t) * exp((mu - sigma^2/2) * dt + sigma * sqrt(dt) * Z)
    where Z ~ N(0, 1)

Features:
- Geometric Brownian Motion price simulation (log-normal returns)
- Configurable drift (mu) and volatility (sigma) per stock
- Realistic market behavior (OHLCV, dividends, stock splits)
- Multiple output formats (JSON, CSV, Parquet)
- Predefined dataset presets (demo, small, medium, large)
- Alpha Vantage API format compatibility
- Incremental data generation support

Usage:
    # Quick demo dataset
    python scripts/data_generator.py --preset demo

    # Custom generation
    python scripts/data_generator.py \
        --symbols AAPL GOOGL MSFT \
        --days 180 \
        --format json \
        --trend bullish

    # Large-scale stress test data
    python scripts/data_generator.py --preset large

    # Incremental generation (append new data)
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
    """
    Stock configuration for GBM simulation.

    Each stock is parameterized by:
        - mu (annualized drift): expected return per year
        - sigma (annualized volatility): standard deviation of returns per year
        - Daily parameters are derived as:
            daily_mu = mu / 252
            daily_sigma = sigma / sqrt(252)
    """

    CONFIGS = {
        "AAPL": {
            "name": "Apple Inc.",
            "sector": "Technology",
            "exchange": "NASDAQ",
            "initial_price": 180.0,
            "mu": 0.08,               # 8% annualized drift
            "sigma": 0.30,            # 30% annualized volatility
            "dividend_yield": 0.005,   # 0.5% annual dividend yield
            "split_probability": 0.001,
            "volume_base": 60_000_000,
        },
        "GOOGL": {
            "name": "Alphabet Inc.",
            "sector": "Technology",
            "exchange": "NASDAQ",
            "initial_price": 140.0,
            "mu": 0.10,               # 10% annualized drift
            "sigma": 0.35,            # 35% annualized volatility
            "dividend_yield": 0.0,
            "split_probability": 0.0005,
            "volume_base": 25_000_000,
        },
        "MSFT": {
            "name": "Microsoft Corporation",
            "sector": "Technology",
            "exchange": "NASDAQ",
            "initial_price": 370.0,
            "mu": 0.12,               # 12% annualized drift
            "sigma": 0.28,            # 28% annualized volatility
            "dividend_yield": 0.008,
            "split_probability": 0.0003,
            "volume_base": 30_000_000,
        },
        "AMZN": {
            "name": "Amazon.com Inc.",
            "sector": "Consumer Cyclical",
            "exchange": "NASDAQ",
            "initial_price": 155.0,
            "mu": 0.06,               # 6% annualized drift
            "sigma": 0.40,            # 40% annualized volatility
            "dividend_yield": 0.0,
            "split_probability": 0.0008,
            "volume_base": 50_000_000,
        },
        "TSLA": {
            "name": "Tesla Inc.",
            "sector": "Automotive",
            "exchange": "NASDAQ",
            "initial_price": 240.0,
            "mu": 0.03,               # 3% annualized drift
            "sigma": 0.55,            # 55% annualized volatility
            "dividend_yield": 0.0,
            "split_probability": 0.001,
            "volume_base": 100_000_000,
        },
        "NVDA": {
            "name": "NVIDIA Corporation",
            "sector": "Technology",
            "exchange": "NASDAQ",
            "initial_price": 500.0,
            "mu": 0.20,               # 20% annualized drift
            "sigma": 0.50,            # 50% annualized volatility
            "dividend_yield": 0.001,
            "split_probability": 0.002,
            "volume_base": 40_000_000,
        },
        "META": {
            "name": "Meta Platforms Inc.",
            "sector": "Technology",
            "exchange": "NASDAQ",
            "initial_price": 350.0,
            "mu": 0.08,               # 8% annualized drift
            "sigma": 0.38,            # 38% annualized volatility
            "dividend_yield": 0.0,
            "split_probability": 0.0,
            "volume_base": 20_000_000,
        },
        "JPM": {
            "name": "JPMorgan Chase & Co.",
            "sector": "Financial Services",
            "exchange": "NYSE",
            "initial_price": 150.0,
            "mu": 0.06,               # 6% annualized drift
            "sigma": 0.22,            # 22% annualized volatility
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
        Adjust GBM parameters (mu, sigma) based on market regime.

        Args:
            symbol: Stock ticker
            condition: Market regime (bullish, bearish, volatile, stable)
        """
        config = cls.get(symbol).copy()

        if condition == "bullish":
            config["mu"] *= 3          # Strong upward drift
            config["sigma"] *= 0.8     # Lower volatility
        elif condition == "bearish":
            config["mu"] *= -2         # Negative drift
            config["sigma"] *= 1.2     # Higher volatility
        elif condition == "volatile":
            config["sigma"] *= 2       # High volatility
            config["mu"] *= 0.5        # Weaker drift
        elif condition == "stable":
            config["sigma"] *= 0.5     # Low volatility
            config["mu"] *= 0.5        # Mild drift

        return config


# ==============================================================================
# 数据生成引擎
# ==============================================================================

class StockDataGenerator:
    """
    Geometric Brownian Motion (GBM) based stock data generator.

    Implements the GBM stochastic differential equation:
        dS = mu * S * dt + sigma * S * dW

    Closed-form (exact) solution for discrete time steps:
        S(t+dt) = S(t) * exp((mu - sigma^2/2) * dt + sigma * sqrt(dt) * Z)
        where Z ~ N(0,1) is a standard normal variate.

    This ensures log-returns are normally distributed (prices are log-normal),
    which is the standard assumption in quantitative finance (Black-Scholes).
    """

    # Trading days per year (used to convert annualized params to daily)
    TRADING_DAYS_PER_YEAR = 252

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.current_price = config["initial_price"]
        self.cumulative_dividend = 0.0
        self.split_coefficient = 1.0

        # Convert annualized GBM parameters to daily
        self.mu_annual = config["mu"]           # annualized drift
        self.sigma_annual = config["sigma"]     # annualized volatility
        self.dt = 1.0 / self.TRADING_DAYS_PER_YEAR  # one trading day

        # Daily parameters derived from annualized values
        self.daily_drift = (self.mu_annual - 0.5 * self.sigma_annual ** 2) * self.dt
        self.daily_diffusion = self.sigma_annual * math.sqrt(self.dt)

    def _gbm_step(self, price: float) -> float:
        """
        Single GBM step using the exact closed-form solution.

        S(t+dt) = S(t) * exp((mu - sigma^2/2)*dt + sigma*sqrt(dt)*Z)

        Returns:
            New price after one trading day.
        """
        z = random.gauss(0, 1)
        return price * math.exp(self.daily_drift + self.daily_diffusion * z)

    def generate_daily_ohlcv(self) -> Tuple[float, float, float, float, int]:
        """
        Generate a single day's OHLCV data using GBM.

        The opening gap and intraday high/low are also driven by the
        GBM volatility parameter to maintain consistent dynamics.

        Returns:
            (open, high, low, close, volume)
        """
        volume_base = self.config["volume_base"]

        # Opening price: GBM step from previous close (overnight gap)
        gap_diffusion = self.daily_diffusion * 0.5  # overnight ~ half-day vol
        z_gap = random.gauss(0, 1)
        open_price = self.current_price * math.exp(gap_diffusion * z_gap)

        # Closing price: GBM step from open (intraday move)
        close_price = self._gbm_step(open_price)

        # Intraday high/low: extend range by intraday volatility
        intraday_sigma = abs(random.gauss(self.daily_diffusion, self.daily_diffusion / 3))
        intraday_sigma = max(0.001, intraday_sigma)

        intraday_range = abs(random.gauss(0, intraday_sigma))
        high_price = max(open_price, close_price) * (1 + intraday_range)
        low_price = min(open_price, close_price) * (1 - intraday_range)

        # Enforce OHLC constraints
        high_price = max(high_price, open_price, close_price)
        low_price = min(low_price, open_price, close_price)
        low_price = max(low_price, 0.01)  # Floor at 1 cent

        # Volume: log-normal distribution (consistent with empirical finance)
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
        Generate dividend payment (quarterly probability at month-end).

        Args:
            current_date: Current date
            close_price: Closing price

        Returns:
            Dividend amount (0.0 if none)
        """
        if current_date.month % 3 != 0 or current_date.day < 28:
            return 0.0

        if random.random() < 0.3:  # 30% probability of payment
            dividend = close_price * self.config["dividend_yield"] / 4
            self.cumulative_dividend += dividend
            return round(dividend, 4)

        return 0.0

    def generate_split(self) -> float:
        """
        Generate stock split (rare event).

        Returns:
            Split coefficient (1.0 = no split)
        """
        if random.random() < self.config["split_probability"]:
            split = random.choice([2.0, 3.0, 0.5])  # 2:1, 3:1, or 1:2
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
        Generate full price history using GBM simulation.

        Each day's close price feeds into the next day's open via the
        GBM process, producing realistic path-dependent price trajectories
        with log-normally distributed returns.

        Args:
            symbol: Stock ticker
            start_date: Start date (inclusive)
            end_date: End date (inclusive)

        Returns:
            List of daily records (most recent first)
        """
        records = []
        current_date = start_date

        while current_date <= end_date:
            # Skip weekends
            if current_date.weekday() >= 5:
                current_date += timedelta(days=1)
                continue

            # Generate OHLCV via GBM
            open_p, high, low, close_p, volume = self.generate_daily_ohlcv()

            # Dividends
            dividend = self.generate_dividend(current_date, close_p)

            # Stock splits
            split = self.generate_split()
            if split != 1.0:
                open_p /= split
                high /= split
                low /= split
                close_p /= split
                volume = int(volume * split)

            # Adjusted close accounts for dividends
            adjusted_close = close_p - dividend

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

            # Update current price for next day's GBM step
            self.current_price = close_p

            current_date += timedelta(days=1)

        # Most recent date first
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
