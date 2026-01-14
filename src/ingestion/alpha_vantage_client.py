"""
Alpha Vantage API Client
========================
Production-grade client for fetching financial market data from Alpha Vantage API.
Implements rate limiting, retry logic, and proper error handling.
"""

import time
import logging
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field
from datetime import datetime, date
import json

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

import sys
sys.path.insert(0, "/opt/airflow/dags")

from config.settings import get_settings

logger = logging.getLogger(__name__)


class AlphaVantageError(Exception):
    """Base exception for Alpha Vantage API errors."""
    pass


class RateLimitError(AlphaVantageError):
    """Raised when API rate limit is exceeded."""
    pass


class InvalidSymbolError(AlphaVantageError):
    """Raised when an invalid stock symbol is provided."""
    pass


@dataclass
class RateLimiter:
    """
    Simple rate limiter for API calls.
    Alpha Vantage free tier: 5 requests/minute, 500 requests/day.
    """
    requests_per_minute: int = 5
    requests_per_day: int = 500
    _minute_requests: List[float] = field(default_factory=list)
    _day_requests: List[float] = field(default_factory=list)
    
    def wait_if_needed(self) -> None:
        """Block if rate limit would be exceeded."""
        now = time.time()
        
        # Clean old requests from tracking
        minute_ago = now - 60
        day_ago = now - 86400
        
        self._minute_requests = [t for t in self._minute_requests if t > minute_ago]
        self._day_requests = [t for t in self._day_requests if t > day_ago]
        
        # Check daily limit
        if len(self._day_requests) >= self.requests_per_day:
            raise RateLimitError("Daily API limit reached (500 requests)")
        
        # Check minute limit and wait if needed
        if len(self._minute_requests) >= self.requests_per_minute:
            wait_time = 60 - (now - self._minute_requests[0]) + 1
            logger.info(f"Rate limit approaching, waiting {wait_time:.1f}s")
            time.sleep(wait_time)
            self._minute_requests = []
        
        # Record this request
        self._minute_requests.append(now)
        self._day_requests.append(now)


class AlphaVantageClient:
    """
    Production-ready Alpha Vantage API client.
    
    Features:
    - Automatic rate limiting
    - Retry with exponential backoff
    - Response validation
    - Proper error handling
    
    Example:
        >>> client = AlphaVantageClient()
        >>> data = client.get_daily_adjusted("AAPL")
        >>> print(data["Meta Data"])
    """
    
    def __init__(
        self,
        api_key: Optional[str] = None,
        base_url: Optional[str] = None,
        timeout: int = 30,
        max_retries: int = 3,
    ):
        """
        Initialize the Alpha Vantage client.
        
        Args:
            api_key: API key (defaults to settings/secrets manager)
            base_url: API base URL
            timeout: Request timeout in seconds
            max_retries: Maximum retry attempts
        """
        self.settings = get_settings()
        self.api_key = api_key or self.settings.alpha_vantage.get_api_key()
        self.base_url = base_url or self.settings.alpha_vantage.base_url
        self.timeout = timeout
        
        # Configure rate limiter
        self.rate_limiter = RateLimiter(
            requests_per_minute=self.settings.alpha_vantage.rate_limit_per_minute,
            requests_per_day=self.settings.alpha_vantage.rate_limit_per_day,
        )
        
        # Configure session with retry strategy
        self.session = self._create_session(max_retries)
    
    def _create_session(self, max_retries: int) -> requests.Session:
        """Create a requests session with retry configuration."""
        session = requests.Session()
        
        retry_strategy = Retry(
            total=max_retries,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        return session
    
    def _make_request(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Make an API request with rate limiting and error handling.
        
        Args:
            params: Query parameters for the API call
            
        Returns:
            Parsed JSON response
            
        Raises:
            AlphaVantageError: On API errors
        """
        # Apply rate limiting
        self.rate_limiter.wait_if_needed()
        
        # Add API key to params
        params["apikey"] = self.api_key
        
        try:
            response = self.session.get(
                self.base_url,
                params=params,
                timeout=self.timeout,
            )
            response.raise_for_status()
            
            data = response.json()
            
            # Check for API-level errors
            if "Error Message" in data:
                raise InvalidSymbolError(data["Error Message"])
            
            if "Note" in data and "call frequency" in data["Note"].lower():
                raise RateLimitError(data["Note"])
            
            if "Information" in data:
                logger.warning(f"API Information: {data['Information']}")
            
            return data
            
        except requests.exceptions.Timeout:
            raise AlphaVantageError(f"Request timeout after {self.timeout}s")
        except requests.exceptions.RequestException as e:
            raise AlphaVantageError(f"Request failed: {e}")
    
    def get_daily_adjusted(
        self,
        symbol: str,
        outputsize: str = "compact",
    ) -> Dict[str, Any]:
        """
        Fetch daily adjusted time series data for a stock.
        
        Args:
            symbol: Stock ticker symbol (e.g., "AAPL")
            outputsize: "compact" (last 100 days) or "full" (20+ years)
            
        Returns:
            Dictionary containing:
            - Meta Data: Symbol info and last refresh time
            - Time Series (Daily): OHLCV data keyed by date
            
        Example:
            >>> data = client.get_daily_adjusted("AAPL")
            >>> latest = list(data["Time Series (Daily)"].items())[0]
            >>> print(f"Latest close: {latest[1]['4. close']}")
        """
        logger.info(f"Fetching daily adjusted data for {symbol}")
        
        params = {
            "function": "TIME_SERIES_DAILY_ADJUSTED",
            "symbol": symbol.upper(),
            "outputsize": outputsize,
            "datatype": "json",
        }
        
        data = self._make_request(params)
        
        # Validate response structure
        if "Meta Data" not in data or "Time Series (Daily)" not in data:
            raise AlphaVantageError(
                f"Unexpected response structure for {symbol}: {list(data.keys())}"
            )
        
        # Transform to standardized format
        return self._transform_daily_data(data, symbol)
    
    def _transform_daily_data(
        self,
        raw_data: Dict[str, Any],
        symbol: str,
    ) -> Dict[str, Any]:
        """
        Transform Alpha Vantage response to standardized format.
        
        Args:
            raw_data: Raw API response
            symbol: Stock symbol
            
        Returns:
            Standardized data structure
        """
        meta = raw_data["Meta Data"]
        time_series = raw_data["Time Series (Daily)"]
        
        records = []
        for date_str, values in time_series.items():
            record = {
                "symbol": symbol.upper(),
                "timestamp": date_str,
                "open_price": float(values["1. open"]),
                "high_price": float(values["2. high"]),
                "low_price": float(values["3. low"]),
                "close_price": float(values["4. close"]),
                "adjusted_close": float(values["5. adjusted close"]),
                "volume": int(values["6. volume"]),
                "dividend_amount": float(values["7. dividend amount"]),
                "split_coefficient": float(values["8. split coefficient"]),
            }
            records.append(record)
        
        return {
            "meta": {
                "symbol": meta["2. Symbol"],
                "last_refreshed": meta["3. Last Refreshed"],
                "output_size": meta.get("4. Output Size", "unknown"),
                "time_zone": meta.get("5. Time Zone", "US/Eastern"),
                "ingestion_timestamp": datetime.utcnow().isoformat(),
            },
            "data": records,
        }
    
    def get_intraday(
        self,
        symbol: str,
        interval: str = "5min",
        outputsize: str = "compact",
    ) -> Dict[str, Any]:
        """
        Fetch intraday time series data.
        
        Args:
            symbol: Stock ticker symbol
            interval: Time interval (1min, 5min, 15min, 30min, 60min)
            outputsize: "compact" or "full"
            
        Returns:
            Intraday OHLCV data
        """
        logger.info(f"Fetching intraday ({interval}) data for {symbol}")
        
        valid_intervals = ["1min", "5min", "15min", "30min", "60min"]
        if interval not in valid_intervals:
            raise ValueError(f"Invalid interval. Must be one of: {valid_intervals}")
        
        params = {
            "function": "TIME_SERIES_INTRADAY",
            "symbol": symbol.upper(),
            "interval": interval,
            "outputsize": outputsize,
            "datatype": "json",
        }
        
        return self._make_request(params)
    
    def get_quote(self, symbol: str) -> Dict[str, Any]:
        """
        Fetch real-time quote for a stock.
        
        Args:
            symbol: Stock ticker symbol
            
        Returns:
            Current quote data
        """
        logger.info(f"Fetching quote for {symbol}")
        
        params = {
            "function": "GLOBAL_QUOTE",
            "symbol": symbol.upper(),
            "datatype": "json",
        }
        
        data = self._make_request(params)
        
        if "Global Quote" not in data or not data["Global Quote"]:
            raise InvalidSymbolError(f"No quote data found for {symbol}")
        
        quote = data["Global Quote"]
        return {
            "symbol": quote.get("01. symbol", symbol),
            "open": float(quote.get("02. open", 0)),
            "high": float(quote.get("03. high", 0)),
            "low": float(quote.get("04. low", 0)),
            "price": float(quote.get("05. price", 0)),
            "volume": int(quote.get("06. volume", 0)),
            "latest_trading_day": quote.get("07. latest trading day", ""),
            "previous_close": float(quote.get("08. previous close", 0)),
            "change": float(quote.get("09. change", 0)),
            "change_percent": quote.get("10. change percent", "0%"),
        }
    
    def get_company_overview(self, symbol: str) -> Dict[str, Any]:
        """
        Fetch company fundamental data.
        
        Args:
            symbol: Stock ticker symbol
            
        Returns:
            Company overview including sector, industry, market cap, etc.
        """
        logger.info(f"Fetching company overview for {symbol}")
        
        params = {
            "function": "OVERVIEW",
            "symbol": symbol.upper(),
        }
        
        data = self._make_request(params)
        
        if not data or "Symbol" not in data:
            raise InvalidSymbolError(f"No company data found for {symbol}")
        
        return data
    
    def batch_fetch(
        self,
        symbols: List[str],
        function: str = "daily_adjusted",
        **kwargs,
    ) -> Dict[str, Dict[str, Any]]:
        """
        Fetch data for multiple symbols.
        
        Args:
            symbols: List of stock ticker symbols
            function: API function to call
            **kwargs: Additional arguments for the function
            
        Returns:
            Dictionary mapping symbols to their data
        """
        results = {}
        errors = {}
        
        function_map = {
            "daily_adjusted": self.get_daily_adjusted,
            "intraday": self.get_intraday,
            "quote": self.get_quote,
            "overview": self.get_company_overview,
        }
        
        if function not in function_map:
            raise ValueError(f"Unknown function: {function}")
        
        fetch_func = function_map[function]
        
        for symbol in symbols:
            try:
                results[symbol] = fetch_func(symbol, **kwargs)
                logger.info(f"Successfully fetched {function} data for {symbol}")
            except Exception as e:
                logger.error(f"Failed to fetch {function} data for {symbol}: {e}")
                errors[symbol] = str(e)
        
        if errors:
            logger.warning(f"Batch fetch completed with {len(errors)} errors: {errors}")
        
        return {
            "successful": results,
            "failed": errors,
            "summary": {
                "total": len(symbols),
                "successful": len(results),
                "failed": len(errors),
            }
        }


# Convenience function for quick testing
def test_client():
    """Quick test of the client functionality."""
    client = AlphaVantageClient()
    
    # Test with a sample symbol
    try:
        data = client.get_daily_adjusted("IBM", outputsize="compact")
        print(f"Fetched {len(data['data'])} records for {data['meta']['symbol']}")
        print(f"Latest record: {data['data'][0]}")
    except AlphaVantageError as e:
        print(f"API Error: {e}")


if __name__ == "__main__":
    test_client()
