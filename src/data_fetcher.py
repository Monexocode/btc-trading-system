#!/usr/bin/env python3
"""
Data Fetcher for BTC Trading System
Fetches real market data from Binance, yfinance, CoinGecko, and Deribit.
Velo OI/CVD fields are left None — patched separately via Make.com.
"""

import requests
import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List


# --------------------------------------------------------------------------- #
# Constants
# --------------------------------------------------------------------------- #
BINANCE_SPOT = "https://api.binance.com"
BINANCE_FUTURES = "https://fapi.binance.com"
COINGECKO = "https://api.coingecko.com/api/v3"
DERIBIT = "https://www.deribit.com/api/v2"

HEADERS = {
    "User-Agent": "btc-trading-system/6.0 (github.com/Monexocode/btc-trading-system)"
}

# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #

def _get(url: str, params: dict = None, timeout: int = 10) -> Any:
    """Simple GET with error handling; returns parsed JSON."""
    resp = requests.get(url, params=params, headers=HEADERS, timeout=timeout)
    resp.raise_for_status()
    return resp.json()


def _ema(series: pd.Series, period: int) -> pd.Series:
    return series.ewm(span=period, adjust=False).mean()


def _atr(high: pd.Series, low: pd.Series, close: pd.Series, period: int = 20) -> pd.Series:
    tr = pd.concat([
        high - low,
        (high - close.shift()).abs(),
        (low - close.shift()).abs(),
    ], axis=1).max(axis=1)
    return tr.ewm(span=period, adjust=False).mean()


def _keltner(close: pd.Series, high: pd.Series, low: pd.Series,
             ema_period: int = 20, atr_period: int = 20,
             mult: float = 2.0):
    mid = _ema(close, ema_period)
    atr = _atr(high, low, close, atr_period)
    return mid + mult * atr, mid - mult * atr


def _bollinger(close: pd.Series, period: int = 20, std_dev: float = 2.0):
    mid = close.rolling(period).mean()
    std = close.rolling(period).std()
    return mid + std_dev * std, mid - std_dev * std


def _vwap(df: pd.DataFrame) -> pd.Series:
    """Session VWAP from OHLCV dataframe with columns open/high/low/close/volume."""
    tp = (df["high"] + df["low"] + df["close"]) / 3
    cum_tpv = (tp * df["volume"]).cumsum()
    cum_vol = df["volume"].cumsum()
    return cum_tpv / cum_vol


def _poc_vah_val(df: pd.DataFrame, num_bins: int = 50):
    """
    Approximate Point of Control, Value Area High/Low from price-volume.
    Uses 70% of total volume as value area definition.
    """
    low_price = df["low"].min()
    high_price = df["high"].max()
    bins = np.linspace(low_price, high_price, num_bins + 1)
    bin_centers = (bins[:-1] + bins[1:]) / 2

    vol_per_bin = np.zeros(num_bins)
    for _, row in df.iterrows():
        in_range = (bin_centers >= row["low"]) & (bin_centers <= row["high"])
        count = in_range.sum()
        if count > 0:
            vol_per_bin[in_range] += row["volume"] / count

    poc_idx = int(np.argmax(vol_per_bin))
    poc = float(bin_centers[poc_idx])

    total_vol = vol_per_bin.sum()
    target = total_vol * 0.70
    accumulated = vol_per_bin[poc_idx]
    lo_idx = hi_idx = poc_idx

    while accumulated < target and (lo_idx > 0 or hi_idx < num_bins - 1):
        lo_next = vol_per_bin[lo_idx - 1] if lo_idx > 0 else -1
        hi_next = vol_per_bin[hi_idx + 1] if hi_idx < num_bins - 1 else -1
        if lo_next >= hi_next and lo_idx > 0:
            lo_idx -= 1
            accumulated += lo_next
        elif hi_idx < num_bins - 1:
            hi_idx += 1
            accumulated += hi_next
        else:
            break

    return poc, float(bin_centers[hi_idx]), float(bin_centers[lo_idx])


# --------------------------------------------------------------------------- #
# DataFetcher
# --------------------------------------------------------------------------- #

class DataFetcher:
    """Fetches real market data from Binance, yfinance, CoinGecko, Deribit."""

    def __init__(self):
        self.last_fetch: Optional[datetime] = None
        self.cached_data: Optional[Dict] = None
        self.cache_duration = timedelta(minutes=5)
        self._prev_btc_price: Optional[float] = None
        self._prev_oi: Optional[float] = None

    # ------------------------------------------------------------------ #
    # Binance
    # ------------------------------------------------------------------ #

    def fetch_btc_price(self) -> float:
        data = _get(f"{BINANCE_SPOT}/api/v3/ticker/price", {"symbol": "BTCUSDT"})
        return float(data["price"])

    def fetch_funding_rate(self) -> float:
        data = _get(f"{BINANCE_FUTURES}/fapi/v1/fundingRate",
                    {"symbol": "BTCUSDT", "limit": 1})
        return float(data[0]["fundingRate"]) * 100  # as percentage

    def fetch_open_interest(self) -> Dict[str, Any]:
        """
        Fetches Binance perpetual OI. Velo OI (more comprehensive) is patched
        separately via Make.com.
        """
        data = _get(f"{BINANCE_FUTURES}/fapi/v1/openInterest", {"symbol": "BTCUSDT"})
        oi_btc = float(data["openInterest"])
        price = self.fetch_btc_price()
        oi_usd = oi_btc * price / 1e9  # in billions USD
        return {
            "total": round(oi_usd, 2),
            "cme": None,           # patched by Make.com via Velo
            "ratio": None,
            "raw_btc": oi_btc,
        }

    def fetch_ohlcv(self, interval: str = "15m", limit: int = 200) -> pd.DataFrame:
        """
        Fetch recent OHLCV candles from Binance spot.
        Returns a DataFrame with columns: open, high, low, close, volume.
        """
        data = _get(f"{BINANCE_SPOT}/api/v3/klines", {
            "symbol": "BTCUSDT",
            "interval": interval,
            "limit": limit,
        })
        df = pd.DataFrame(data, columns=[
            "open_time", "open", "high", "low", "close", "volume",
            "close_time", "quote_volume", "trades",
            "taker_buy_base", "taker_buy_quote", "ignore"
        ])
        for col in ["open", "high", "low", "close", "volume"]:
            df[col] = df[col].astype(float)
        df["open_time"] = pd.to_datetime(df["open_time"], unit="ms")
        df.set_index("open_time", inplace=True)
        return df[["open", "high", "low", "close", "volume"]]

    # ------------------------------------------------------------------ #
    # Technical indicators from OHLCV
    # ------------------------------------------------------------------ #

    def compute_technicals(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Compute EMA, VWAP, Keltner, Bollinger, Squeeze, POC/VAH/VAL
        from a 15m OHLCV DataFrame.
        """
        close = df["close"]
        high = df["high"]
        low = df["low"]

        ema_20 = _ema(close, 20)
        ema_50 = _ema(close, 50)
        ema_200 = _ema(close, 200)

        current_close = close.iloc[-1]
        cur_ema20 = ema_20.iloc[-1]
        cur_ema50 = ema_50.iloc[-1]
        cur_ema200 = ema_200.iloc[-1]

        if current_close > cur_ema20 > cur_ema50:
            ema_trend = "bullish"
        elif current_close < cur_ema20 < cur_ema50:
            ema_trend = "bearish"
        else:
            ema_trend = "neutral"

        vwap_series = _vwap(df)
        vwap = float(vwap_series.iloc[-1])
        vwap_position = "above" if current_close > vwap else "below"

        kc_upper_s, kc_lower_s = _keltner(close, high, low)
        bb_upper_s, bb_lower_s = _bollinger(close)

        kc_upper = float(kc_upper_s.iloc[-1])
        kc_lower = float(kc_lower_s.iloc[-1])
        bb_upper = float(bb_upper_s.iloc[-1])
        bb_lower = float(bb_lower_s.iloc[-1])

        squeeze = (bb_upper < kc_upper) and (bb_lower > kc_lower)

        kc_position = "inside" if kc_lower < current_close < kc_upper else "outside"
        bb_position = "inside" if bb_lower < current_close < bb_upper else "outside"

        # POC / VAH / VAL from last 100 bars
        poc, vah, val = _poc_vah_val(df.tail(100))

        return {
            "ema_20": round(cur_ema20, 2),
            "ema_50": round(cur_ema50, 2),
            "ema_200": round(cur_ema200, 2),
            "ema_trend": ema_trend,
            "vwap": round(vwap, 2),
            "vwap_position": vwap_position,
            "kc_upper": round(kc_upper, 2),
            "kc_lower": round(kc_lower, 2),
            "bb_upper": round(bb_upper, 2),
            "bb_lower": round(bb_lower, 2),
            "squeeze": squeeze,
            "kc_position": kc_position,
            "bb_position": bb_position,
            "poc": round(poc, 2),
            "vah": round(vah, 2),
            "val": round(val, 2),
        }

    # ------------------------------------------------------------------ #
    # TradFi via yfinance
    # ------------------------------------------------------------------ #

    def fetch_tradfi(self) -> Dict[str, Optional[float]]:
        """
        Fetch ES (S&P futures), NQ (Nasdaq futures), DXY, GOLD, VIX.
        Returns latest close for each; None on failure.
        """
        tickers = {
            "es": "ES=F",
            "nq": "NQ=F",
            "dxy": "DX-Y.NYB",
            "gold": "GC=F",
            "vix": "^VIX",
        }
        result: Dict[str, Optional[float]] = {}
        for key, symbol in tickers.items():
            try:
                ticker = yf.Ticker(symbol)
                hist = ticker.history(period="2d", interval="1d")
                if not hist.empty:
                    result[key] = round(float(hist["Close"].iloc[-1]), 4)
                else:
                    result[key] = None
            except Exception:
                result[key] = None
        return result

    # ------------------------------------------------------------------ #
    # CoinGecko
    # ------------------------------------------------------------------ #

    def fetch_crypto_market(self) -> Dict[str, Optional[float]]:
        """BTC dominance and stablecoin market cap from CoinGecko /global."""
        try:
            data = _get(f"{COINGECKO}/global", timeout=15)
            gdata = data.get("data", {})
            btc_d = gdata.get("market_cap_percentage", {}).get("btc")
            total_mc = gdata.get("total_market_cap", {}).get("usd")
            stablecoin_mc = None
            if total_mc:
                stable_pct = (
                    gdata.get("market_cap_percentage", {}).get("usdt", 0) +
                    gdata.get("market_cap_percentage", {}).get("usdc", 0) +
                    gdata.get("market_cap_percentage", {}).get("busd", 0)
                )
                stablecoin_mc = round(total_mc * stable_pct / 100 / 1e9, 2)  # billions
            return {
                "btc_dominance": round(float(btc_d), 2) if btc_d else None,
                "stablecoin_supply_b": stablecoin_mc,
            }
        except Exception:
            return {"btc_dominance": None, "stablecoin_supply_b": None}

    # ------------------------------------------------------------------ #
    # Deribit (DVOL — BTC implied vol index)
    # ------------------------------------------------------------------ #

    def fetch_bviv(self) -> Optional[float]:
        """Fetch Deribit DVOL (BTC implied vol), a proxy for BVIV."""
        try:
            data = _get(
                f"{DERIBIT}/public/get_index_price",
                {"index_name": "btcdvol"},
                timeout=10,
            )
            return round(float(data["result"]["index_price"]), 2)
        except Exception:
            return None

    # ------------------------------------------------------------------ #
    # Liquidations (Velo-only, patched via Make.com)
    # ------------------------------------------------------------------ #

    def fetch_liquidations(self) -> Dict[str, Optional[float]]:
        """
        True liquidation data requires Velo.xyz — patched via Make.com.
        Returns None for all fields.
        """
        return {
            "total_24h": None,
            "longs": None,
            "shorts": None,
            "ratio": None,
        }

    # ------------------------------------------------------------------ #
    # CVD (Velo-only, patched via Make.com)
    # ------------------------------------------------------------------ #

    def fetch_cvd(self) -> Dict[str, Optional[float]]:
        """
        CVD data comes from Velo.xyz — patched via Make.com.
        Returns None so scoring_engine handles gracefully.
        """
        return {"futures": None, "spot": None}

    # ------------------------------------------------------------------ #
    # ETF flow
    # ------------------------------------------------------------------ #

    def fetch_etf_flow(self) -> Optional[float]:
        """ETF net flow requires a paid source. Skipped — returns None."""
        return None

    # ------------------------------------------------------------------ #
    # Trend determination
    # ------------------------------------------------------------------ #

    def determine_trends(
        self,
        current_price: float,
        current_oi: Optional[float],
    ) -> Dict[str, str]:
        """
        Compare current values to cached previous values to derive trends.
        Falls back to 'neutral' on first run.
        """
        price_trend = "neutral"
        oi_trend = "neutral"

        if self._prev_btc_price is not None:
            diff = current_price - self._prev_btc_price
            if diff > current_price * 0.001:
                price_trend = "up"
            elif diff < -current_price * 0.001:
                price_trend = "down"

        if self._prev_oi is not None and current_oi is not None:
            diff = current_oi - self._prev_oi
            if diff > 0.05:
                oi_trend = "up"
            elif diff < -0.05:
                oi_trend = "down"

        self._prev_btc_price = current_price
        if current_oi is not None:
            self._prev_oi = current_oi

        return {"oi_trend": oi_trend, "price_trend": price_trend}

    # ------------------------------------------------------------------ #
    # Main entry point
    # ------------------------------------------------------------------ #

    def fetch_all_data(self) -> Dict[str, Any]:
        """
        Fetch all market data. Uses a 5-minute cache to avoid hammering APIs
        on repeated calls within the same pipeline run.
        """
        if (
            self.cached_data
            and self.last_fetch
            and datetime.now() - self.last_fetch < self.cache_duration
        ):
            print("   Using cached data...")
            return self.cached_data

        print("   Fetching BTC price from Binance...")
        btc_price = self.fetch_btc_price()

        print("   Fetching OI from Binance futures...")
        oi_data = self.fetch_open_interest()

        print("   Fetching 15m OHLCV from Binance...")
        ohlcv = self.fetch_ohlcv(interval="15m", limit=200)

        print("   Computing technicals (EMA/VWAP/KC/BB/POC)...")
        technicals = self.compute_technicals(ohlcv)

        print("   Fetching funding rate from Binance...")
        funding = self.fetch_funding_rate()

        print("   Fetching TradFi data (yfinance: ES/NQ/DXY/GOLD/VIX)...")
        tradfi = self.fetch_tradfi()

        print("   Fetching crypto market data from CoinGecko...")
        crypto_market = self.fetch_crypto_market()

        print("   Fetching DVOL from Deribit...")
        bviv = self.fetch_bviv()

        liquidations = self.fetch_liquidations()   # None — Velo via Make.com
        cvd = self.fetch_cvd()                     # None — Velo via Make.com
        etf_flow = self.fetch_etf_flow()           # None — no free source

        trends = self.determine_trends(btc_price, oi_data["total"])

        data: Dict[str, Any] = {
            "timestamp": datetime.now().isoformat(),
            "btc_price": round(btc_price, 2),

            # Open Interest (Binance perp; Velo CME patched separately)
            "oi_total": oi_data["total"],
            "oi_cme": oi_data["cme"],
            "oi_cme_ratio": oi_data["ratio"],

            # Funding
            "funding_rate": round(funding, 6),

            # Liquidations (None until Velo patch)
            "liquidations_24h": liquidations["total_24h"],
            "liquidations_longs": liquidations["longs"],
            "liquidations_shorts": liquidations["shorts"],
            "liquidation_ratio": liquidations["ratio"],

            # CVD (None until Velo patch)
            "cvd_futures": cvd["futures"],
            "cvd_spot": cvd["spot"],

            # ETF flow
            "etf_flow": etf_flow,

            # Volume profile
            "poc": technicals["poc"],
            "vah": technicals["vah"],
            "val": technicals["val"],

            # Technical indicators
            "vwap": technicals["vwap"],
            "vwap_position": technicals["vwap_position"],
            "ema_trend": technicals["ema_trend"],
            "ema_20": technicals["ema_20"],
            "ema_50": technicals["ema_50"],
            "ema_200": technicals["ema_200"],
            "squeeze": technicals["squeeze"],
            "kc_position": technicals["kc_position"],
            "bb_position": technicals["bb_position"],

            # TradFi
            "es": tradfi.get("es"),
            "nq": tradfi.get("nq"),
            "dxy": tradfi.get("dxy"),
            "gold": tradfi.get("gold"),
            "vix": tradfi.get("vix"),

            # Crypto market
            "btc_dominance": crypto_market.get("btc_dominance"),
            "stablecoin_supply_b": crypto_market.get("stablecoin_supply_b"),

            # Implied vol
            "bviv": bviv,
        }

        data.update(trends)

        self.cached_data = data
        self.last_fetch = datetime.now()
        return data
