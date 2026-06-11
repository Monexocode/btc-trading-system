#!/usr/bin/env python3
"""
Data Fetcher for BTC Trading System.

Spot price / OHLCV   : api.binance.us (US-compliant, no geo-block)
OI + Funding         : Coinalyze REST (fapi.binance.com is geo-blocked on GitHub Actions US runners)
CVD futures klines   : fapi.binance.com in try/except (non-fatal if blocked)
TradFi               : yfinance
Crypto market        : CoinGecko
Implied vol          : Deribit DVOL
Liquidations / ETF   : None (Velo patched separately via Make.com)
"""

import os
import requests
import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, Any, Optional


# --------------------------------------------------------------------------- #
# Constants
# --------------------------------------------------------------------------- #
BINANCE_SPOT    = "https://api.binance.us"    # US-compliant (no geo-block)
BINANCE_FUTURES = "https://fapi.binance.com"  # 451 from GitHub Actions; only used in try/except
COINGECKO       = "https://api.coingecko.com/api/v3"
DERIBIT         = "https://www.deribit.com/api/v2"
COINALYZE       = "https://api.coinalyze.net/v1"
COINALYZE_KEY   = os.environ.get("COINALYZE_API_KEY")  # set as GitHub Actions secret

HEADERS = {
    "User-Agent": "btc-trading-system/6.0 (github.com/Monexocode/btc-trading-system)"
}

# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #

def _get(url: str, params: dict = None, timeout: int = 10) -> Any:
    resp = requests.get(url, params=params, headers=HEADERS, timeout=timeout)
    resp.raise_for_status()
    return resp.json()


def _coinalyze(endpoint: str, params: dict = None, timeout: int = 15) -> Any:
    """Coinalyze REST call; returns None on any error and logs the failure."""
    if not COINALYZE_KEY:
        return None
    p = dict(params or {})
    p["api_key"] = COINALYZE_KEY
    try:
        result = _get(f"{COINALYZE}/{endpoint}", params=p, timeout=timeout)
        if isinstance(result, list) and len(result) == 0:
            print(f"   Coinalyze {endpoint}: empty list — check symbol format or API plan")
        return result
    except Exception as e:
        print(f"   Coinalyze {endpoint} failed: {e}")
        return None


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


def _vwap_with_daily_bands(df: pd.DataFrame) -> dict:
    df = df.copy()
    df["_date"] = df.index.date
    tp = (df["high"] + df["low"] + df["close"]) / 3
    df["_tpv"] = tp * df["volume"]
    df["_cum_tpv"] = df.groupby("_date")["_tpv"].cumsum()
    df["_cum_vol"] = df.groupby("_date")["volume"].cumsum()
    df["_vwap"] = df["_cum_tpv"] / df["_cum_vol"]
    sq_dev   = (df["close"] - df["_vwap"]) ** 2
    vwap_std = sq_dev.rolling(14).mean().apply(np.sqrt)
    vwap_upper = df["_vwap"] + vwap_std
    vwap_lower = df["_vwap"] - vwap_std
    cur_vwap  = float(df["_vwap"].iloc[-1])
    cur_upper = float(vwap_upper.iloc[-1])
    cur_lower = float(vwap_lower.iloc[-1])
    cur_close = float(df["close"].iloc[-1])
    band_range = cur_upper - cur_lower
    pos_pct = (cur_close - cur_lower) / band_range * 100 if band_range > 0 else 50.0
    return {
        "vwap":            round(cur_vwap, 2),
        "vwap_upper_band": round(cur_upper, 2),
        "vwap_lower_band": round(cur_lower, 2),
        "vwap_pos_percent": round(pos_pct, 1),
    }


def _poc_vah_val(df: pd.DataFrame, num_bins: int = 50):
    low_price  = df["low"].min()
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
    total_vol   = vol_per_bin.sum()
    target      = total_vol * 0.70
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


def _compute_normal_box(df: pd.DataFrame, ema9: pd.Series, ema21: pd.Series) -> dict:
    close = df["close"].values
    high  = df["high"].values
    low   = df["low"].values
    e9    = ema9.values
    e21   = ema21.values
    box_high = np.nan
    box_low  = np.nan
    in_box   = False
    for i in range(len(df)):
        c = close[i]
        if c == 0:
            continue
        ema_diff_pct = abs(e9[i] - e21[i]) / c * 100
        ema_near = 0.2 <= ema_diff_pct <= 0.5
        if ema_near:
            if np.isnan(box_high):
                box_high = high[i]
                box_low  = low[i]
                in_box   = True
            else:
                box_high = max(box_high, high[i])
                box_low  = min(box_low, low[i])
        elif in_box:
            in_box   = False
            box_high = np.nan
            box_low  = np.nan
    cur_close = float(close[-1])
    if np.isnan(box_high):
        return {
            "box_high": None, "box_low": None,
            "box_break_up": False, "box_break_down": False,
            "near_box_top": False, "near_box_bottom": False,
        }
    return {
        "box_high":       round(float(box_high), 2),
        "box_low":        round(float(box_low), 2),
        "box_break_up":   cur_close > box_high,
        "box_break_down": cur_close < box_low,
        "near_box_top":   cur_close >= box_high * 0.998,
        "near_box_bottom": cur_close <= box_low * 1.002,
    }


def _compute_swing_levels(df: pd.DataFrame, lookback: int = 14) -> dict:
    high  = df["high"].values
    low   = df["low"].values
    close = df["close"].values
    n = len(df)
    last_pivot_high = None
    last_pivot_low  = None
    for i in range(n - 1 - lookback, lookback - 1, -1):
        if high[i] == max(high[max(0, i - lookback):i + lookback + 1]):
            last_pivot_high = float(high[i])
            break
    for i in range(n - 1 - lookback, lookback - 1, -1):
        if low[i] == min(low[max(0, i - lookback):i + lookback + 1]):
            last_pivot_low = float(low[i])
            break
    cur_close  = float(close[-1])
    prev_close = float(close[-2]) if n >= 2 else cur_close
    return {
        "swing_high": last_pivot_high,
        "swing_low":  last_pivot_low,
        "swing_high_break": last_pivot_high is not None and prev_close <= last_pivot_high < cur_close,
        "swing_low_break":  last_pivot_low  is not None and prev_close >= last_pivot_low  > cur_close,
    }


def _parse_klines(data) -> pd.DataFrame:
    """Parse raw Binance klines into DataFrame with taker_buy_base."""
    df = pd.DataFrame(data, columns=[
        "open_time", "open", "high", "low", "close", "volume",
        "close_time", "quote_volume", "trades",
        "taker_buy_base", "taker_buy_quote", "ignore"
    ])
    for col in ["open", "high", "low", "close", "volume", "taker_buy_base"]:
        df[col] = df[col].astype(float)
    df["open_time"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
    df.set_index("open_time", inplace=True)
    return df[["open", "high", "low", "close", "volume", "taker_buy_base"]]


# --------------------------------------------------------------------------- #
# DataFetcher
# --------------------------------------------------------------------------- #

class DataFetcher:
    """Fetches real market data. Spot from Binance.US, OI/funding from Coinalyze."""

    def __init__(self):
        self.last_fetch: Optional[datetime] = None
        self.cached_data: Optional[Dict] = None
        self.cache_duration = timedelta(minutes=5)
        self._prev_btc_price: Optional[float] = None
        self._prev_oi: Optional[float] = None

    # ------------------------------------------------------------------ #
    # Binance.US (spot — no geo-block)
    # ------------------------------------------------------------------ #

    def fetch_btc_price(self) -> float:
        data = _get(f"{BINANCE_SPOT}/api/v3/ticker/price", {"symbol": "BTCUSDT"})
        return float(data["price"])

    def fetch_ohlcv(self, interval: str = "15m", limit: int = 200) -> pd.DataFrame:
        """Binance.US spot klines with taker_buy_base for CVD spot."""
        data = _get(f"{BINANCE_SPOT}/api/v3/klines", {
            "symbol": "BTCUSDT", "interval": interval, "limit": limit,
        })
        return _parse_klines(data)

    def fetch_daily_weekly_levels(self) -> Dict[str, Optional[float]]:
        """PDH/PDL/PWH/PWL from Binance.US daily and weekly klines."""
        result: Dict[str, Optional[float]] = {
            "pdh": None, "pdl": None, "pwh": None, "pwl": None
        }
        try:
            daily = _get(f"{BINANCE_SPOT}/api/v3/klines", {
                "symbol": "BTCUSDT", "interval": "1d", "limit": 3,
            })
            result["pdh"] = float(daily[-2][2])
            result["pdl"] = float(daily[-2][3])
        except Exception:
            pass
        try:
            weekly = _get(f"{BINANCE_SPOT}/api/v3/klines", {
                "symbol": "BTCUSDT", "interval": "1w", "limit": 3,
            })
            result["pwh"] = float(weekly[-2][2])
            result["pwl"] = float(weekly[-2][3])
        except Exception:
            pass
        return result

    # ------------------------------------------------------------------ #
    # Binance Futures (non-fatal in try/except — 451 from GitHub Actions)
    # ------------------------------------------------------------------ #

    def fetch_futures_ohlcv(self, interval: str = "15m", limit: int = 200) -> pd.DataFrame:
        """Binance USDT-M perp klines. Caller wraps in try/except."""
        data = _get(f"{BINANCE_FUTURES}/fapi/v1/klines", {
            "symbol": "BTCUSDT", "interval": interval, "limit": limit,
        })
        return _parse_klines(data)

    # ------------------------------------------------------------------ #
    # Coinalyze (OI + Funding — works from GitHub Actions)
    # ------------------------------------------------------------------ #

    def fetch_open_interest(self) -> Dict[str, Any]:
        """OI from Coinalyze primary; returns None values if unavailable."""
        oi_usd  = None
        cme_usd = None

        raw = _coinalyze("open-interest", {"symbols": "BTCUSDT_PERP.6"})
        if raw and isinstance(raw, list) and len(raw) > 0:
            oi_usd = round(raw[0].get("value", 0) / 1e9, 2)

        cme = _coinalyze("open-interest", {"symbols": "BTC_USD.9"})
        if cme and isinstance(cme, list) and len(cme) > 0:
            cme_usd = round(cme[0].get("value", 0) / 1e9, 2)

        return {
            "total":   oi_usd,
            "cme":     cme_usd,
            "ratio":   None,
            "raw_btc": None,
        }

    def fetch_funding_rate(self) -> float:
        """Funding rate from Coinalyze. Returns 0.0 if unavailable."""
        # Note: correct endpoint is 'funding-rate', not 'current-funding-rate'
        raw = _coinalyze("funding-rate", {"symbols": "BTCUSDT_PERP.6"})
        if raw and isinstance(raw, list) and len(raw) > 0:
            return float(raw[0].get("value", 0)) * 100  # as percentage
        return 0.0

    # ------------------------------------------------------------------ #
    # Technical indicators from OHLCV
    # ------------------------------------------------------------------ #

    def compute_technicals(
        self,
        df: pd.DataFrame,
        daily_weekly: Optional[Dict] = None,
        futures_df: Optional[pd.DataFrame] = None,
    ) -> Dict[str, Any]:
        close  = df["close"]
        high   = df["high"]
        low    = df["low"]
        volume = df["volume"]

        ema9_s   = _ema(close, 9)
        ema21_s  = _ema(close, 21)
        ema50_s  = _ema(close, 50)
        ema200_s = _ema(close, 200)

        cur_close  = float(close.iloc[-1])
        cur_ema9   = float(ema9_s.iloc[-1])
        cur_ema21  = float(ema21_s.iloc[-1])
        cur_ema50  = float(ema50_s.iloc[-1])
        cur_ema200 = float(ema200_s.iloc[-1])

        if cur_ema9 >= cur_ema21 and cur_ema21 >= cur_ema50:
            ema_trend = "bullish"
        elif cur_ema9 < cur_ema21 and cur_ema21 < cur_ema50:
            ema_trend = "bearish"
        else:
            ema_trend = "neutral"

        vwap_data     = _vwap_with_daily_bands(df)
        vwap          = vwap_data["vwap"]
        vwap_position = "above" if cur_close > vwap else "below"

        kc_upper_s, kc_lower_s = _keltner(close, high, low)
        kc_upper = float(kc_upper_s.iloc[-1])
        kc_lower = float(kc_lower_s.iloc[-1])

        bb_upper_s, bb_lower_s = _bollinger(close)
        bb_upper = float(bb_upper_s.iloc[-1])
        bb_lower = float(bb_lower_s.iloc[-1])

        bb_width_s   = bb_upper_s - bb_lower_s
        bb_width_sma = bb_width_s.rolling(10).mean()
        bb_contracting  = float(bb_width_s.iloc[-1]) < float(bb_width_sma.iloc[-1])
        kc_range = kc_upper - kc_lower
        price_near_kc_edge = (
            cur_close > (kc_upper - kc_range * 0.20) or
            cur_close < (kc_lower + kc_range * 0.20)
        ) if kc_range > 0 else False
        squeeze = bb_contracting and price_near_kc_edge

        kc_position = "outside" if (cur_close > kc_upper or cur_close < kc_lower) else "inside"
        bb_position = "outside" if (cur_close > bb_upper or cur_close < bb_lower) else "inside"

        avg_vol      = float(volume.rolling(20).mean().iloc[-1])
        volume_spike = float(volume.iloc[-1]) >= avg_vol * 1.5

        cur_high  = float(high.iloc[-1])
        cur_low   = float(low.iloc[-1])
        candle_range = cur_high - cur_low
        if candle_range > 0:
            close_frac = (cur_close - cur_low) / candle_range
            if close_frac >= 0.70:
                candle_close_position = "upper"
            elif close_frac <= 0.30:
                candle_close_position = "lower"
            else:
                candle_close_position = "neutral"
        else:
            candle_close_position = "neutral"

        poc, vah, val = _poc_vah_val(df.tail(100))

        today    = df.index[-1].date()
        today_df = df[df.index.date == today]
        vah_touched_today = bool((today_df["high"] >= vah).any())
        val_touched_today = bool((today_df["low"]  <= val).any())

        box   = _compute_normal_box(df, ema9_s, ema21_s)
        swing = _compute_swing_levels(df, lookback=14)

        dw  = daily_weekly or {}
        pdh = dw.get("pdh")
        pdl = dw.get("pdl")
        pwh = dw.get("pwh")
        pwl = dw.get("pwl")

        prev_close = float(close.iloc[-2]) if len(close) >= 2 else cur_close
        pdh_break = pdh is not None and prev_close <= pdh < cur_close
        pdl_break = pdl is not None and prev_close >= pdl > cur_close
        pwh_break = pwh is not None and prev_close <= pwh < cur_close
        pwl_break = pwl is not None and prev_close >= pwl > cur_close

        cvd_futures = None
        cvd_spot    = None
        if futures_df is not None and "taker_buy_base" in futures_df.columns:
            f_delta = 2 * futures_df["taker_buy_base"] - futures_df["volume"]
            cvd_futures = round(float(f_delta.cumsum().iloc[-1]), 2)
        if "taker_buy_base" in df.columns:
            s_delta = 2 * df["taker_buy_base"] - df["volume"]
            cvd_spot = round(float(s_delta.cumsum().iloc[-1]), 2)

        return {
            "ema9":  round(cur_ema9, 2),   "ema21": round(cur_ema21, 2),
            "ema50": round(cur_ema50, 2),  "ema200": round(cur_ema200, 2),
            "ema_trend": ema_trend,
            "vwap": vwap,
            "vwap_upper_band":  vwap_data["vwap_upper_band"],
            "vwap_lower_band":  vwap_data["vwap_lower_band"],
            "vwap_pos_percent": vwap_data["vwap_pos_percent"],
            "vwap_position":    vwap_position,
            "kc_upper": round(kc_upper, 2), "kc_lower": round(kc_lower, 2),
            "bb_upper": round(bb_upper, 2), "bb_lower": round(bb_lower, 2),
            "squeeze":        squeeze,
            "kc_position":    kc_position,
            "bb_position":    bb_position,
            "volume_spike":   volume_spike,
            "candle_close_position": candle_close_position,
            "poc": round(poc, 2), "vah": round(vah, 2), "val": round(val, 2),
            "vah_touched_today": vah_touched_today,
            "val_touched_today": val_touched_today,
            "box_high":       box["box_high"],
            "box_low":        box["box_low"],
            "box_break_up":   box["box_break_up"],
            "box_break_down": box["box_break_down"],
            "near_box_top":   box["near_box_top"],
            "near_box_bottom": box["near_box_bottom"],
            "swing_high":       swing["swing_high"],
            "swing_low":        swing["swing_low"],
            "swing_high_break": swing["swing_high_break"],
            "swing_low_break":  swing["swing_low_break"],
            "pdh": pdh, "pdl": pdl, "pwh": pwh, "pwl": pwl,
            "pdh_break": pdh_break, "pdl_break": pdl_break,
            "pwh_break": pwh_break, "pwl_break": pwl_break,
            "cvd_futures": cvd_futures,
            "cvd_spot":    cvd_spot,
        }

    # ------------------------------------------------------------------ #
    # TradFi via yfinance
    # ------------------------------------------------------------------ #

    def fetch_tradfi(self) -> Dict[str, Optional[float]]:
        tickers = {
            "es":   "ES=F",
            "nq":   "NQ=F",
            "dxy":  "DX-Y.NYB",
            "gold": "GC=F",
            "vix":  "^VIX",
        }
        result: Dict[str, Optional[float]] = {}
        for key, symbol in tickers.items():
            try:
                ticker = yf.Ticker(symbol)
                hist   = ticker.history(period="2d", interval="1d")
                result[key] = round(float(hist["Close"].iloc[-1]), 4) if not hist.empty else None
            except Exception:
                result[key] = None
        return result

    # ------------------------------------------------------------------ #
    # CoinGecko
    # ------------------------------------------------------------------ #

    def fetch_crypto_market(self) -> Dict[str, Optional[float]]:
        try:
            data  = _get(f"{COINGECKO}/global", timeout=15)
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
                stablecoin_mc = round(total_mc * stable_pct / 100 / 1e9, 2)
            return {
                "btc_dominance":     round(float(btc_d), 2) if btc_d else None,
                "stablecoin_supply_b": stablecoin_mc,
            }
        except Exception:
            return {"btc_dominance": None, "stablecoin_supply_b": None}

    # ------------------------------------------------------------------ #
    # Deribit (DVOL)
    # ------------------------------------------------------------------ #

    def fetch_bviv(self) -> Optional[float]:
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
    # Velo-only sources
    # ------------------------------------------------------------------ #

    def fetch_liquidations(self) -> Dict[str, Optional[float]]:
        return {"total_24h": None, "longs": None, "shorts": None, "ratio": None}

    def fetch_etf_flow(self) -> Optional[float]:
        return None

    # ------------------------------------------------------------------ #
    # Trend determination
    # ------------------------------------------------------------------ #

    def determine_trends(
        self,
        current_price: float,
        current_oi: Optional[float],
    ) -> Dict[str, str]:
        price_trend = "neutral"
        oi_trend    = "neutral"

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
        if (
            self.cached_data
            and self.last_fetch
            and datetime.now() - self.last_fetch < self.cache_duration
        ):
            print("   Using cached data...")
            return self.cached_data

        print("   Fetching BTC price from Binance.US...")
        btc_price = self.fetch_btc_price()

        print("   Fetching OI from Coinalyze...")
        oi_data = self.fetch_open_interest()

        print("   Fetching 15m spot OHLCV from Binance.US...")
        ohlcv = self.fetch_ohlcv(interval="15m", limit=200)

        print("   Fetching 15m futures OHLCV (CVD, non-fatal if blocked)...")
        futures_ohlcv = None
        try:
            futures_ohlcv = self.fetch_futures_ohlcv(interval="15m", limit=200)
        except Exception as e:
            print(f"   Futures OHLCV skipped: {e}")

        print("   Fetching daily/weekly levels from Binance.US...")
        daily_weekly = self.fetch_daily_weekly_levels()

        print("   Computing technicals (EMA/VWAP/KC/BB/squeeze/box/swing/CVD)...")
        technicals = self.compute_technicals(ohlcv, daily_weekly, futures_df=futures_ohlcv)

        print("   Fetching funding rate from Coinalyze...")
        funding = self.fetch_funding_rate()

        print("   Fetching TradFi data (yfinance: ES/NQ/DXY/GOLD/VIX)...")
        tradfi = self.fetch_tradfi()

        print("   Fetching crypto market data from CoinGecko...")
        crypto_market = self.fetch_crypto_market()

        print("   Fetching DVOL from Deribit...")
        bviv = self.fetch_bviv()

        liquidations = self.fetch_liquidations()
        etf_flow     = self.fetch_etf_flow()

        trends = self.determine_trends(btc_price, oi_data["total"])

        data: Dict[str, Any] = {
            "timestamp": datetime.now().isoformat(),
            "btc_price": round(btc_price, 2),
            "oi_total":    oi_data["total"],
            "oi_cme":      oi_data["cme"],
            "oi_cme_ratio": oi_data["ratio"],
            "funding_rate": round(funding, 6),
            "liquidations_24h":   liquidations["total_24h"],
            "liquidation_ratio":  liquidations["ratio"],
            "cvd_futures": technicals.get("cvd_futures"),
            "cvd_spot":    technicals.get("cvd_spot"),
            "etf_flow": etf_flow,
            "poc": technicals["poc"],
            "vah": technicals["vah"],
            "val": technicals["val"],
            "vah_touched_today": technicals["vah_touched_today"],
            "val_touched_today": technicals["val_touched_today"],
            "vwap":            technicals["vwap"],
            "vwap_upper_band": technicals["vwap_upper_band"],
            "vwap_lower_band": technicals["vwap_lower_band"],
            "vwap_pos_percent": technicals["vwap_pos_percent"],
            "vwap_position":   technicals["vwap_position"],
            "ema_trend":       technicals["ema_trend"],
            "ema9":            technicals["ema9"],
            "ema21":           technicals["ema21"],
            "ema50":           technicals["ema50"],
            "ema200":          technicals["ema200"],
            "squeeze":         technicals["squeeze"],
            "kc_upper":        technicals["kc_upper"],
            "kc_lower":        technicals["kc_lower"],
            "bb_upper":        technicals["bb_upper"],
            "bb_lower":        technicals["bb_lower"],
            "kc_position":     technicals["kc_position"],
            "bb_position":     technicals["bb_position"],
            "volume_spike":    technicals["volume_spike"],
            "candle_close_position": technicals["candle_close_position"],
            "box_high":       technicals["box_high"],
            "box_low":        technicals["box_low"],
            "box_break_up":   technicals["box_break_up"],
            "box_break_down": technicals["box_break_down"],
            "near_box_top":   technicals["near_box_top"],
            "near_box_bottom": technicals["near_box_bottom"],
            "swing_high":       technicals["swing_high"],
            "swing_low":        technicals["swing_low"],
            "swing_high_break": technicals["swing_high_break"],
            "swing_low_break":  technicals["swing_low_break"],
            "pdh": technicals["pdh"], "pdl": technicals["pdl"],
            "pwh": technicals["pwh"], "pwl": technicals["pwl"],
            "pdh_break": technicals["pdh_break"],
            "pdl_break": technicals["pdl_break"],
            "pwh_break": technicals["pwh_break"],
            "pwl_break": technicals["pwl_break"],
            "es":   tradfi.get("es"),
            "nq":   tradfi.get("nq"),
            "dxy":  tradfi.get("dxy"),
            "gold": tradfi.get("gold"),
            "vix":  tradfi.get("vix"),
            "btc_dominance":       crypto_market.get("btc_dominance"),
            "stablecoin_supply_b": crypto_market.get("stablecoin_supply_b"),
            "bviv": bviv,
        }

        data.update(trends)

        self.cached_data = data
        self.last_fetch  = datetime.now()
        return data
