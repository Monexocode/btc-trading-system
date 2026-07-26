"""
Supabase Integration - Upload trading data and scores to Supabase (Postgres).

This module handles syncing calculated trading signals and raw data to the
Supabase `daily` and `intraday_signals` tables. It writes the SAME rows as
the trading-tpi-pipeline repo (upsert on the `name` column = "YYYY-MM-DD BTC/USDT"),
so both crons converge on one row per day.

Env:
  SUPABASE_URL          — project URL (https://<ref>.supabase.co)
  SUPABASE_SERVICE_KEY  — service_role key (bypasses RLS for server-side writes)
"""

import os
from typing import Dict, Optional
from datetime import datetime
from dataclasses import dataclass

from supabase import create_client, Client


@dataclass
class SupabaseConfig:
    """Supabase configuration"""
    url: Optional[str] = None
    key: Optional[str] = None
    daily_table: str = "daily"
    signals_table: str = "intraday_signals"

    def __post_init__(self):
        if self.url is None:
            self.url = os.environ.get("SUPABASE_URL")
        if self.key is None:
            self.key = os.environ.get("SUPABASE_SERVICE_KEY")
        table_override = os.environ.get("SUPABASE_DAILY_TABLE")
        if table_override:
            self.daily_table = table_override
        signals_override = os.environ.get("SUPABASE_INTRADAY_TABLE")
        if signals_override:
            self.signals_table = signals_override


class SupabaseClient:
    """Client for writing trading data to Supabase."""

    # Local data-key → daily table column name.
    # (Mirrors the shared columns owned by trading-tpi-pipeline.)
    COLUMN_MAP = {
        "name":            "name",
        "btc":             "btc",
        "oi":              "oi_bn",
        "cme_oi":          "cme_oi_bn",
        "funding":         "funding",
        "etf":             "etf_m",
        "cvd_futs":        "cvd_futs",
        "cvd_spot":        "cvd_spot",
        "liqs_prev":       "liqs",
        "liqs_prev_price": "liqs_price",
        "eth":             "eth",
        "ethbtc":          "ethbtc",
        "sol":             "sol",
        "btc_d":           "btc_d",
        "stables":         "stables_bn",
        "es":              "es",
        "nq":              "nq",
        "dxy":             "dxy",
        "gold":            "gold",
        "us10y":           "us10y",
        "us20y":           "us20y",
        "vix":             "vix",
        "bvix":            "bvix",
        "anomalies":       "anomalies",
        "cvd_vs_price":    "cvd_vs_price",
        "oi_vs_price":     "oi_vs_price",
    }

    # Local signal-key → intraday_signals column name.
    SIGNALS_COLUMN_MAP = {
        "timestamp":          "ts",
        "btc_price":          "btc_price",
        "signal":             "signal",
        "total_score":        "total_score",
        "entry_mode":         "entry_mode",
        "direction_score":    "direction_score",
        "momentum_score":     "momentum_score",
        "breakout_score":     "breakout_score",
        "price_action_score": "price_action_score",
        "key_level_score":    "key_level_score",
        "daily_tpi":          "daily_tpi",
        "daily_oi_trend":     "daily_oi_trend",
        "notes":              "notes",
    }

    def __init__(self, config: Optional[SupabaseConfig] = None):
        self.config = config or SupabaseConfig()
        if not self.config.url or not self.config.key:
            raise ValueError(
                "Supabase not configured. Set SUPABASE_URL and SUPABASE_SERVICE_KEY env vars."
            )
        self.client: Client = create_client(self.config.url, self.config.key)

    def _map_columns(self, data: Dict, mapping: Dict) -> Dict:
        """Translate local data keys to DB column names; pass through unknown keys."""
        return {mapping.get(k, k): v for k, v in data.items()}

    def upsert_daily_data(self, data: Dict) -> Dict:
        """
        Upsert daily trading data, matching on the `name` column.

        Args:
            data: Dictionary with trading data fields (local keys).

        Returns:
            The created/updated record.
        """
        today = datetime.now().strftime("%Y-%m-%d")
        record_name = f"{today} BTC/USDT"
        data["name"] = record_name

        row = self._map_columns(data, self.COLUMN_MAP)
        resp = (
            self.client.table(self.config.daily_table)
            .upsert(row, on_conflict="name")
            .execute()
        )
        return resp.data[0] if resp.data else {}

    def upload_15min_signal(
        self,
        btc_price: float,
        total_score: float,
        signal: str,  # "BUY", "SELL", "STALL"
        entry_mode: str,
        direction_score: float = 0,
        momentum_score: float = 0,
        breakout_score: float = 0,
        price_action_score: float = 0,
        key_level_score: float = 0,
        daily_tpi: Optional[float] = None,
        daily_oi_trend: Optional[str] = None,
        daily_funding: Optional[float] = None,
        notes: str = "",
    ) -> Dict:
        """Insert a 15-minute signal row into the intraday_signals table."""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M")

        data = {
            "timestamp": timestamp,
            "btc_price": btc_price,
            "signal": signal,
            "total_score": round(total_score, 2),
            "entry_mode": entry_mode,
            "direction_score": round(direction_score, 2),
            "momentum_score": round(momentum_score, 2),
            "breakout_score": round(breakout_score, 2),
            "price_action_score": round(price_action_score, 2),
            "key_level_score": round(key_level_score, 2),
        }
        if daily_tpi is not None:
            data["daily_tpi"] = daily_tpi
        if daily_oi_trend:
            data["daily_oi_trend"] = daily_oi_trend
        if notes:
            data["notes"] = notes

        row = self._map_columns(data, self.SIGNALS_COLUMN_MAP)
        resp = self.client.table(self.config.signals_table).insert(row).execute()
        return resp.data[0] if resp.data else {}
