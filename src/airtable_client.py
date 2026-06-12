"""
Airtable Integration - Upload trading data and scores to Airtable
 
This module handles syncing calculated trading signals and raw data
to your Airtable "Trading" base.
"""
 
import os
import requests
from typing import Dict, List, Optional
from datetime import datetime
from dataclasses import dataclass
 
 
@dataclass
class AirtableConfig:
    """Airtable configuration"""
    base_id: str = "appmD7KWTpcrRtAd6"
    daily_table_id: str = "tbl0Y1Ak9v1CWivid"
    signals_table_id: str = "tblE4X11pjjNNCfYU"
    api_key: Optional[str] = None
    
    @property
    def table_id(self) -> str:
        return self.daily_table_id
    
    def __post_init__(self):
        if self.api_key is None:
            self.api_key = os.environ.get("AIRTABLE_API_KEY")
        signals_override = os.environ.get("AIRTABLE_SIGNALS_TABLE_ID")
        if signals_override:
            self.signals_table_id = signals_override
 
 
class AirtableClient:
    
    BASE_URL = "https://api.airtable.com/v0"
    
    FIELD_IDS = {
        "name": "fldgmgIFmXy2XYXuv",
        "btc": "fld5kPXlCUF9v8qoj",
        "oi": "fldDXTQLutuc1kmMx",
        "cme_oi": "fldcFH9HV8ocWt1lb",
        "funding": "fldM6t0ftIDJWdCjy",
        "etf": "fldcrACeSVslVGSVT",
        "cvd_futs": "fldqsVTSXrUFahAh7",
        "cvd_spot": "fldgEKgDJgaZhvzpf",
        "liqs_prev": "fldYlvB3mniCFeCVh",
        "liqs_prev_price": "fldBVTdGsJSNRC7MQ",
        "eth": "fldhobfwGY348TtJU",
        "ethbtc": "fldALY8uI8e5DQWEf",
        "sol": "fldHe2k08EfHbzwqh",
        "btc_d": "fldWPfQ1JlY7PJZKp",
        "stables": "fldDeJ4IyBnS2WyNk",
        "es": "fldlfuOOjnjfCmeqL",
        "nq": "fldrCNNHwC2J0YKCs",
        "dxy": "fldI98REYB4vDG53N",
        "gold": "fldLeseU3rvnDdKtM",
        "us10y": "fld61JzaFGzf7oqXc",
        "us20y": "fldpup7rorMSoOkvX",
        "vix": "fld5rt5FT6JpnTjxt",
        "bvix": "fldmop0YXdPI22Bas",
        "poc": "fldxwCrweRSCQ9kCv",
        "vwap": "fld0UX9wzNQKrhkWO",
        "ema_trend": "fldGdiKGWiWwGyxLd",
        "vah_val": "fldAY4Mo4h9ezDVUL",
        "kc_bb_squeeze": "fldbxaSECMWbvpdDL",
        "vol_1_5": "fld3RzRqMzmUf1Z0m",
        "normal_box": "fldGPLMK1afIyiLBU",
        "breaking_point": "fldRjqYFpp1JWUz8s",
        "kc_positioning": "fldpW9T4z7f3oTkn0",
        "bb_positioning": "fldnZ5ssbI4zNNZ68",
        "vwap_band": "fld79uOSjXBr3lmBr",
        "price_oi": "fldp1yE4ARNW9mxpA",
        "synergy_tw": "fld1t1ZQAyctSHN0h",
        "strength_tw": "fldBJmRjCEr2ZKfnF",
        "anomalies": "fldzWhwZlqfZTHb1C",
        "cvd_vs_price": "fldARbV2NdEyooh0z",
        "oi_vs_price": "fldFUGboKsJULgwcE",
    }
    
    SIGNALS_FIELD_IDS = {
        "timestamp": "fldSHm19YL0JQroMe",
        "btc_price": "fldQnVKq45ZYwCV99",
        "signal": "fld04YSvo006cUVQc",
        "total_score": "flddlcvkAuirQ3P4b",
        "entry_mode": "fldggYQbtrByvPWvk",
        "direction_score": "fld0WXdAqMhsmkJVQ",
        "momentum_score": "fldwBSLF3j04tPsQ6",
        "breakout_score": "flded8BHoROcOItBw",
        "price_action_score": "fldaErH0NeIJBP8eB",
        "key_level_score": "fldzTL2MZkndySp7G",
        "daily_tpi": "fldwUVbrKmMvjlQbB",
        "daily_oi_trend": "fldI6SEiF3TK5VpQT",
        "notes": "fld1AyU1fNy5WWQmu",
    }
    
    def __init__(self, config: Optional[AirtableConfig] = None):
        self.config = config or AirtableConfig()
        if not self.config.api_key:
            raise ValueError("Airtable API key not set. Set AIRTABLE_API_KEY env var.")
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {self.config.api_key}",
            "Content-Type": "application/json"
        })
    
    def _get_url(self, table_id: Optional[str] = None) -> str:
        tid = table_id or self.config.table_id
        return f"{self.BASE_URL}/{self.config.base_id}/{tid}"
    
    def list_records(self, table_id: Optional[str] = None, max_records: int = 100,
                     filter_formula: Optional[str] = None) -> List[Dict]:
        params = {"maxRecords": max_records}
        if filter_formula:
            params["filterByFormula"] = filter_formula
        response = self.session.get(self._get_url(table_id), params=params)
        response.raise_for_status()
        return response.json().get("records", [])
    
    def create_record(self, fields: Dict, table_id: Optional[str] = None,
                      typecast: bool = True) -> Dict:
        field_data = {}
        for key, value in fields.items():
            if key in self.FIELD_IDS:
                field_data[self.FIELD_IDS[key]] = value
            else:
                field_data[key] = value
        payload = {"records": [{"fields": field_data}], "typecast": typecast}
        response = self.session.post(self._get_url(table_id), json=payload)
        response.raise_for_status()
        return response.json()["records"][0]
    
    def update_record(self, record_id: str, fields: Dict, table_id: Optional[str] = None,
                      typecast: bool = True) -> Dict:
        field_data = {}
        for key, value in fields.items():
            if key in self.FIELD_IDS:
                field_data[self.FIELD_IDS[key]] = value
            else:
                field_data[key] = value
        payload = {
            "records": [{"id": record_id, "fields": field_data}],
            "typecast": typecast
        }
        response = self.session.patch(self._get_url(table_id), json=payload)
        response.raise_for_status()
        return response.json()["records"][0]
    
    def upsert_daily_data(self, data: Dict) -> Dict:
        today = datetime.now().strftime("%Y-%m-%d")
        record_name = f"{today} BTC/USDT"
        filter_formula = f"{{Name}}='{record_name}'"
        existing = self.list_records(filter_formula=filter_formula, max_records=1)
        data["name"] = record_name
        if existing:
            return self.update_record(existing[0]["id"], data)
        else:
            return self.create_record(data)
    
    def upload_15min_signal(
        self,
        btc_price: float,
        total_score: float,
        signal: str,
        entry_mode: str,
        direction_score: float = 0,
        momentum_score: float = 0,
        breakout_score: float = 0,
        price_action_score: float = 0,
        key_level_score: float = 0,
        daily_tpi: Optional[float] = None,
        daily_oi_trend: Optional[str] = None,
        daily_funding: Optional[float] = None,
        notes: str = ""
    ) -> Dict:
        if not self.config.signals_table_id:
            raise ValueError("Signals table ID not configured.")
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
        field_data = {}
        for key, value in data.items():
            if key in self.SIGNALS_FIELD_IDS and self.SIGNALS_FIELD_IDS[key]:
                field_data[self.SIGNALS_FIELD_IDS[key]] = value
            else:
                field_data[key] = value
        payload = {"records": [{"fields": field_data}], "typecast": True}
        response = self.session.post(self._get_url(self.config.signals_table_id), json=payload)
        response.raise_for_status()
        return response.json()["records"][0]
    
    def get_latest_daily_context(self) -> Optional[Dict]:
        records = self.list_records(max_records=1)
        if not records:
            return None
        fields = records[0].get("fields", {})
        return {
            "tpi": fields.get(self.FIELD_IDS.get("strength_tw", ""), 0),
            "oi_trend": fields.get(self.FIELD_IDS.get("oi_vs_price", ""), "Neutral"),
            "funding": fields.get(self.FIELD_IDS.get("funding", ""), 0),
        }
    
    def determine_signal(self, total_score: float, threshold: float = 5.0,
                         daily_context: Optional[Dict] = None) -> str:
        if total_score >= threshold:
            return "BUY"
        elif total_score <= -threshold:
            return "SELL"
        return "STALL"
