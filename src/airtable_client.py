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
    base_id: str = "appmD7KWTpcrRtAd6"  # Your Trading base
    daily_table_id: str = "tbl0Y1Ak9v1CWivid"  # "Just nu daily data & intra day data"
    signals_table_id: str = "tblE4X11pjjNNCfYU"  # "Intra day" - 15-min signals
    api_key: Optional[str] = None  # Set via environment variable
    
    # Alias for backward compatibility
    @property
    def table_id(self) -> str:
        return self.daily_table_id
    
    def __post_init__(self):
        if self.api_key is None:
            self.api_key = os.environ.get("AIRTABLE_API_KEY")
        # Allow override from env
        signals_override = os.environ.get("AIRTABLE_SIGNALS_TABLE_ID")
        if signals_override:
            self.signals_table_id = signals_override
 
 
class AirtableClient:
    """Client for interacting with Airtable API"""
    
    BASE_URL = "https://api.airtable.com/v0"
    
    # Field IDs from your Airtable schema
    FIELD_IDS = {
        # Core data
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
        
        # Crypto prices
        "eth": "fldhobfwGY348TtJU",
        "ethbtc": "fldALY8uI8e5DQWEf",
        "sol": "fldHe2k08EfHbzwqh",
        "btc_d": "fldWPfQ1JlY7PJZKp",
        "stables": "fldDeJ4IyBnS2WyNk",
        
        # TradFi
        "es": "fldlfuOOjnjfCmeqL",
        "nq": "fldrCNNHwC2J0YKCs",
        "dxy": "fldI98REYB4vDG53N",
        "gold": "fldLeseU3rvnDdKtM",
        "us10y": "fld61JzaFGzf7oqXc",
        "us20y": "fldpup7rorMSoOkvX",
        "vix": "fld5rt5FT6JpnTjxt",
        "bvix": "fldmop0YXdPI22Bas",
        
        # Technical indicators (from TradingView script)
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
        
        # Scoring
        "synergy_tw": "fld1t1ZQAyctSHN0h",
        "strength_tw": "fldBJmRjCEr2ZKfnF",
        "anomalies": "fldzWhwZlqfZTHb1C",
        
        # Analysis fields
        "cvd_vs_price": "fldARbV2NdEyooh0z",
        "oi_vs_price": "fldFUGboKsJULgwcE",
    }
    
    # Field IDs for 15-Minute Signals table ("Intra day" - tblE4X11pjjNNCfYU)
    SIGNALS_FIELD_IDS = {
        "timestamp": "fldSHm19YL0JQroMe",  # Primary field
        "btc_price": "fldQnVKq45ZYwCV99",
        "signal": "fld04YSvo006cUVQc",  # multipleSelects: BUY, SELL, STALL
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
        """Get API URL for table"""
        tid = table_id or self.config.table_id
        return f"{self.BASE_URL}/{self.config.base_id}/{tid}"
    
    def list_records(
        self, 
        table_id: Optional[str] = None,
        max_records: int = 100,
        filter_formula: Optional[str] = None
    ) -> List[Dict]:
        """List records from a table"""
        params = {"maxRecords": max_records}
        if filter_formula:
            params["filterByFormula"] = filter_formula
        
        response = self.session.get(self._get_url(table_id), params=params)
        response.raise_for_status()
        return response.json().get("records", [])
    
    def create_record(
        self, 
        fields: Dict,
        table_id: Optional[str] = None,
        typecast: bool = True
    ) -> Dict:
        """Create a new record"""
        # Convert field names to IDs
        field_data = {}
        for key, value in fields.items():
            if key in self.FIELD_IDS:
                field_data[self.FIELD_IDS[key]] = value
            else:
                # Assume it's already a field ID
                field_data[key] = value
        
        payload = {
            "records": [{"fields": field_data}],
            "typecast": typecast
        }
        
        response = self.session.post(self._get_url(table_id), json=payload)
        response.raise_for_status()
        return response.json()["records"][0]
    
    def update_record(
        self,
        record_id: str,
        fields: Dict,
        table_id: Optional[str] = None,
        typecast: bool = True
    ) -> Dict:
        """Update an existing record"""
        # Convert field names to IDs
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
        """
        Upsert daily trading data.
        
        Creates a new record or updates if one exists for today.
        
        Args:
            data: Dictionary with trading data fields
            
        Returns:
            The created/updated record
        """
        today = datetime.now().strftime("%Y-%m-%d")
        
        # Check if record for today exists
        filter_formula = f"{{Name}}='{today}'"
        existing = self.list_records(filter_formula=filter_formula, max_records=1)
        
        # Add name (date) to data
        data["name"] = today
        
        if existing:
            # Update existing record
            return self.update_record(existing[0]["id"], data)
        else:
            # Create new record
            return self.create_record(data)
    
    def upload_trading_scores(
        self,
        btc_price: float,
        direction_score: float,
        momentum_score: float,
        breakout_score: float,
        price_action_score: float,
        key_level_score: float,
        total_score: float,
        signal: str,
        coinalyze_data: Optional[Dict] = None,
        indicator_states: Optional[Dict] = None
    ) -> Dict:
        """
        Upload calculated trading scores to Airtable.
        
        Args:
            btc_price: Current BTC price
            direction_score: Direction category score
            momentum_score: Momentum category score  
            breakout_score: Breakout category score
            price_action_score: Price action category score
            key_level_score: Key level category score
            total_score: Total weighted score
            signal: Signal string (STRONG_BUY, LEAN_LONG, etc.)
            coinalyze_data: Optional Coinalyze metrics
            indicator_states: Optional indicator state values
            
        Returns:
            Created/updated Airtable record
        """
        data = {
            "btc": btc_price,
            "strength_tw": total_score,
        }
        
        # Add Coinalyze data if provided
        if coinalyze_data:
            if "open_interest" in coinalyze_data:
                data["oi"] = coinalyze_data["open_interest"] / 1e9  # Convert to billions
            if "funding_rate" in coinalyze_data:
                data["funding"] = coinalyze_data["funding_rate"]
            if "cvd_futures" in coinalyze_data:
                data["cvd_futs"] = coinalyze_data["cvd_futures"] / 1e6  # Convert to millions
            if "cvd_spot" in coinalyze_data:
                data["cvd_spot"] = coinalyze_data["cvd_spot"]
            if "cme_oi" in coinalyze_data:
                data["cme_oi"] = coinalyze_data["cme_oi"] / 1e9
            if "liquidations_long" in coinalyze_data:
                total_liqs = coinalyze_data.get("liquidations_long", 0) + coinalyze_data.get("liquidations_short", 0)
                data["liqs_prev"] = total_liqs / 1e6  # Convert to millions
        
        # Add indicator states if provided
        if indicator_states:
            if "poc" in indicator_states:
                data["poc"] = indicator_states["poc"]
            if "vwap" in indicator_states:
                data["vwap"] = indicator_states["vwap"]
            if "squeeze_active" in indicator_states:
                data["kc_bb_squeeze"] = 1 if indicator_states["squeeze_active"] else 0
            if "normal_box_active" in indicator_states:
                data["normal_box"] = 1 if indicator_states["normal_box_active"] else 0
        
        # Build anomalies text
        anomalies = []
        
        if total_score >= 7:
            anomalies.append(f"🟢 STRONG BUY signal: {total_score:.1f}")
        elif total_score >= 5:
            anomalies.append(f"🟡 Buy signal: {total_score:.1f}")
        elif total_score <= -7:
            anomalies.append(f"🔴 STRONG SELL signal: {total_score:.1f}")
        elif total_score <= -5:
            anomalies.append(f"🟠 Sell signal: {total_score:.1f}")
        
        if coinalyze_data:
            funding = coinalyze_data.get("funding_rate", 0)
            if funding and abs(funding) > 0.01:
                direction = "long" if funding > 0 else "short"
                anomalies.append(f"⚠️ High funding rate ({funding:.4f}) - crowded {direction}s")
        
        if anomalies:
            data["anomalies"] = "\n".join(anomalies)
        
        # Set CVD vs Price analysis
        # This would need more sophisticated analysis
        
        return self.upsert_daily_data(data)
    
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
        notes: str = ""
    ) -> Dict:
        """
        Upload a 15-minute signal to the signals table.
        
        The signal is determined by total_score:
        - BUY: total_score >= threshold (default 5.0)
        - SELL: total_score <= -threshold
        - STALL: otherwise
        
        Args:
            btc_price: Current BTC price
            total_score: Total weighted score from scoring engine
            signal: "BUY", "SELL", or "STALL"
            entry_mode: Entry mode used (e.g., "scalper", "swing_trader")
            direction_score: Direction category score
            momentum_score: Momentum category score
            breakout_score: Breakout category score
            price_action_score: Price action category score
            key_level_score: Key level category score
            daily_tpi: TPI from daily Coinalyze data (provides macro context)
            daily_oi_trend: OI trend from daily data
            daily_funding: Funding rate from daily data
            notes: Additional notes or anomalies
            
        Returns:
            Created Airtable record
        """
        if not self.config.signals_table_id:
            raise ValueError("Signals table ID not configured. Set AIRTABLE_SIGNALS_TABLE_ID env var.")
        
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M")
        
        # Build record with dynamic field mapping
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
        if daily_funding is not None:
            data["daily_funding"] = daily_funding
        if notes:
            data["notes"] = notes
        
        # Convert to field IDs
        field_data = {}
        for key, value in data.items():
            if key in self.SIGNALS_FIELD_IDS and self.SIGNALS_FIELD_IDS[key]:
                field_data[self.SIGNALS_FIELD_IDS[key]] = value
            else:
                # Use field name directly if no ID mapped (will need typecast)
                field_data[key] = value
        
        payload = {
            "records": [{"fields": field_data}],
            "typecast": True  # Auto-create select options
        }
        
        response = self.session.post(
            self._get_url(self.config.signals_table_id), 
            json=payload
        )
        response.raise_for_status()
        return response.json()["records"][0]
    
    def get_latest_daily_context(self) -> Optional[Dict]:
        """
        Get the latest daily data for context in 15-min signals.
        
        Returns TPI, OI trend, funding rate to inform intraday decisions.
        """
        records = self.list_records(max_records=1)
        if not records:
            return None
        
        fields = records[0].get("fields", {})
        
        # Map field IDs back to names
        tpi_field = self.FIELD_IDS.get("strength_tw", "") or "fldBJmRjCEr2ZKfnF"
        oi_field = self.FIELD_IDS.get("oi_vs_price", "") or "fldFUGboKsJULgwcE"
        funding_field = self.FIELD_IDS.get("funding", "") or "fldM6t0ftIDJWdCjy"
        
        return {
            "tpi": fields.get(tpi_field, 0),
            "oi_trend": fields.get(oi_field, "Neutral"),
            "funding": fields.get(funding_field, 0),
            "date": fields.get(self.FIELD_IDS.get("name", ""), ""),
        }
    
    def determine_signal(
        self,
        total_score: float,
        threshold: float = 5.0,
        daily_context: Optional[Dict] = None
    ) -> str:
        """
        Determine BUY/SELL/STALL signal from score and daily context.
        
        Args:
            total_score: The 15-minute total score
            threshold: Score threshold for signals (default 5.0)
            daily_context: Optional daily data for confluence
            
        Returns:
            "BUY", "SELL", or "STALL"
        """
        # Base signal from score
        if total_score >= threshold:
            base_signal = "BUY"
        elif total_score <= -threshold:
            base_signal = "SELL"
        else:
            return "STALL"
        
        # Optional: Check daily context for confluence
        if daily_context:
            daily_tpi = daily_context.get("tpi", 0)
            
            # If 15-min says BUY but daily TPI is very negative, might STALL
            if base_signal == "BUY" and daily_tpi < -5:
                # Strong daily bearish context - could override
                # For now, trust 15-min signal but log it
                pass
            
            # If 15-min says SELL but daily TPI is very positive, might STALL
            if base_signal == "SELL" and daily_tpi > 5:
                pass
        
        return base_signal
 
 
def format_score_breakdown_for_airtable(breakdown) -> Dict:
    """
    Format a ScoreBreakdown object for Airtable upload.
    
    Args:
        breakdown: ScoreBreakdown dataclass from scoring_engine
        
    Returns:
        Dictionary formatted for Airtable
    """
    return {
        "ema_trend": breakdown.direction_weighted,
        "vah_val": breakdown.dir_vah_val,
        "kc_positioning": breakdown.mom_kc,
        "bb_positioning": breakdown.mom_bb,
        "vwap_band": breakdown.mom_vwap_band,
        "kc_bb_squeeze": breakdown.bo_squeeze,
        "strength_tw": breakdown.total_score,
        "synergy_tw": (
            1 if breakdown.direction_weighted > 0 and breakdown.momentum_weighted > 0 else
            -1 if breakdown.direction_weighted < 0 and breakdown.momentum_weighted < 0 else
            0
        ),
    }
 
 
if __name__ == "__main__":
    # Test the Airtable client (requires API key)
    import os
    
    api_key = os.environ.get("AIRTABLE_API_KEY")
    if not api_key:
        print("Set AIRTABLE_API_KEY environment variable to test")
        print("\nExample usage:")
        print("  export AIRTABLE_API_KEY='your_api_key'")
        print("  python airtable_client.py")
    else:
        client = AirtableClient()
        
        # List recent records
        print("Fetching recent records...")
        records = client.list_records(max_records=5)
        for record in records:
            name = record.get("fields", {}).get("fldgmgIFmXy2XYXuv", "Unknown")
            btc = record.get("fields", {}).get("fld5kPXlCUF9v8qoj", 0)
            print(f"  {name}: BTC ${btc:,.2f}")
 
