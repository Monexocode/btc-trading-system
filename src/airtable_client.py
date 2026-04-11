#!/usr/bin/env python3
"""
Airtable Client for BTC Trading System
Handles all Airtable API interactions for both daily data and 15-min signals.
"""

import os
import requests
from datetime import datetime
from typing import Dict, Any, Optional, List


class AirtableClient:
    """Client for interacting with Airtable API."""
    
    # Base and table configuration
    BASE_ID = "appmD7KWTpcrRtAd6"
    DAILY_TABLE_ID = "tbl0Y1Ak9v1CWivid"
    SIGNALS_TABLE_ID = "tblE4X11pjjNNCfYU"
    
    # Daily table field IDs
    DAILY_FIELDS = {
        'name': 'fldgmgIFmXy2XYXuv',
        'btc_price': 'fld5kPXlCUF9v8qoj',
        'oi_total': 'fldDXTQLutuc1kmMx',
        'oi_cme': 'fldcFH9HV8ocWt1lb',
        'funding': 'fldM6t0ftIDJWdCjy',
        'etf_flow': 'fldcrACeSVslVGSVT',
        'cvd_futures': 'fldqsVTSXrUFahAh7',
        'cvd_spot': 'fldgEKgDJgaZhvzpf',
        'liquidations': 'fldYlvB3mniCFeCVh',
        'poc': 'fldxwCrweRSCQ9kCv',
        'vwap': 'fld0UX9wzNQKrhkWO',
        'ema_trend': 'fldGdiKGWiWwGyxLd',
        'vah_val': 'fldAY4Mo4h9ezDVUL',
        'kc_bb_squeeze': 'fldbxaSECMWbvpdDL',
        'normal_box': 'fldGPLMK1afIyiLBU',
        'breaking_point': 'fldRjqYFpp1JWUz8s',
        'kc_positioning': 'fldpW9T4z7f3oTkn0',
        'bb_positioning': 'fldnZ5ssbI4zNNZ68',
        'vwap_band': 'fld79uOSjXBr3lmBr',
        'price_oi': 'fldp1yE4ARNW9mxpA',
        'synergy': 'fld1t1ZQAyctSHN0h',
        'strength': 'fldBJmRjCEr2ZKfnF',
        'anomalies': 'fldzWhwZlqfZTHb1C',
        'cvd_vs_price': 'fldARbV2NdEyooh0z',
        'oi_vs_price': 'fldFUGboKsJULgwcE'
    }
    
    # 15-min signals table field IDs
    SIGNAL_FIELDS = {
        'timestamp': 'fldSHm19YL0JQroMe',
        'btc_price': 'fldQnVKq45ZYwCV99',
        'signal': 'fld04YSvo006cUVQc',
        'total_score': 'flddlcvkAuirQ3P4b',
        'entry_mode': 'fldggYQbtrByvPWvk',
        'direction_score': 'fld0WXdAqMhsmkJVQ',
        'momentum_score': 'fldwBSLF3j04tPsQ6',
        'breakout_score': 'flded8BHoROcOItBw',
        'price_action_score': 'fldaErH0NeIJBP8eB',
        'key_level_score': 'fldzTL2MZkndySp7G',
        'daily_tpi': 'fldwUVbrKmMvjlQbB',
        'daily_oi_trend': 'fldI6SEiF3TK5VpQT',
        'notes': 'fld1AyU1fNy5WWQmu'
    }
    
    def __init__(self, api_key: Optional[str] = None):
        """Initialize the Airtable client."""
        self.api_key = api_key or os.environ.get('AIRTABLE_API_KEY')
        if not self.api_key:
            raise ValueError("AIRTABLE_API_KEY environment variable not set")
        
        self.base_url = f"https://api.airtable.com/v0/{self.BASE_ID}"
        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
    
    def _make_request(
        self,
        method: str,
        endpoint: str,
        data: Optional[Dict] = None
    ) -> Dict[str, Any]:
        """Make an API request to Airtable."""
        url = f"{self.base_url}/{endpoint}"
        
        try:
            if method == "GET":
                response = requests.get(url, headers=self.headers)
            elif method == "POST":
                response = requests.post(url, headers=self.headers, json=data)
            elif method == "PATCH":
                response = requests.patch(url, headers=self.headers, json=data)
            else:
                raise ValueError(f"Unsupported HTTP method: {method}")
            
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.RequestException as e:
            print(f"Airtable API error: {e}")
            if hasattr(e, 'response') and e.response is not None:
                print(f"Response: {e.response.text}")
            raise
    
    def upload_to_airtable(
        self,
        market_data: Dict[str, Any],
        scores: Dict[str, Any]
    ) -> bool:
        """
        Upload daily macro data to the daily table.
        
        Args:
            market_data: Dictionary containing market data from DataFetcher
            scores: Dictionary containing calculated scores from ScoringEngine
        
        Returns:
            True if successful, False otherwise
        """
        try:
            # Build the record
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M')
            
            fields = {
                self.DAILY_FIELDS['name']: timestamp,
                self.DAILY_FIELDS['btc_price']: market_data.get('btc_price', 0),
                self.DAILY_FIELDS['oi_total']: market_data.get('oi_total', 0),
                self.DAILY_FIELDS['funding']: market_data.get('funding_rate', 0),
                self.DAILY_FIELDS['liquidations']: market_data.get('liquidations_24h', 0),
            }
            
            # Add optional fields if present
            if 'oi_cme' in market_data:
                fields[self.DAILY_FIELDS['oi_cme']] = market_data['oi_cme']
            
            if 'cvd_futures' in market_data:
                fields[self.DAILY_FIELDS['cvd_futures']] = market_data['cvd_futures']
            
            if 'cvd_spot' in market_data:
                fields[self.DAILY_FIELDS['cvd_spot']] = market_data['cvd_spot']
            
            if 'etf_flow' in market_data:
                fields[self.DAILY_FIELDS['etf_flow']] = market_data['etf_flow']
            
            # Add scores
            if 'synergy' in scores:
                fields[self.DAILY_FIELDS['synergy']] = scores['synergy']
            
            if 'strength' in scores:
                fields[self.DAILY_FIELDS['strength']] = scores['strength']
            
            # Determine OI vs Price relationship
            oi_trend = market_data.get('oi_trend', 'neutral')
            price_trend = market_data.get('price_trend', 'neutral')
            
            if oi_trend == 'up' and price_trend == 'up':
                oi_vs_price = 'Bullish (OI↑ Price↑)'
            elif oi_trend == 'up' and price_trend == 'down':
                oi_vs_price = 'Bearish (OI↑ Price↓)'
            elif oi_trend == 'down' and price_trend == 'up':
                oi_vs_price = 'Short squeeze (OI↓ Price↑)'
            elif oi_trend == 'down' and price_trend == 'down':
                oi_vs_price = 'Long liquidation (OI↓ Price↓)'
            else:
                oi_vs_price = 'Neutral'
            
            fields[self.DAILY_FIELDS['oi_vs_price']] = oi_vs_price
            
            # Create the record
            data = {"records": [{"fields": fields}]}
            result = self._make_request("POST", self.DAILY_TABLE_ID, data)
            
            print(f"   Daily record created: {result['records'][0]['id']}")
            return True
            
        except Exception as e:
            print(f"   Error uploading daily data: {e}")
            return False
    
    def upload_15min_signal(self, signal_data: Dict[str, Any]) -> bool:
        """
        Upload a 15-minute signal to the Intra day table.
        
        Args:
            signal_data: Dictionary containing:
                - timestamp: Signal timestamp
                - btc_price: Current BTC price
                - signal: BUY/SELL/STALL
                - total_score: Combined score
                - entry_mode: Mode preset used
                - direction_score: Direction component
                - momentum_score: Momentum component
                - breakout_score: Breakout component
                - price_action_score: Price action component
                - key_level_score: Key level component
                - daily_tpi: Daily TPI value
                - daily_oi_trend: OI trend direction
                - notes: Optional notes
        
        Returns:
            True if successful, False otherwise
        """
        try:
            fields = {
                self.SIGNAL_FIELDS['timestamp']: signal_data.get('timestamp', 
                    datetime.now().strftime('%Y-%m-%d %H:%M')),
                self.SIGNAL_FIELDS['btc_price']: signal_data.get('btc_price', 0),
                self.SIGNAL_FIELDS['signal']: [signal_data.get('signal', 'STALL')],
                self.SIGNAL_FIELDS['total_score']: signal_data.get('total_score', 0),
                self.SIGNAL_FIELDS['entry_mode']: signal_data.get('entry_mode', 'full_system'),
                self.SIGNAL_FIELDS['direction_score']: signal_data.get('direction_score',
