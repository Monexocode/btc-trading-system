#!/usr/bin/env python3
"""
Data Fetcher for BTC Trading System
Fetches market data from various sources (simulated for now, ready for API integration).
"""

import random
from datetime import datetime, timedelta
from typing import Dict, Any, Optional


class DataFetcher:
    """Fetches market data from various sources."""
    
    def __init__(self):
        """Initialize the data fetcher."""
        self.last_fetch = None
        self.cached_data = None
        self.cache_duration = timedelta(minutes=5)
    
    def fetch_btc_price(self) -> float:
        """
        Fetch current BTC price.
        
        In production, this would call CoinGecko, Binance, or similar API.
        For now, returns simulated data based on realistic ranges.
        """
        # Simulated price around $70,000 range
        base_price = 70000
        variance = random.uniform(-2000, 2000)
        return round(base_price + variance, 2)
    
    def fetch_open_interest(self) -> Dict[str, float]:
        """
        Fetch Open Interest data.
        
        Returns:
            Dictionary with total OI and CME OI in billions.
        """
        # Simulated OI data (in billions)
        total_oi = round(random.uniform(20, 25), 2)
        cme_oi = round(random.uniform(8, 12), 2)
        
        return {
            'total': total_oi,
            'cme': cme_oi,
            'ratio': round(cme_oi / total_oi * 100, 1)
        }
    
    def fetch_funding_rate(self) -> float:
        """
        Fetch current funding rate.
        
        Returns:
            Funding rate as a percentage (e.g., 0.01 = 0.01%)
        """
        # Simulated funding rate (-0.05% to 0.1%)
        return round(random.uniform(-0.05, 0.1), 4)
    
    def fetch_liquidations(self) -> Dict[str, float]:
        """
        Fetch 24h liquidation data.
        
        Returns:
            Dictionary with total, long, and short liquidations in millions.
        """
        long_liqs = round(random.uniform(10, 50), 1)
        short_liqs = round(random.uniform(10, 50), 1)
        total = round(long_liqs + short_liqs, 1)
        
        return {
            'total_24h': total,
            'longs': long_liqs,
            'shorts': short_liqs,
            'ratio': round(long_liqs / short_liqs, 2) if short_liqs > 0 else 0
        }
    
    def fetch_cvd(self) -> Dict[str, float]:
        """
        Fetch Cumulative Volume Delta data.
        
        Returns:
            Dictionary with futures and spot CVD values.
        """
        return {
            'futures': round(random.uniform(-500, 500), 1),
            'spot': round(random.uniform(-300, 300), 1)
        }
    
    def fetch_etf_flow(self) -> float:
        """
        Fetch BTC ETF net flow.
        
        Returns:
            Net flow in millions (positive = inflow, negative = outflow)
        """
        return round(random.uniform(-200, 300), 1)
    
    def fetch_volume_profile(self) -> Dict[str, float]:
        """
        Fetch Volume Profile data (POC, VAH, VAL).
        
        Returns:
            Dictionary with Point of Control, Value Area High/Low.
        """
        btc_price = 70000
        
        poc = round(btc_price + random.uniform(-500, 500), 0)
        vah = round(poc + random.uniform(500, 1500), 0)
        val = round(poc - random.uniform(500, 1500), 0)
        
        return {
            'poc': poc,
            'vah': vah,
            'val': val,
            'value_area_width': vah - val
        }
    
    def fetch_technical_indicators(self, btc_price: float) -> Dict[str, Any]:
        """
        Calculate technical indicators.
        
        Args:
            btc_price: Current BTC price
        
        Returns:
            Dictionary with various technical indicators.
        """
        # VWAP
        vwap = round(btc_price + random.uniform(-300, 300), 2)
        
        # EMA values
        ema_20 = round(btc_price + random.uniform(-200, 200), 2)
        ema_50 = round(btc_price + random.uniform(-500, 500), 2)
        ema_200 = round(btc_price + random.uniform(-1000, 1000), 2)
        
        # Determine trend
        if btc_price > ema_20 > ema_50:
            ema_trend = 'bullish'
        elif btc_price < ema_20 < ema_50:
            ema_trend = 'bearish'
        else:
            ema_trend = 'neutral'
        
        # Keltner Channel / Bollinger Band positions
        kc_upper = round(btc_price + random.uniform(800, 1200), 2)
        kc_lower = round(btc_price - random.uniform(800, 1200), 2)
        bb_upper = round(btc_price + random.uniform(600, 1000), 2)
        bb_lower = round(btc_price - random.uniform(600, 1000), 2)
        
        # Squeeze detection
        squeeze = bb_upper < kc_upper and bb_lower > kc_lower
        
        return {
            'vwap': vwap,
            'vwap_position': 'above' if btc_price > vwap else 'below',
            'ema_20': ema_20,
            'ema_50': ema_50,
            'ema_200': ema_200,
            'ema_trend': ema_trend,
            'kc_upper': kc_upper,
            'kc_lower': kc_lower,
            'bb_upper': bb_upper,
            'bb_lower': bb_lower,
            'squeeze': squeeze,
            'kc_position': 'inside' if kc_lower < btc_price < kc_upper else 'outside',
            'bb_position': 'inside' if bb_lower < btc_price < bb_upper else 'outside'
        }
    
    def determine_trends(self, data: Dict[str, Any]) -> Dict[str, str]:
        """
        Determine OI and price trends based on data.
        
        Args:
            data: Market data dictionary
        
        Returns:
            Dictionary with trend directions.
        """
        # Simplified trend determination
        # In production, compare with previous values
        oi_change = random.choice(['up', 'down', 'neutral'])
        price_change = random.choice(['up', 'down', 'neutral'])
        
        return {
            'oi_trend': oi_change,
            'price_trend': price_change
        }
    
    def fetch_all_data(self) -> Dict[str, Any]:
        """
        Fetch all market data in a single call.
        
        Returns:
            Comprehensive dictionary with all market data.
        """
        # Check cache
        if (self.cached_data and self.last_fetch and 
            datetime.now() - self.last_fetch < self.cache_duration):
            print("   Using cached data...")
            return self.cached_data
        
        # Fetch all data
        btc_price = self.fetch_btc_price()
        oi_data = self.fetch_open_interest()
        funding = self.fetch_funding_rate()
        liquidations = self.fetch_liquidations()
        cvd = self.fetch_cvd()
        etf_flow = self.fetch_etf_flow()
        volume_profile = self.fetch_volume_profile()
        technicals = self.fetch_technical_indicators(btc_price)
        
        # Combine all data
        data = {
            'timestamp': datetime.now().isoformat(),
            'btc_price': btc_price,
            
            # Open Interest
            'oi_total': oi_data['total'],
            'oi_cme': oi_data['cme'],
            'oi_cme_ratio': oi_data['ratio'],
            
            # Funding
            'funding_rate': funding,
            
            # Liquidations
            'liquidations_24h': liquidations['total_24h'],
            'liquidations_longs': liquidations['longs'],
            'liquidations_shorts': liquidations['shorts'],
            'liquidation_ratio': liquidations['ratio'],
            
            # CVD
            'cvd_futures': cvd['futures'],
            'cvd_spot': cvd['spot'],
            
            # ETF
            'etf_flow': etf_flow,
            
            # Volume Profile
            'poc': volume_profile['poc'],
            'vah': volume_profile['vah'],
            'val': volume_profile['val'],
            
            # Technical Indicators
            'vwap': technicals['vwap'],
            'vwap_position': technicals['vwap_position'],
            'ema_trend': technicals['ema_trend'],
            'squeeze': technicals['squeeze'],
            'kc_position': technicals['kc_position'],
            'bb_position': technicals['bb_position'],
        }
        
        # Add trends
        trends = self.determine_trends(data)
        data.update(trends)
        
        # Update cache
        self.cached_data = data
        self.last_fetch = datetime.now()
        
        return data
