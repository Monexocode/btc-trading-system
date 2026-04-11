#!/usr/bin/env python3
"""
Scoring Engine for BTC Trading System
Calculates trading scores based on multiple indicators.
"""

from typing import Dict, Any


class ScoringEngine:
    """Calculates trading scores from market data."""
    
    def __init__(self):
        """Initialize the scoring engine."""
        self.weights = {
            'direction': 2.0,
            'momentum': 1.5,
            'breakout': 1.5,
            'price_action': 1.0,
            'key_level': 1.0
        }
    
    def calculate_direction_score(self, data: Dict[str, Any]) -> float:
        """
        Calculate direction score based on trend indicators.
        
        Range: -10 to +10
        """
        score = 0.0
        
        # EMA trend
        ema_trend = data.get('ema_trend', 'neutral')
        if ema_trend == 'bullish':
            score += 3.0
        elif ema_trend == 'bearish':
            score -= 3.0
        
        # VWAP position
        vwap_position = data.get('vwap_position', 'neutral')
        if vwap_position == 'above':
            score += 2.0
        elif vwap_position == 'below':
            score -= 2.0
        
        # OI trend combined with price
        oi_trend = data.get('oi_trend', 'neutral')
        price_trend = data.get('price_trend', 'neutral')
        
        if oi_trend == 'up' and price_trend == 'up':
            score += 2.0  # Bullish: new money entering, price rising
        elif oi_trend == 'up' and price_trend == 'down':
            score -= 2.0  # Bearish: shorts opening
        elif oi_trend == 'down' and price_trend == 'up':
            score += 1.0  # Short squeeze
        elif oi_trend == 'down' and price_trend == 'down':
            score -= 1.0  # Long liquidation
        
        # Funding rate
        funding = data.get('funding_rate', 0)
        if funding > 0.05:
            score -= 1.0  # Overleveraged longs
        elif funding < -0.02:
            score += 1.0  # Overleveraged shorts
        
        return max(-10, min(10, score))
    
    def calculate_momentum_score(self, data: Dict[str, Any]) -> float:
        """
        Calculate momentum score.
        
        Range: -10 to +10
        """
        score = 0.0
        
        # CVD analysis
        cvd_futures = data.get('cvd_futures', 0)
        cvd_spot = data.get('cvd_spot', 0)
        
        if cvd_futures > 200:
            score += 2.0
        elif cvd_futures < -200:
            score -= 2.0
        
        if cvd_spot > 100:
            score += 1.5
        elif cvd_spot < -100:
            score -= 1.5
        
        # CVD divergence (futures vs spot)
        if cvd_futures > 0 and cvd_spot > 0:
            score += 1.0  # Aligned bullish
        elif cvd_futures < 0 and cvd_spot < 0:
            score -= 1.0  # Aligned bearish
        
        # Liquidation analysis
        liq_ratio = data.get('liquidation_ratio', 1)
        if liq_ratio > 1.5:
            score -= 1.0  # More longs liquidated
        elif liq_ratio < 0.67:
            score += 1.0  # More shorts liquidated
        
        # ETF flow
        etf_flow = data.get('etf_flow', 0)
        if etf_flow > 100:
            score += 1.5
        elif etf_flow < -100:
            score -= 1.5
        
        return max(-10, min(10, score))
    
    def calculate_breakout_score(self, data: Dict[str, Any]) -> float:
        """
        Calculate breakout potential score.
        
        Range: -10 to +10
        """
        score = 0.0
        
        # Squeeze detection
        squeeze = data.get('squeeze', False)
        if squeeze:
            score += 2.0  # Volatility compression
        
        # KC/BB position
        kc_position = data.get('kc_position', 'inside')
        bb_position = data.get('bb_position', 'inside')
        
        if kc_position == 'outside':
            score += 1.5  # Breaking KC
        if bb_position == 'outside':
            score += 1.5  # Breaking BB
        
        # Price relative to POC
        btc_price = data.get('btc_price', 0)
        poc = data.get('poc', btc_price)
        
        if btc_price > 0 and poc > 0:
            poc_diff_pct = (btc_price - poc) / poc * 100
            if poc_diff_pct > 1:
                score += 1.5  # Above POC
            elif poc_diff_pct < -1:
                score -= 1.5  # Below POC
        
        # VAH/VAL analysis
        vah = data.get('vah', btc_price + 1000)
        val = data.get('val', btc_price - 1000)
        
        if btc_price > vah:
            score += 2.0  # Above value area
        elif btc_price < val:
            score -= 2.0  # Below value area
        
        return max(-10, min(10, score))
    
    def calculate_price_action_score(self, data: Dict[str, Any]) -> float:
        """
        Calculate price action score.
        
        Range: -10 to +10
        """
        score = 0.0
        
        # Simple price action based on trends
        ema_trend = data.get('ema_trend', 'neutral')
        price_trend = data.get('price_trend', 'neutral')
        
        if ema_trend == 'bullish' and price_trend == 'up':
            score += 3.0
        elif ema_trend == 'bearish' and price_trend == 'down':
            score -= 3.0
        elif ema_trend != price_trend:
            pass  # Neutral on divergence
        
        # VWAP as dynamic support/resistance
        vwap_position = data.get('vwap_position', 'neutral')
        if vwap_position == 'above':
            score += 1.5
        elif vwap_position == 'below':
            score -= 1.5
        
        return max(-10, min(10, score))
    
    def calculate_key_level_score(self, data: Dict[str, Any]) -> float:
        """
        Calculate key level proximity score.
        
        Range: -10 to +10
        """
        score = 0.0
        
        btc_price = data.get('btc_price', 0)
        poc = data.get('poc', btc_price)
        vah = data.get('vah', btc_price + 1000)
        val = data.get('val', btc_price - 1000)
        vwap = data.get('vwap', btc_price)
        
        if btc_price == 0:
            return 0.0
        
        # Distance from POC (strong level)
        poc_dist_pct = abs(btc_price - poc) / btc_price * 100
        if poc_dist_pct < 0.5:
            score += 2.0 if btc_price > poc else -2.0
        
        # Distance from VWAP
        vwap_dist_pct = abs(btc_price - vwap) / btc_price * 100
        if vwap_dist_pct < 0.3:
            score += 1.0 if btc_price > vwap else -1.0
        
        # Position in value area
        if val < btc_price < vah:
            pass  # Inside value area, neutral
        elif btc_price > vah:
            score += 2.0  # Breakout above
        elif btc_price < val:
            score -= 2.0  # Breakdown below
        
        return max(-10, min(10, score))
    
    def calculate_tpi(self, data: Dict[str, Any]) -> float:
        """
        Calculate Trading Performance Index (TPI).
        
        Combines multiple factors into a single index.
        Range: 0 to 20
        """
        tpi = 10.0  # Start neutral
        
        # OI health
        oi_total = data.get('oi_total', 22)
        if 20 <= oi_total <= 25:
            tpi += 1.0  # Healthy OI range
        
        # Funding health
        funding = data.get('funding_rate', 0)
        if -0.01 <= funding <= 0.03:
            tpi += 1.0  # Healthy funding
        
        # CVD alignment
        cvd_futures = data.get('cvd_futures', 0)
        cvd_spot = data.get('cvd_spot', 0)
        if (cvd_futures > 0 and cvd_spot > 0) or (cvd_futures < 0 and cvd_spot < 0):
            tpi += 0.5  # CVD aligned
        
        # ETF positive
        etf_flow = data.get('etf_flow', 0)
        if etf_flow > 0:
            tpi += 0.5
        
        # Trend alignment
        ema_trend = data.get('ema_trend', 'neutral')
        price_trend = data.get('price_trend', 'neutral')
        if ema_trend == 'bullish' and price_trend == 'up':
            tpi += 1.0
        elif ema_trend == 'bearish' and price_trend == 'down':
            tpi -= 1.0
        
        return max(0, min(20, tpi))
    
    def calculate_all_scores(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Calculate all scores and combine into total score.
        
        Returns:
            Dictionary with all individual scores and total score.
        """
        direction = self.calculate_direction_score(data)
        momentum = self.calculate_momentum_score(data)
        breakout = self.calculate_breakout_score(data)
        price_action = self.calculate_price_action_score(data)
        key_level = self.calculate_key_level_score(data)
        tpi = self.calculate_tpi(data)
        
        # Weighted total
        total = (
            direction * self.weights['direction'] +
            momentum * self.weights['momentum'] +
            breakout * self.weights['breakout'] +
            price_action * self.weights['price_action'] +
            key_level * self.weights['key_level']
        ) / sum(self.weights.values())
        
        # Synergy: how well indicators align
        scores = [direction, momentum, breakout, price_action, key_level]
        positive = sum(1 for s in scores if s > 0)
        negative = sum(1 for s in scores if s < 0)
        synergy = abs(positive - negative) / len(scores) * 10
        
        # Strength: average magnitude
        strength = sum(abs(s) for s in scores) / len(scores)
        
        return {
            'direction_score': round(direction, 2),
            'momentum_score': round(momentum, 2),
            'breakout_score': round(breakout, 2),
            'price_action_score': round(price_action, 2),
            'key_level_score': round(key_level, 2),
            'total_score': round(total, 2),
            'tpi': round(tpi, 1),
            'synergy': round(synergy, 1),
            'strength': round(strength, 1)
        }
