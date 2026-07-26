#!/usr/bin/env python3
"""
Scoring Engine for BTC Trading System
Implements the Pine Script v6 "BTC Multi-Box Breakout Strategy v4" 5-category
scoring system ported to Python.
"""

from typing import Dict, Any


class ScoringEngine:
    """
    Calculates trading scores matching the Pine Script 5-category system.

    Category weights (Full System):
        direction:    2.0  — trend alignment across price levels and EMAs
        momentum:     1.5  — band/EMA expansion and candle structure
        breakout:     1.5  — squeeze + box proximity
        price_action: 1.0  — swing/PD/PW level breaks
        key_level:    1.0  — price vs volume-profile levels (POC/VAH/VAL/VWAP)
    """

    def __init__(self):
        self.weights = {
            "direction":    2.0,
            "momentum":     1.5,
            "breakout":     1.5,
            "price_action": 1.0,
            "key_level":    1.0,
        }

    # ------------------------------------------------------------------ #
    # Direction  (range: -10 to +10)
    # ------------------------------------------------------------------ #

    def calculate_direction_score(self, data: Dict[str, Any]) -> float:
        """
        Pine Script direction indicators — price position relative to key EMAs,
        POC, value area, normal box, volume spike, and VWAP overextension.

        Per-indicator weights sum to 10 at maximum (all bullish):
            poc         ±1.5
            vahval      ±1.5  (±1.0 inside value area = 0)
            ema9        ±1.5
            ema21       ±1.5
            ema50       ±1.0
            box_break   ±1.5  (0 if no active box)
            vol_spike   +0.5  (amplifier, non-directional)
            vwap_rev    ±1.0  (reversal: overbought → bearish, oversold → bullish)
        """
        score = 0.0
        price = data.get("btc_price", 0)
        if price == 0:
            return 0.0

        # POC — price above = bullish
        poc = data.get("poc", price)
        score += 1.5 if price > poc else -1.5

        # Value area — above VAH = breakout (bullish), below VAL = breakdown
        vah = data.get("vah", price + 1)
        val = data.get("val", price - 1)
        if price > vah:
            score += 1.5
        elif price < val:
            score -= 1.5
        # inside value area: 0

        # EMA 9 / 21 / 50
        ema9 = data.get("ema9", price)
        ema21 = data.get("ema21", price)
        ema50 = data.get("ema50", price)
        score += 1.5 if price > ema9 else -1.5
        score += 1.5 if price > ema21 else -1.5
        score += 1.0 if price > ema50 else -1.0

        # Normal box breakout
        if data.get("box_break_up"):
            score += 1.5
        elif data.get("box_break_down"):
            score -= 1.5

        # Volume spike (non-directional amplifier — adds only in existing direction)
        if data.get("volume_spike"):
            score += 0.5

        # VWAP overextension reversal signal
        vwap_pos = data.get("vwap_pos_percent", 50) or 50
        if vwap_pos >= 80:
            score -= 1.0   # overbought relative to daily VWAP band
        elif vwap_pos <= 20:
            score += 1.0   # oversold relative to daily VWAP band

        return max(-10.0, min(10.0, score))

    # ------------------------------------------------------------------ #
    # Momentum  (range: -10 to +10)
    # ------------------------------------------------------------------ #

    def calculate_momentum_score(self, data: Dict[str, Any]) -> float:
        """
        Pine Script momentum indicators — EMA alignment, price vs KC/BB/VWAP
        stdev bands, and candle close structure.

        Per-indicator weights sum to 10 at maximum:
            ema_cross   ±2.0
            kc_break    ±2.0  (0 if inside)
            bb_break    ±2.0  (0 if inside)
            vwap_band   ±2.0  (0 if inside)
            candle_pos  ±2.0  (0 if neutral)

        Legacy CVD/liquidation/ETF fields (Velo-patched) still included at
        reduced weight so Velo data improves signal when available.
        """
        score = 0.0

        # EMA 9 vs 21 — cross direction
        ema9 = data.get("ema9")
        ema21 = data.get("ema21")
        if ema9 is not None and ema21 is not None:
            score += 2.0 if ema9 >= ema21 else -2.0

        # Price vs Keltner Channel
        price = data.get("btc_price", 0)
        kc_upper = data.get("kc_upper")
        kc_lower = data.get("kc_lower")
        if kc_upper is not None and kc_lower is not None and price:
            if price > kc_upper:
                score += 2.0
            elif price < kc_lower:
                score -= 2.0
            # inside KC: 0

        # Price vs Bollinger Bands
        bb_upper = data.get("bb_upper")
        bb_lower = data.get("bb_lower")
        if bb_upper is not None and bb_lower is not None and price:
            if price > bb_upper:
                score += 2.0
            elif price < bb_lower:
                score -= 2.0

        # Price vs VWAP stdev band
        vwap_upper = data.get("vwap_upper_band")
        vwap_lower = data.get("vwap_lower_band")
        if vwap_upper is not None and vwap_lower is not None and price:
            if price > vwap_upper:
                score += 2.0
            elif price < vwap_lower:
                score -= 2.0

        # Candle close position
        candle_pos = data.get("candle_close_position", "neutral")
        if candle_pos == "upper":
            score += 2.0
        elif candle_pos == "lower":
            score -= 2.0

        # Velo-patched fields (weight reduced; None-safe via `or 0`)
        cvd_futures = data.get("cvd_futures") or 0
        cvd_spot = data.get("cvd_spot") or 0
        if cvd_futures > 200:
            score += 1.0
        elif cvd_futures < -200:
            score -= 1.0
        if cvd_spot > 100:
            score += 0.5
        elif cvd_spot < -100:
            score -= 0.5

        liq_ratio = data.get("liquidation_ratio") or 1
        if liq_ratio < 0.67:
            score += 0.5   # more shorts liquidated → bullish
        elif liq_ratio > 1.5:
            score -= 0.5   # more longs liquidated → bearish

        etf_flow = data.get("etf_flow") or 0
        if etf_flow > 100:
            score += 1.0
        elif etf_flow < -100:
            score -= 1.0

        return max(-10.0, min(10.0, score))

    # ------------------------------------------------------------------ #
    # Breakout  (range: -10 to +10)
    # ------------------------------------------------------------------ #

    def calculate_breakout_score(self, data: Dict[str, Any]) -> float:
        """
        Pine Script breakout indicators — volatility squeeze and price proximity
        to the normal consolidation box edges.

        Weights:
            squeeze      +5.0  (compression → impending move, directionally neutral)
            box_near_top ±5.0  (near top = +5 bullish, near bottom = -5 bearish)
        """
        score = 0.0

        # Volatility squeeze (BB contracting inside KC + price near KC edge)
        if data.get("squeeze"):
            score += 5.0

        # Price proximity to normal box edge
        if data.get("near_box_top"):
            score += 5.0
        elif data.get("near_box_bottom"):
            score -= 5.0

        return max(-10.0, min(10.0, score))

    # ------------------------------------------------------------------ #
    # Price Action  (range: -10 to +10)
    # ------------------------------------------------------------------ #

    def calculate_price_action_score(self, data: Dict[str, Any]) -> float:
        """
        Pine Script price action indicators — pivot swing level breaks and
        previous day/week high-low breakouts.

        Weights:
            swing_high_break  +4.0
            swing_low_break   -4.0
            pdh_break         +2.0
            pdl_break         -2.0
            pwh_break         +3.0
            pwl_break         -3.0
        """
        score = 0.0

        if data.get("swing_high_break"):
            score += 4.0
        if data.get("swing_low_break"):
            score -= 4.0

        if data.get("pdh_break"):
            score += 2.0
        if data.get("pdl_break"):
            score -= 2.0

        if data.get("pwh_break"):
            score += 3.0
        if data.get("pwl_break"):
            score -= 3.0

        return max(-10.0, min(10.0, score))

    # ------------------------------------------------------------------ #
    # Key Level  (range: -10 to +10)
    # ------------------------------------------------------------------ #

    def calculate_key_level_score(self, data: Dict[str, Any]) -> float:
        """
        Price proximity to volume-profile levels: POC, VWAP, VAH/VAL.
        Disabled (returns 0.0) for the Full System entry mode per Pine Script.
        Kept for potential use in other modes.
        """
        score = 0.0
        price = data.get("btc_price", 0)
        if price == 0:
            return 0.0

        poc = data.get("poc", price)
        vwap = data.get("vwap", price)
        vah = data.get("vah", price + 1000)
        val = data.get("val", price - 1000)

        poc_dist_pct = abs(price - poc) / price * 100
        if poc_dist_pct < 0.5:
            score += 2.0 if price > poc else -2.0

        vwap_dist_pct = abs(price - vwap) / price * 100
        if vwap_dist_pct < 0.3:
            score += 1.0 if price > vwap else -1.0

        if price > vah:
            score += 2.0
        elif price < val:
            score -= 2.0

        return max(-10.0, min(10.0, score))

    # ------------------------------------------------------------------ #
    # TPI  (range: 0 to 20)
    # ------------------------------------------------------------------ #

    def calculate_tpi(self, data: Dict[str, Any]) -> float:
        """
        Trading Performance Index — macro health gauge, range 0–20.
        Neutral baseline = 10.
        """
        tpi = 10.0

        # OI health (Binance perp; Velo CME patched separately)
        oi_total = data.get("oi_total", 22)
        if oi_total and 20 <= oi_total <= 25:
            tpi += 1.0

        # Funding health
        funding = data.get("funding_rate", 0) or 0
        if -0.01 <= funding <= 0.03:
            tpi += 1.0
        elif funding > 0.05:
            tpi -= 0.5   # heavily skewed longs

        # CVD alignment (None-safe)
        cvd_futures = data.get("cvd_futures") or 0
        cvd_spot = data.get("cvd_spot") or 0
        if (cvd_futures > 0 and cvd_spot > 0) or (cvd_futures < 0 and cvd_spot < 0):
            tpi += 0.5

        # ETF positive (None-safe)
        etf_flow = data.get("etf_flow") or 0
        if etf_flow > 0:
            tpi += 0.5

        # EMA 9/21 alignment
        ema9 = data.get("ema9")
        ema21 = data.get("ema21")
        if ema9 is not None and ema21 is not None:
            if ema9 >= ema21:
                tpi += 1.0
            else:
                tpi -= 1.0

        # Price above/below EMA 50
        price = data.get("btc_price", 0)
        ema50 = data.get("ema50")
        if price and ema50:
            if price > ema50:
                tpi += 0.5
            else:
                tpi -= 0.5

        return max(0.0, min(20.0, tpi))

    # ------------------------------------------------------------------ #
    # Combined  (preserves existing output interface)
    # ------------------------------------------------------------------ #

    def calculate_all_scores(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Calculate all scores and return as a dictionary.
        Output keys are unchanged from v5 so Supabase upload code stays intact.
        """
        direction    = self.calculate_direction_score(data)
        momentum     = self.calculate_momentum_score(data)
        breakout     = self.calculate_breakout_score(data)
        price_action = self.calculate_price_action_score(data)
        key_level    = self.calculate_key_level_score(data)
        tpi          = self.calculate_tpi(data)

        # Weighted average — same formula as v5
        total = (
            direction    * self.weights["direction"] +
            momentum     * self.weights["momentum"] +
            breakout     * self.weights["breakout"] +
            price_action * self.weights["price_action"] +
            key_level    * self.weights["key_level"]
        ) / sum(self.weights.values())

        # Synergy: fraction of categories pointing the same way × 10
        scores = [direction, momentum, breakout, price_action, key_level]
        positive = sum(1 for s in scores if s > 0)
        negative = sum(1 for s in scores if s < 0)
        synergy = abs(positive - negative) / len(scores) * 10

        # Strength: average magnitude of category scores
        strength = sum(abs(s) for s in scores) / len(scores)

        return {
            "direction_score":    round(direction, 2),
            "momentum_score":     round(momentum, 2),
            "breakout_score":     round(breakout, 2),
            "price_action_score": round(price_action, 2),
            "key_level_score":    round(key_level, 2),
            "total_score":        round(total, 2),
            "tpi":                round(tpi, 1),
            "synergy":            round(synergy, 1),
            "strength":           round(strength, 1),
        }
