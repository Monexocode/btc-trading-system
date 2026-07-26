#!/usr/bin/env python3
"""
BTC Trading System v6 - Main Entry Point
Converts TradingView Pine Script logic to Python with Supabase integration.
"""

import argparse
import time
from datetime import datetime
from typing import Dict, Any, Optional

from data_fetcher import DataFetcher
from scoring_engine import ScoringEngine
from supabase_client import SupabaseClient


ENTRY_MODES = {
    'full_system':     {'threshold': 5.0, 'risk_reward': 1.5, 'description': 'Full system with all indicators'},
    'scalper':         {'threshold': 3.0, 'risk_reward': 1.0, 'description': 'Quick entries, tight stops'},
    'swing_trader':    {'threshold': 5.0, 'risk_reward': 2.0, 'description': 'Longer holds, wider stops'},
    'breakout_hunter': {'threshold': 4.0, 'risk_reward': 1.5, 'description': 'Focus on breakout signals'},
    'conservative':    {'threshold': 7.0, 'risk_reward': 2.0, 'description': 'High confidence entries only'},
    'price_action':    {'threshold': 3.0, 'risk_reward': 1.5, 'description': 'Pure price action focus'},
    'momentum':        {'threshold': 4.0, 'risk_reward': 1.5, 'description': 'Momentum-based entries'},
    'volume_profile':  {'threshold': 4.0, 'risk_reward': 1.5, 'description': 'Volume profile focus'},
}


def _fmt(value, fmt, fallback='N/A'):
    if value is None:
        return fallback
    return format(value, fmt)


def determine_signal(score: float, threshold: float) -> str:
    if score >= threshold:
        return "BUY"
    elif score <= -threshold:
        return "SELL"
    return "STALL"


def run_pipeline(mode: str = 'full_system', dry_run: bool = False,
                 upload_signals: bool = True) -> Dict[str, Any]:
    print(f"\n{'='*60}")
    print(f"BTC Trading System v6 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Mode: {mode} | Dry Run: {dry_run} | Signals: {upload_signals}")
    print(f"{'='*60}\n")

    mode_config = ENTRY_MODES.get(mode, ENTRY_MODES['full_system'])
    threshold   = mode_config['threshold']

    fetcher = DataFetcher()
    scorer  = ScoringEngine()
    db      = None if dry_run else SupabaseClient()

    results = {
        'timestamp':   datetime.now().isoformat(),
        'mode':        mode,
        'mode_config': mode_config,
        'success':     False,
        'errors':      [],
    }

    try:
        print("Fetching market data...")
        market_data = fetcher.fetch_all_data()
        results['market_data'] = market_data
        print(f"   BTC Price: ${_fmt(market_data.get('btc_price'), ',.2f')}")
        print(f"   Open Interest: ${_fmt(market_data.get('oi_total'), '.2f')}B")
        print(f"   CME OI: ${_fmt(market_data.get('oi_cme'), '.2f')}B")
        print(f"   Funding Rate: {_fmt(market_data.get('funding_rate'), '.6f')}")

        print("\nCalculating scores...")
        scores = scorer.calculate_all_scores(market_data)
        results['scores'] = scores

        total_score = scores.get('total_score', 0)
        print(f"   Total Score: {total_score:.2f}")
        print(f"   Direction: {scores.get('direction_score', 0):.2f}")
        print(f"   Momentum: {scores.get('momentum_score', 0):.2f}")
        print(f"   Breakout: {scores.get('breakout_score', 0):.2f}")

        signal = determine_signal(total_score, threshold)
        results['signal'] = signal
        print(f"\nSignal: {signal} (threshold: ±{threshold})")

        if not dry_run:
            print("\nUploading to Supabase...")

            # Build daily record — macro/price/CVD/OI/liqs only.
            # All technical indicator fields (poc, vwap, ema_trend, kc_bb_squeeze,
            # kc_positioning, bb_positioning, vah_val, price_oi, vol_1_5, normal_box,
            # breaking_point, vwap_band, swing levels, etc.) are owned by
            # trading-tpi-pipeline and must NOT be written here to avoid collisions.
            daily_data = {
                'btc':             market_data.get('btc_price'),
                'oi':              market_data.get('oi_total'),
                'cme_oi':          market_data.get('oi_cme'),
                'funding':         market_data.get('funding_rate'),
                'cvd_futs':        market_data.get('cvd_futures'),
                'cvd_spot':        market_data.get('cvd_spot'),
                'liqs_prev':       market_data.get('liquidations_24h'),
                'liqs_prev_price': market_data.get('liqs_price'),
                'etf':             market_data.get('etf_flow'),
                'es':              market_data.get('es'),
                'nq':              market_data.get('nq'),
                'dxy':             market_data.get('dxy'),
                'gold':            market_data.get('gold'),
                'vix':             market_data.get('vix'),
                'bvix':            market_data.get('bviv'),
                'btc_d':           market_data.get('btc_dominance'),
            }
            # Remove None values to avoid overwriting tpi-pipeline fields
            daily_data = {k: v for k, v in daily_data.items() if v is not None}

            daily_result = db.upsert_daily_data(daily_data)
            results['daily_upload'] = daily_result
            print(f"   Daily data: {'✓' if daily_result else '✗'}")

            if upload_signals:
                signal_result = db.upload_15min_signal(
                    btc_price=market_data.get('btc_price', 0),
                    total_score=total_score,
                    signal=signal,
                    entry_mode=mode,
                    direction_score=scores.get('direction_score', 0),
                    momentum_score=scores.get('momentum_score', 0),
                    breakout_score=scores.get('breakout_score', 0),
                    price_action_score=scores.get('price_action_score', 0),
                    key_level_score=scores.get('key_level_score', 0),
                    daily_tpi=scores.get('tpi'),
                    daily_oi_trend=market_data.get('oi_trend', 'neutral'),
                    notes=f"Auto-generated via {mode} mode",
                )
                results['signal_upload'] = signal_result
                print(f"   15-min signal: {'✓' if signal_result else '✗'}")
        else:
            print("\nDry run - skipping Supabase upload")

        results['success'] = True
        print("\nPipeline completed successfully!")

    except Exception as e:
        results['errors'].append(str(e))
        print(f"\nError: {e}")
        raise

    return results


def main():
    parser = argparse.ArgumentParser(description='BTC Trading System v6')
    parser.add_argument('--mode', '-m', choices=list(ENTRY_MODES.keys()), default='full_system')
    parser.add_argument('--dry-run', '-d', action='store_true', help='Run without uploading to Supabase')
    parser.add_argument('--no-signals', action='store_true')
    parser.add_argument('--continuous', '-c', action='store_true')
    parser.add_argument('--list-modes', action='store_true')
    args = parser.parse_args()

    if args.list_modes:
        print("\nAvailable Entry Modes:")
        for name, config in ENTRY_MODES.items():
            print(f"  {name}: threshold={config['threshold']} — {config['description']}")
        return

    if args.continuous:
        print("Running in continuous mode (every 30 minutes)")
        while True:
            try:
                run_pipeline(mode=args.mode, dry_run=args.dry_run,
                             upload_signals=not args.no_signals)
                print("\nWaiting 30 minutes...")
                time.sleep(1800)
            except KeyboardInterrupt:
                print("\nStopped by user")
                break
            except Exception as e:
                print(f"\nError: {e} — retrying in 5 min")
                time.sleep(300)
    else:
        run_pipeline(mode=args.mode, dry_run=args.dry_run,
                     upload_signals=not args.no_signals)


if __name__ == '__main__':
    main()
