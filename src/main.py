#!/usr/bin/env python3
"""
BTC Trading System v6 - Main Entry Point
"""

import argparse
import time
from datetime import datetime
from typing import Dict, Any, Optional

from data_fetcher import DataFetcher
from scoring_engine import ScoringEngine
from airtable_client import AirtableClient


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

_EMA_TREND_MAP = {'bullish': 1, 'neutral': 0, 'bearish': -1}


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

    fetcher  = DataFetcher()
    scorer   = ScoringEngine()
    airtable = None if dry_run else AirtableClient()

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
            print("\nUploading to Airtable...")

            btc   = market_data.get('btc_price', 0)
            vah_v = market_data.get('vah', 0) or 0
            val_v = market_data.get('val', 0) or 0
            vah_val_num = 1 if btc > vah_v else -1 if btc < val_v else 0

            cvd_futs    = market_data.get('cvd_futures')
            vwap_pos    = market_data.get('vwap_position', '')
            if cvd_futs is not None:
                price_oi_val = 1 if (cvd_futs > 0 and vwap_pos == 'above') else \
                              -1 if (cvd_futs < 0 and vwap_pos == 'below') else 0
            else:
                price_oi_val = 0

            daily_data = {
                'btc':           market_data.get('btc_price'),
                'oi':            market_data.get('oi_total'),
                'cme_oi':        market_data.get('oi_cme'),
                'funding':       market_data.get('funding_rate'),
                'cvd_futs':      market_data.get('cvd_futures'),
                'cvd_spot':      market_data.get('cvd_spot'),
                'liqs_prev':     market_data.get('liquidations_24h'),
                'liqs_prev_price': market_data.get('liqs_price'),
                # poc/vwap/vwap_band omitted: tpi-pipeline owns those fields as normalized scores
                'vah_val':       vah_val_num,
                'price_oi':      price_oi_val,
                'ema_trend':     _EMA_TREND_MAP.get(market_data.get('ema_trend', 'neutral'), 0),
                'kc_bb_squeeze': 1 if market_data.get('squeeze') else 0,
                'kc_positioning': market_data.get('kc_pos_value', 0),
                'bb_positioning': market_data.get('bb_pos_value', 0),
                'es':   market_data.get('es'),
                'nq':   market_data.get('nq'),
                'dxy':  market_data.get('dxy'),
                'gold': market_data.get('gold'),
                'vix':  market_data.get('vix'),
                'bvix': market_data.get('bviv'),
                'btc_d': market_data.get('btc_dominance'),
                'strength_tw': scores.get('tpi'),
                'synergy_tw':  scores.get('synergy'),
                'vol_1_5': 1 if market_data.get('volume_spike') else 0,
                'normal_box': 1 if market_data.get('box_high') is not None else 0,
                'breaking_point': (
                    1 if any([market_data.get('box_break_up'), market_data.get('swing_high_break'),
                              market_data.get('pdh_break'), market_data.get('pwh_break')])
                    else -1 if any([market_data.get('box_break_down'), market_data.get('swing_low_break'),
                                    market_data.get('pdl_break'), market_data.get('pwl_break')])
                    else 0
                ),
            }
            daily_data = {k: v for k, v in daily_data.items() if v is not None}

            daily_result = airtable.upsert_daily_data(daily_data)
            results['daily_upload'] = daily_result
            print(f"   Daily data: {'✓' if daily_result else '✗'}")

            if upload_signals:
                signal_result = airtable.upload_15min_signal(
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
            print("\nDry run - skipping Airtable upload")

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
    parser.add_argument('--dry-run', '-d', action='store_true')
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
