#!/usr/bin/env python3
"""
BTC Trading System v6 - Main Entry Point
Converts TradingView Pine Script logic to Python with Airtable integration.
"""

import argparse
import time
from datetime import datetime
from typing import Dict, Any, Optional

from data_fetcher import DataFetcher
from scoring_engine import ScoringEngine
from airtable_client import AirtableClient


# Entry mode presets with their configurations
ENTRY_MODES = {
    'full_system': {
        'threshold': 5.0,
        'risk_reward': 1.5,
        'description': 'Full system with all indicators'
    },
    'scalper': {
        'threshold': 3.0,
        'risk_reward': 1.0,
        'description': 'Quick entries, tight stops'
    },
    'swing_trader': {
        'threshold': 5.0,
        'risk_reward': 2.0,
        'description': 'Longer holds, wider stops'
    },
    'breakout_hunter': {
        'threshold': 4.0,
        'risk_reward': 1.5,
        'description': 'Focus on breakout signals'
    },
    'conservative': {
        'threshold': 7.0,
        'risk_reward': 2.0,
        'description': 'High confidence entries only'
    },
    'price_action': {
        'threshold': 5.0,
        'risk_reward': 1.5,
        'description': 'Pure price action focus'
    },
    'momentum': {
        'threshold': 5.0,
        'risk_reward': 1.5,
        'description': 'Momentum-based entries'
    },
    'volume_profile': {
        'threshold': 5.0,
        'risk_reward': 1.5,
        'description': 'Volume profile focus'
    }
}


def determine_signal(score: float, threshold: float) -> str:
    """Determine BUY/SELL/STALL based on score and threshold."""
    if score >= threshold:
        return "BUY"
    elif score <= -threshold:
        return "SELL"
    else:
        return "STALL"


def run_pipeline(
    mode: str = 'full_system',
    dry_run: bool = False,
    upload_signals: bool = True
) -> Dict[str, Any]:
    """
    Run the complete trading pipeline.
    
    Args:
        mode: Entry mode preset name
        dry_run: If True, don't upload to Airtable
        upload_signals: If True, upload 15-min signals to Intra day table
    
    Returns:
        Dictionary with pipeline results
    """
    print(f"\n{'='*60}")
    print(f"BTC Trading System v6 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Mode: {mode} | Dry Run: {dry_run} | Signals: {upload_signals}")
    print(f"{'='*60}\n")
    
    # Get mode configuration
    mode_config = ENTRY_MODES.get(mode, ENTRY_MODES['full_system'])
    threshold = mode_config['threshold']
    
    # Initialize components
    fetcher = DataFetcher()
    scorer = ScoringEngine()
    airtable = AirtableClient()
    
    results = {
        'timestamp': datetime.now().isoformat(),
        'mode': mode,
        'mode_config': mode_config,
        'success': False,
        'errors': []
    }
    
    try:
        # Step 1: Fetch market data
        print("📊 Fetching market data...")
        market_data = fetcher.fetch_all_data()
        results['market_data'] = market_data
        print(f"   BTC Price: ${market_data.get('btc_price', 'N/A'):,.2f}")
        print(f"   Open Interest: ${market_data.get('oi_total', 'N/A'):.2f}B")
        print(f"   Funding Rate: {market_data.get('funding_rate', 'N/A'):.4f}%")
        
        # Step 2: Calculate scores
        print("\n🧮 Calculating scores...")
        scores = scorer.calculate_all_scores(market_data)
        results['scores'] = scores
        
        total_score = scores.get('total_score', 0)
        print(f"   Total Score: {total_score:.2f}")
        print(f"   Direction: {scores.get('direction_score', 0):.2f}")
        print(f"   Momentum: {scores.get('momentum_score', 0):.2f}")
        print(f"   Breakout: {scores.get('breakout_score', 0):.2f}")
        
        # Step 3: Determine signal
        signal = determine_signal(total_score, threshold)
        results['signal'] = signal
        print(f"\n🚦 Signal: {signal} (threshold: ±{threshold})")
        
        # Step 4: Upload to Airtable
        if not dry_run:
            print("\n☁️ Uploading to Airtable...")
            
            # Upload daily data
            daily_result = airtable.upload_to_airtable(market_data, scores)
            results['daily_upload'] = daily_result
            print(f"   Daily data: {'✓' if daily_result else '✗'}")
            
            # Upload 15-min signal if enabled
            if upload_signals:
                signal_data = {
                    'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M'),
                    'btc_price': market_data.get('btc_price', 0),
                    'signal': signal,
                    'total_score': total_score,
                    'entry_mode': mode,
                    'direction_score': scores.get('direction_score', 0),
                    'momentum_score': scores.get('momentum_score', 0),
                    'breakout_score': scores.get('breakout_score', 0),
                    'price_action_score': scores.get('price_action_score', 0),
                    'key_level_score': scores.get('key_level_score', 0),
                    'daily_tpi': scores.get('tpi', 0),
                    'daily_oi_trend': market_data.get('oi_trend', 'neutral'),
                    'notes': f"Auto-generated via {mode} mode"
                }
                signal_result = airtable.upload_15min_signal(signal_data)
                results['signal_upload'] = signal_result
                print(f"   15-min signal: {'✓' if signal_result else '✗'}")
        else:
            print("\n⏸️ Dry run - skipping Airtable upload")
        
        results['success'] = True
        print(f"\n✅ Pipeline completed successfully!")
        
    except Exception as e:
        results['errors'].append(str(e))
        print(f"\n❌ Error: {e}")
        raise
    
    return results


def main():
    """Main entry point with CLI argument parsing."""
    parser = argparse.ArgumentParser(
        description='BTC Trading System v6 - Automated trading signals'
    )
    parser.add_argument(
        '--mode', '-m',
        choices=list(ENTRY_MODES.keys()),
        default='full_system',
        help='Entry mode preset (default: full_system)'
    )
    parser.add_argument(
        '--dry-run', '-d',
        action='store_true',
        help='Run without uploading to Airtable'
    )
    parser.add_argument(
        '--no-signals',
        action='store_true',
        help='Skip 15-min signal uploads (daily data only)'
    )
    parser.add_argument(
        '--continuous', '-c',
        action='store_true',
        help='Run continuously every 30 minutes'
    )
    parser.add_argument(
        '--list-modes',
        action='store_true',
        help='List all available entry modes and exit'
    )
    
    args = parser.parse_args()
    
    # List modes and exit
    if args.list_modes:
        print("\nAvailable Entry Modes:")
        print("-" * 50)
        for name, config in ENTRY_MODES.items():
            print(f"  {name}:")
            print(f"    Threshold: {config['threshold']}")
            print(f"    R:R: {config['risk_reward']}")
            print(f"    {config['description']}")
            print()
        return
    
    # Run pipeline
    if args.continuous:
        print("🔄 Running in continuous mode (every 30 minutes)")
        print("   Press Ctrl+C to stop\n")
        while True:
            try:
                run_pipeline(
                    mode=args.mode,
                    dry_run=args.dry_run,
                    upload_signals=not args.no_signals
                )
                print(f"\n⏳ Waiting 30 minutes until next run...")
                time.sleep(1800)  # 30 minutes
            except KeyboardInterrupt:
                print("\n\n👋 Stopped by user")
                break
            except Exception as e:
                print(f"\n⚠️ Error in continuous run: {e}")
                print("   Retrying in 5 minutes...")
                time.sleep(300)
    else:
        run_pipeline(
            mode=args.mode,
            dry_run=args.dry_run,
            upload_signals=not args.no_signals
        )


if __name__ == '__main__':
    main()
