# BTC Trading System v6

Automated BTC trading signals with Pine Script v6 → Python conversion, Airtable integration, and GitHub Actions automation.

## Features

- **Daily Macro Data**: Fetches OI, Funding, CVD, Liquidations from Coinalyze
- **15-min Signal Generation**: BUY/SELL/STALL signals every 30 minutes
- **Dual Airtable Tables**: Daily data + Intra day signals
- **9 Entry Mode Presets**: scalper, swing_trader, breakout_hunter, conservative, etc.
- **GitHub Actions**: Automated runs every 30 minutes

## Setup

1. Clone this repo
2. Install dependencies: `pip install -r requirements.txt`
3. Set environment variable: `AIRTABLE_API_KEY`
4. Run: `python src/main.py --mode scalper`

## Usage

```bash
# Single run
python src/main.py --mode scalper

# Continuous (every 30 min)
python src/main.py --continuous

# Dry run (no upload)
python src/main.py --dry-run

# Daily data only (no 15-min signals)
python src/main.py --no-signals
```

## Entry Modes

| Mode | Threshold | R:R |
|------|-----------|-----|
| full_system | 5.0 | 1.5 |
| scalper | 3.0 | 1.0 |
| swing_trader | 5.0 | 2.0 |
| breakout_hunter | 4.0 | 1.5 |
| conservative | 7.0 | 2.0 |

## License

MIT
