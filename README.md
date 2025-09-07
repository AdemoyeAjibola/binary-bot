# TradingView → Telegram Binary Options Signal Bot

This repository is tailored for binary-only signals (CALL/PUT) to be used manually with a binary broker (e.g., Pocket Option).

## What it does
- Receives TradingView webhook alerts (JSON) and verifies a secret.
- Runs a 12-strategy ensemble on recent candles to produce a binary CALL/PUT signal and a confidence score.
- Stores every signal and per-strategy votes in SQLite for auditing and backtesting.
- Schedules automatic evaluation at expiry and marks WIN or LOSS (binary outcome).
- Sends hourly/daily/weekly reports with nested hourly/daily breakdowns and per-strategy leaderboards.
- Provides `/menu` in Telegram with buttons for on-demand reports and `/stop`/`/start` to pause/resume forwarding.

## Setup
1. Copy `.env.sample` to `.env` and fill values (TELEGRAM_TOKEN, CHAT_ID, WEBHOOK_SECRET, etc).
2. Install dependencies: `pip install -r requirements.txt`
3. Run: `python bot.py` (or `gunicorn bot:app` in production)
4. Create TradingView alerts with webhook URL `https://<your-host>/webhook` and put a single-line JSON message such as:
   `{"action":"CALL","pair":"EURUSDT","price":"{{close}}","timeframe":"1m","secret":"YOUR_SECRET"}`

## Notes
- Expiry: include `expiry_epoch` (unix seconds) in the alert if you want explicit expiry; otherwise the bot derives expiry from `timeframe` placeholder.
- For best indicator accuracy, map TradingView symbols to Binance-style pairs (e.g., EURUSDT). If your broker uses different symbols, adapt accordingly.
- The bot uses Binance public APIs for candles and price lookup. For production, consider using a paid data feed or broker API that matches your trading asset's exact tick data.
