# Polymarket ↔ Opinion Arbitrage Toolkit

A Python toolkit for discovering and monitoring **cross-platform arbitrage opportunities** between prediction markets on:

* **Polymarket**
* **Opinion**

The system fetches markets, pairs equivalent events across platforms, and continuously monitors price spreads to detect potential **risk-free arbitrage opportunities**.

---

# How It Works

The system consists of three stages:

1. **Market Collection**
2. **Market Matching**
3. **Spread Monitoring**

```text
find_arbitrage.py
        │
        ▼
fetched_markets.json
        │
        ▼
pairs_strict.py / gpt.py
        │
        ▼
pairs.json
        │
        ▼
monitor.py
        │
        ▼
Telegram alerts
```

---

# Features

* Fetch thousands of prediction markets
* Match equivalent markets across platforms
* Detect price discrepancies
* Continuous monitoring
* Telegram notifications
* Highly parallel monitoring
* Fully deterministic matching option (no AI required)

---

# Quick Start

## 1️⃣ Clone the repository

```bash
git clone https://github.com/YOUR_USERNAME/prediction-market-arbitrage.git
cd prediction-market-arbitrage
```

---

## 2️⃣ Install dependencies

```bash
pip install requests
```

Python **3.9+ recommended**

---

## 3️⃣ Fetch markets

```bash
export OPINION_API_KEY="your_api_key"

python find_arbitrage.py
```

Creates:

```text
fetched_markets.json
```

---

## 4️⃣ Match markets

### Recommended (deterministic)

```bash
python pairs_strict.py
```

Creates:

```text
pairs_strict.json
```

Rename or copy:

```bash
cp pairs_strict.json pairs.json
```

---

### Alternative (GPT matching)

```bash
export OPENAI_API_KEY="your_key"

python gpt.py
```

---

## 5️⃣ Start monitoring spreads

```bash
export TELEGRAM_BOT_TOKEN="your_bot_token"
export TELEGRAM_CHAT_ID="your_chat_id"

python monitor.py
```

The monitor will run continuously and send alerts when spreads exceed the threshold.

---

# Example Alert

Example Telegram notification:

```
📈 Spread Alert: 5.21%

Will Bitcoin reach $100k in 2025?

Leg: BUY Poly YES + Opin NO
Cost: 0.95

Poly YES/NO: 0.45 / 0.55
Opin YES/NO: 0.50 / 0.50
```

---

# File Descriptions

## `find_arbitrage.py`

Fetches markets from both platforms.

Output:

```
fetched_markets.json
```

Contains all market metadata required for later steps.

---

## `pairs_strict.py`

Creates **strict one-to-one market matches** using normalized text comparison.

Output:

```
pairs_strict.json
```

Format:

```json
[
  {
    "market1_platform": "polymarket",
    "market1_id": "...",
    "market1_name": "...",
    "market2_platform": "opinion",
    "market2_id": "...",
    "market2_name": "..."
  }
]
```

---

## `gpt.py` (optional)

Uses GPT to detect **exact market equivalence** when strict text matching fails.

Requires:

```
OPENAI_API_KEY
```

---

## `monitor.py`

Continuously monitors spreads between paired markets.

Features:

* parallel monitoring
* Telegram alerts
* price caching
* alert suppression logic

Outputs:

```
monitor_state.json
monitor_cache.json
```

---

# Environment Variables

| Variable                   | Description                       |
| -------------------------- | --------------------------------- |
| OPINION_API_KEY            | Opinion API key                   |
| OPENAI_API_KEY             | (Optional) GPT matching           |
| TELEGRAM_BOT_TOKEN         | Telegram bot token                |
| TELEGRAM_CHAT_ID           | Telegram chat ID                  |
| MONITOR_INTERVAL_S         | Monitoring interval (default 30s) |
| SPREAD_ALERT_THRESHOLD_PCT | Alert threshold                   |
| SPREAD_RENOTIFY_STEP_PCT   | Minimum increase for repeat alert |
| WORKERS                    | Monitoring parallelism            |

---

# Performance

Typical performance:

| Task             | Size          |
| ---------------- | ------------- |
| Markets fetched  | ~12,000       |
| Pairs detected   | 50–500        |
| Monitoring cycle | ~5–10 seconds |

---

# Important Disclaimer

Prediction market arbitrage is **not guaranteed risk-free**.

Markets that appear identical may differ in:

* resolution rules
* cutoff dates
* wording definitions
* data sources

Always **verify markets manually** before trading.

This software is provided **for research and educational purposes only**.

---

# Contributing

Pull requests and improvements are welcome.

Possible improvements:

* semantic market matching
* liquidity-aware trade sizing
* automated trade execution
* web dashboard
* historical spread analysis

---

# License

MIT License

Feel free to use, modify, and distribute.
