#!/usr/bin/env python3
"""
monitor.py - Polymarket vs Opinion spread monitor for a fixed set of matched pairs.

Reads:
  pairs.json  (JSON LIST of confirmed matches; each item has market1_id/market1_name/market2_id/market2_name)

Writes:
  monitor_state.json  (persists last alerted spread per pair key)
  monitor_cache.json  (caches Opinion token ids per opinion market id)

Env required:
  OPINION_API_KEY
  TELEGRAM_BOT_TOKEN
  TELEGRAM_CHAT_ID   (or hardcode)

Optional env:
  MONITOR_INTERVAL_S=30
  SPREAD_ALERT_THRESHOLD_PCT=2.0
  SPREAD_RENOTIFY_STEP_PCT=0.5
  WORKERS=8
  LOG_LEVEL=INFO|DEBUG
  DEBUG_FAIL_SAMPLES=6
"""

from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import Counter
import threading
import tempfile

import requests

from opinion_api import OpinionClient


# ==========================
# USER CONFIG
# ==========================
PAIRS_FILE = os.environ.get("PAIRS_FILE", "pairs.json")
STATE_FILE = os.environ.get("STATE_FILE", "monitor_state.json")
CACHE_FILE = os.environ.get("CACHE_FILE", "monitor_cache.json")

TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "").strip()
# Hardcode if you prefer:
# TELEGRAM_CHAT_ID = "123456789"

MONITOR_INTERVAL_S = float(os.environ.get("MONITOR_INTERVAL_S", "30"))
SPREAD_ALERT_THRESHOLD_PCT = float(os.environ.get("SPREAD_ALERT_THRESHOLD_PCT", "2.0"))
SPREAD_RENOTIFY_STEP_PCT = float(os.environ.get("SPREAD_RENOTIFY_STEP_PCT", "0.5"))
WORKERS = int(os.environ.get("WORKERS", "8"))

LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").strip().upper()
DEBUG_FAIL_SAMPLES = int(os.environ.get("DEBUG_FAIL_SAMPLES", "6"))

POLY_BASE_URL = "https://gamma-api.polymarket.com"

_JSON_WRITE_LOCK = threading.Lock()

# ==========================
# Logging
# ==========================
def log(msg: str, level: str = "INFO") -> None:
    levels = {"DEBUG": 10, "INFO": 20, "WARNING": 30, "ERROR": 40}
    cur = levels.get(LOG_LEVEL, 20)
    lvl = levels.get(level, 20)
    if lvl < cur:
        return
    ts = time.strftime("%H:%M:%S")
    print(f"[{ts}] {level}: {msg}", flush=True)


# ==========================
# Telegram
# ==========================
class TelegramNotifier:
    def __init__(self, bot_token: str, chat_id: str):
        if not bot_token:
            raise ValueError("Missing TELEGRAM_BOT_TOKEN")
        if not chat_id:
            raise ValueError("Missing TELEGRAM_CHAT_ID (set env or hardcode in script)")
        self.bot_token = bot_token
        self.chat_id = chat_id
        self.s = requests.Session()

    def send(self, text: str) -> None:
        url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
        payload = {
            "chat_id": self.chat_id,
            "text": text,
            "disable_web_page_preview": True,
        }
        r = self.s.post(url, json=payload, timeout=15)
        r.raise_for_status()


# ==========================
# Helpers
# ==========================
def atomic_write_json(path: str, obj: Any) -> None:
    """
    Thread-safe atomic JSON write:
    - uses a unique temp file in the same directory
    - serializes writers with a lock
    """
    dir_name = os.path.dirname(path) or "."
    os.makedirs(dir_name, exist_ok=True)

    with _JSON_WRITE_LOCK:
        fd, tmp_path = tempfile.mkstemp(prefix=os.path.basename(path) + ".", suffix=".tmp", dir=dir_name)
        try:
            with os.fdopen(fd, "w") as f:
                json.dump(obj, f, indent=2)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp_path, path)
        finally:
            # If something failed before replace, cleanup
            try:
                if os.path.exists(tmp_path):
                    os.remove(tmp_path)
            except Exception:
                pass

def load_json(path: str, default: Any) -> Any:
    if not os.path.exists(path):
        return default
    try:
        with open(path, "r") as f:
            return json.load(f)
    except Exception:
        return default

def best_spread_pct(poly_yes: float, poly_no: float, opin_yes: float, opin_no: float) -> Tuple[float, int, float]:
    best = (-1e9, 0, 0.0)

    cost1 = poly_yes + opin_no
    if 0 < cost1 < 1:
        prof1 = (1 - cost1) / cost1 * 100
        best = max(best, (prof1, 1, cost1), key=lambda x: x[0])

    cost2 = poly_no + opin_yes
    if 0 < cost2 < 1:
        prof2 = (1 - cost2) / cost2 * 100
        best = max(best, (prof2, 2, cost2), key=lambda x: x[0])

    return best


# ==========================
# Opinion: market detail
# ==========================
class OpinionClientPlus(OpinionClient):
    def market_detail(self, market_id: str) -> Dict[str, Any]:
        data = self._get(f"/market/{market_id}", params=None, timeout=15)
        result = data.get("result") or {}
        # Docs: result.data contains the market fields (yesTokenId/noTokenId/etc)
        if isinstance(result, dict) and isinstance(result.get("data"), dict):
            return result["data"]
        return result  # fallback if API returns fields directly


# ==========================
# Polymarket fetching
# ==========================
def fetch_polymarket_prices(market_id: str, session: requests.Session) -> Tuple[Optional[Tuple[float, float]], str]:
    """
    market_id: Polymarket market id (NOT conditionId)
    Returns ((yes,no), reason)
      reason in: ok|not_found|closed|bad_prices|http_error
    """
    try:
        url = f"{POLY_BASE_URL}/markets/{market_id}"
        r = session.get(url, timeout=15)
        if r.status_code == 404:
            return None, "not_found"
        r.raise_for_status()

        m = r.json()
        if not isinstance(m, dict):
            return None, "http_error"

        if m.get("closed") or m.get("archived"):
            return None, "closed"

        prices_raw = m.get("outcomePrices", "[]")
        prices = json.loads(prices_raw) if isinstance(prices_raw, str) else prices_raw
        if not prices or len(prices) < 2:
            return None, "bad_prices"

        yes = float(prices[0])
        no = float(prices[1])
        if not (0 < yes < 1 and 0 < no < 1):
            return None, "bad_prices"

        return (yes, no), "ok"

    except Exception:
        return None, "http_error"


# ==========================
# Pair model
# ==========================
@dataclass
class Pair:
    poly_id: str
    poly_name: str
    opin_id: str
    opin_name: str


# ==========================
# Monitor
# ==========================
class SpreadMonitor:
    def __init__(self, pairs: List[Pair], opinion_api_key: str, notifier: TelegramNotifier):
        self.pairs = pairs
        self.notifier = notifier
        self.poly_session = requests.Session()
        self.opinion = OpinionClientPlus(api_key=opinion_api_key, base_url="https://proxy.opinion.trade:8443/openapi")

        self.state: Dict[str, Dict[str, Any]] = load_json(STATE_FILE, default={})
        self.cache: Dict[str, Dict[str, str]] = load_json(CACHE_FILE, default={})

        self.cache_dirty = False
        self.cache_lock = threading.Lock()

    def _pair_key(self, p: Pair) -> str:
        return f"{p.poly_id}__{p.opin_id}"

    def _get_opinion_tokens(self, opin_market_id: str) -> Tuple[Optional[Tuple[str, str]], str]:
        with self.cache_lock:
            cached = self.cache.get(str(opin_market_id))
            if cached and cached.get("yesTokenId") and cached.get("noTokenId"):
                return (cached["yesTokenId"], cached["noTokenId"]), "ok_cached"

        try:
            detail = self.opinion.market_detail(str(opin_market_id))
        except Exception as e:
            return None, f"detail_error:{type(e).__name__}"

        yes_id = detail.get("yesTokenId") or detail.get("yesTokenID") or detail.get("yes_token_id")
        no_id = detail.get("noTokenId") or detail.get("noTokenID") or detail.get("no_token_id")
        if not yes_id or not no_id:
            return None, "missing_tokens"

        with self.cache_lock:
            self.cache[str(opin_market_id)] = {"yesTokenId": yes_id, "noTokenId": no_id}
            self.cache_dirty = True
        return (yes_id, no_id), "ok_fetched"

    def _fetch_opinion_prices(self, opin_market_id: str) -> Tuple[Optional[Tuple[float, float]], str]:
        tokens, tok_reason = self._get_opinion_tokens(opin_market_id)
        if not tokens:
            return None, tok_reason

        yes_id, no_id = tokens
        try:
            yes = self.opinion.token_buy_price(yes_id)
            no = self.opinion.token_buy_price(no_id)
            if yes is None or no is None:
                return None, "price_none"
            yes = float(yes)
            no = float(no)
            if not (0 < yes < 1 and 0 < no < 1):
                return None, "price_bad"
            return (yes, no), "ok"
        except Exception as e:
            return None, f"price_error:{type(e).__name__}"

    def _should_notify(self, pair_key: str, spread_pct: float) -> bool:
        if spread_pct < SPREAD_ALERT_THRESHOLD_PCT:
            return False

        last = self.state.get(pair_key, {}).get("last_notified_spread_pct")
        if last is None:
            return True
        try:
            last = float(last)
        except Exception:
            return True
        return spread_pct >= (last + SPREAD_RENOTIFY_STEP_PCT)

    def _record_notified(self, pair_key: str, spread_pct: float) -> None:
        self.state.setdefault(pair_key, {})
        self.state[pair_key]["last_notified_spread_pct"] = float(spread_pct)
        self.state[pair_key]["last_notified_at"] = time.strftime("%Y-%m-%d %H:%M:%S")
        atomic_write_json(STATE_FILE, self.state)

    def _format_message(
        self,
        p: Pair,
        spread_pct: float,
        direction: int,
        cost: float,
        poly_yes: float,
        poly_no: float,
        opin_yes: float,
        opin_no: float,
    ) -> str:
        leg = "BUY Poly YES + Opin NO" if direction == 1 else "BUY Poly NO + Opin YES"
        poly_search = f"https://polymarket.com/search?q={requests.utils.quote(p.poly_name[:120])}"
        opin_search = f"https://opinion.trade/search?q={requests.utils.quote(p.opin_name[:120])}"
        return (
            f"📈 Spread Alert: {spread_pct:.2f}%\n"
            f"{p.poly_name}\n"
            f"Leg: {leg}\n"
            f"Cost: {cost:.4f}\n\n"
            f"Poly YES/NO: {poly_yes:.3f}/{poly_no:.3f}\n"
            f"Opin YES/NO: {opin_yes:.3f}/{opin_no:.3f}\n\n"
            f"Poly id: {p.poly_id}\n"
            f"Opin id: {p.opin_id}\n"
            f"{poly_search}\n"
            f"{opin_search}"
        )

    def check_one(self, p: Pair) -> Tuple[str, Optional[str]]:
        """
        Returns (status_key, debug_sample or None)
        status_key used for per-cycle counters.
        """
        pair_key = self._pair_key(p)

        poly_prices, poly_reason = fetch_polymarket_prices(p.poly_id, self.poly_session)
        if not poly_prices:
            return f"poly_{poly_reason}", f"{p.poly_name} -> poly:{poly_reason}"

        poly_yes, poly_no = poly_prices

        opin_prices, opin_reason = self._fetch_opinion_prices(p.opin_id)
        if not opin_prices:
            return f"opin_{opin_reason}", f"{p.poly_name} -> opin:{opin_reason}"

        opin_yes, opin_no = opin_prices

        spread_pct, direction, cost = best_spread_pct(poly_yes, poly_no, opin_yes, opin_no)
        if direction == 0:
            return "no_spread", None

        # If spread meets threshold but we suppress due to last-notified, count it
        if spread_pct >= SPREAD_ALERT_THRESHOLD_PCT and not self._should_notify(pair_key, spread_pct):
            return "spread_suppressed", None

        if not self._should_notify(pair_key, spread_pct):
            return "spread_below_notify_rule", None

        msg = self._format_message(p, spread_pct, direction, cost, poly_yes, poly_no, opin_yes, opin_no)
        try:
            self.notifier.send(msg)
            self._record_notified(pair_key, spread_pct)
            return "alert_sent", None
        except Exception as e:
            return "telegram_error", f"telegram_error:{type(e).__name__}:{e}"

    def run_forever(self) -> None:
        log(f"Monitoring {len(self.pairs)} pairs every {MONITOR_INTERVAL_S:.0f}s")
        while True:
            t0 = time.time()
            counts = Counter()
            samples: List[str] = []

            with ThreadPoolExecutor(max_workers=WORKERS) as ex:
                futs = [ex.submit(self.check_one, p) for p in self.pairs]
                for fut in as_completed(futs):
                    status, sample = fut.result()
                    counts[status] += 1
                    if sample and len(samples) < DEBUG_FAIL_SAMPLES:
                        samples.append(sample)

            # Flush cache once per cycle (instead of from worker threads)
            if getattr(self, "cache_dirty", False):
                try:
                    with self.cache_lock:
                        cache_snapshot = dict(self.cache)
                        self.cache_dirty = False
                    atomic_write_json(CACHE_FILE, cache_snapshot)
                except Exception as e:
                    log(f"Failed to write cache file {CACHE_FILE}: {type(e).__name__}: {e}", "WARNING")

            dt = time.time() - t0

            # Always print a useful per-cycle summary
            summary = ", ".join([f"{k}={v}" for k, v in counts.most_common()])
            log(f"Cycle summary: {summary} | dt={dt:.2f}s")

            if samples and LOG_LEVEL == "DEBUG":
                for s in samples:
                    log(f"Skip sample: {s}", "DEBUG")

            sleep_s = max(0.0, MONITOR_INTERVAL_S - dt)
            log(f"Cycle done in {dt:.1f}s, sleeping {sleep_s:.1f}s")
            time.sleep(sleep_s)


def load_pairs(path: str) -> List[Pair]:
    raw = load_json(path, default=None)
    if raw is None:
        raise SystemExit(f"Could not read {path}")
    if not isinstance(raw, list):
        raise SystemExit(f"{path} must be a JSON list of pairs")

    pairs: List[Pair] = []
    for item in raw:
        try:
            pairs.append(Pair(
                poly_id=str(item["market1_id"]),
                poly_name=str(item["market1_name"]),
                opin_id=str(item["market2_id"]),
                opin_name=str(item["market2_name"]),
            ))
        except Exception:
            continue

    if not pairs:
        raise SystemExit("No valid pairs loaded")
    return pairs


def main() -> None:
    opinion_key = os.environ.get("OPINION_API_KEY", "").strip()
    tg_token = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()

    if not opinion_key:
        raise SystemExit('Missing OPINION_API_KEY. Example: export OPINION_API_KEY="..."')
    if not tg_token:
        raise SystemExit('Missing TELEGRAM_BOT_TOKEN. Example: export TELEGRAM_BOT_TOKEN="..."')
    if not TELEGRAM_CHAT_ID:
        raise SystemExit('Missing TELEGRAM_CHAT_ID. Example: export TELEGRAM_CHAT_ID="123456789"')

    pairs = load_pairs(PAIRS_FILE)
    notifier = TelegramNotifier(bot_token=tg_token, chat_id=TELEGRAM_CHAT_ID)

    mon = SpreadMonitor(pairs=pairs, opinion_api_key=opinion_key, notifier=notifier)
    mon.run_forever()


if __name__ == "__main__":
    main()