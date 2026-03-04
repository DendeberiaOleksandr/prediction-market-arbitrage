#!/usr/bin/env python3
"""
pairs_strict.py - Deterministic strict market pairing (Polymarket <-> Opinion)

- Loads markets from fetched_markets.json (produced by find_arbitrage.py)
- Pairs markets ONLY when the normalized event name (question/title) matches exactly
- Writes pairs_strict.json

No GPT. No price fetching. No arbitrage calculation.
"""

from __future__ import annotations

import json
import os
import re
import time
from typing import Any, Dict, List, Optional, Tuple


POLY_PLATFORM = "polymarket"
OPINION_PLATFORM = "opinion"

INPUT_FILE = "fetched_markets.json"
OUTPUT_FILE = "pairs_strict.json"

# How strict you want normalization:
# - "lower_only": just .lower().strip()
# - "normalized": lower + whitespace collapse + mild punctuation/unicode normalization
STRICT_MODE = os.environ.get("STRICT_MODE", "normalized").strip().lower()


_WS_RE = re.compile(r"\s+")

def normalize_text(s: str) -> str:
    s = (s or "").strip()

    if STRICT_MODE == "lower_only":
        return s.lower()

    # normalized mode
    s = s.lower()

    # normalize a few common unicode variants
    s = s.replace("’", "'").replace("“", '"').replace("”", '"')
    s = s.replace("–", "-").replace("—", "-")

    # collapse whitespace
    s = _WS_RE.sub(" ", s).strip()

    # OPTIONAL: uncomment if you want to ignore a trailing question mark/period
    # s = s.rstrip("?.!,")

    return s


def extract_poly_name(m: Dict[str, Any]) -> str:
    # Polymarket uses "question" most of the time
    return (m.get("question") or m.get("name") or "").strip()


def extract_opinion_name(m: Dict[str, Any]) -> str:
    # Opinion might use question/marketTitle depending on your fetcher
    return (m.get("question") or m.get("marketTitle") or m.get("title") or "").strip()


def load_markets(path: str) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    with open(path, "r") as f:
        data = json.load(f)
    markets = data.get("markets") or {}
    poly = markets.get(POLY_PLATFORM) or []
    opi = markets.get(OPINION_PLATFORM) or []
    if not poly or not opi:
        raise SystemExit("Need both polymarket and opinion markets in fetched_markets.json")
    return poly, opi


def main() -> None:
    if not os.path.exists(INPUT_FILE):
        raise SystemExit(f"Missing {INPUT_FILE}. Run find_arbitrage.py first.")

    poly_raw, opi_raw = load_markets(INPUT_FILE)

    # Build Opinion index: normalized_name -> list[(id, display_name)]
    # We keep a list to detect collisions (multiple Opinion markets with same normalized text).
    opinion_index: Dict[str, List[Tuple[str, str]]] = {}
    for m in opi_raw:
        oid = str(m.get("id") or m.get("marketId") or "").strip()
        name = extract_opinion_name(m)
        if not oid or not name:
            continue

        key = normalize_text(name)
        opinion_index.setdefault(key, []).append((oid, name))

    pairs: List[Dict[str, Any]] = []
    used_poly = set()
    used_opi = set()

    collisions = 0
    no_match = 0

    for m in poly_raw:
        pid = str(m.get("id") or m.get("conditionId") or "").strip()
        name = extract_poly_name(m)
        if not pid or not name:
            continue

        key = normalize_text(name)
        candidates = opinion_index.get(key)

        if not candidates:
            no_match += 1
            continue

        # If multiple Opinion markets share the same normalized text, that's ambiguous.
        # With "strict" philosophy, we skip ambiguous matches.
        if len(candidates) > 1:
            collisions += 1
            continue

        oid, oname = candidates[0]

        # one-to-one
        if pid in used_poly or oid in used_opi:
            continue

        used_poly.add(pid)
        used_opi.add(oid)

        pairs.append({
            "market1_platform": POLY_PLATFORM,
            "market1_id": pid,
            "market1_name": name,
            "market2_platform": OPINION_PLATFORM,
            "market2_id": oid,
            "market2_name": oname,
            "match_method": f"strict:{STRICT_MODE}",
            "normalized_key": key,
        })

    with open(OUTPUT_FILE, "w") as f:
        json.dump(pairs, f, indent=2)

    print(f"✓ Wrote {len(pairs)} strict pairs to {OUTPUT_FILE}")
    print(f"  Ambiguous collisions skipped: {collisions}")
    print(f"  Polymarket with no match:     {no_match}")
    print(f"  Strict mode:                 {STRICT_MODE}")


if __name__ == "__main__":
    main()
