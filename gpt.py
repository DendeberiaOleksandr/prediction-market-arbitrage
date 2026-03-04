#!/usr/bin/env python3
"""
gpt.py - Strict FULL-MATCH ONLY market pairing (Polymarket -> Opinion)

What it does
------------
1) Loads markets from fetched_markets.json (produced by find_arbitrage.py)
2) Uses GPT to find ONLY *exact/full* matches between Polymarket and Opinion markets
3) Writes matches incrementally to pairs.json (atomic writes) so you can see progress

What it DOES NOT do
-------------------
- No price fetching
- No arbitrage/profit calculation
- No liquidity/volume filtering (beyond requiring ids + question text)

Input
-----
fetched_markets.json with:
  markets.polymarket: [{id/question/...}, ...]
  markets.opinion:    [{id/question/yesTokenId/noTokenId/...}, ...]

Output
------
pairs.json:
{
  "generated_at": "...",
  "model": "...",
  "batch_size_polymarket": 30,
  "prefilter_topk": 18,
  "total_pairs": 123,
  "pairs": [
    {
      "market1_platform": "polymarket",
      "market1_id": "...",
      "market1_name": "...",
      "market2_platform": "opinion",
      "market2_id": "...",
      "market2_name": "..."
    }
  ]
}

Required env
------------
OPENAI_API_KEY

Recommended env knobs
---------------------
OPENAI_MODEL=gpt-4o-mini
GPT_BATCH_SIZE=30
GPT_WORKERS=4
OPENAI_TIMEOUT_S=240
OPENAI_MAX_RETRIES=5
OPINION_PREFILTER_TOPK=18
POLY_MAX_TEXT_CHARS=320
OPINION_MAX_TEXT_CHARS=320
FLUSH_EVERY_N_PAIRS=20
FLUSH_EVERY_SECONDS=10
RESUME_FROM_OUTPUT=1

Notes
-----
- GPT must output ONLY full matches and omit everything else.
- We do a cheap local prefilter to reduce tokens and improve quality:
  for each Polymarket market, send only top-K most similar Opinion candidates by word overlap.
- We enforce one-to-one matching again in code (no repeated ids).
"""

from __future__ import annotations

import json
import os
import random
import re
import sys
import time
from dataclasses import dataclass
from threading import Lock
from typing import Any, Dict, List, Optional, Set, Tuple

import requests
from concurrent.futures import ThreadPoolExecutor, as_completed


# =============================================================================
# Config
# =============================================================================
OPENAI_MODEL = os.environ.get("OPENAI_MODEL", "gpt-4o-mini")
BATCH_SIZE_POLY = int(os.environ.get("GPT_BATCH_SIZE", "30"))
GPT_WORKERS = int(os.environ.get("GPT_WORKERS", "4"))
OPENAI_TIMEOUT_S = int(os.environ.get("OPENAI_TIMEOUT_S", "240"))
OPENAI_MAX_RETRIES = int(os.environ.get("OPENAI_MAX_RETRIES", "5"))

OPINION_PREFILTER_TOPK = int(os.environ.get("OPINION_PREFILTER_TOPK", "18"))
OPINION_MAX_TEXT_CHARS = int(os.environ.get("OPINION_MAX_TEXT_CHARS", "320"))
POLY_MAX_TEXT_CHARS = int(os.environ.get("POLY_MAX_TEXT_CHARS", "320"))

FLUSH_EVERY_N_PAIRS = int(os.environ.get("FLUSH_EVERY_N_PAIRS", "20"))
FLUSH_EVERY_SECONDS = float(os.environ.get("FLUSH_EVERY_SECONDS", "10"))
RESUME_FROM_OUTPUT = os.environ.get("RESUME_FROM_OUTPUT", "1").strip().lower() not in ("0", "false", "no")

POLY_PLATFORM = "polymarket"
OPINION_PLATFORM = "opinion"


# =============================================================================
# Logging
# =============================================================================
def log(msg: str, level: str = "INFO") -> None:
    ts = time.strftime("%H:%M:%S")
    print(f"[{ts}] {level}: {msg}", flush=True)


# =============================================================================
# Text / Prefilter helpers
# =============================================================================
_WORD_RE = re.compile(r"\b[a-z0-9]{2,}\b", re.IGNORECASE)
STOP_WORDS = {
    "will", "the", "be", "to", "in", "on", "at", "by", "for", "of", "and", "or",
    "is", "are", "a", "an", "before", "after", "with", "from", "into", "over",
    "this", "that", "it", "as", "than", "if"
}

def clamp_text(s: str, max_chars: int) -> str:
    s = (s or "").strip()
    if len(s) <= max_chars:
        return s
    return s[:max_chars].rstrip() + "…"

def tokenize(s: str) -> Set[str]:
    s = (s or "").lower()
    return {w for w in _WORD_RE.findall(s) if w not in STOP_WORDS}

def jaccard(a: Set[str], b: Set[str]) -> float:
    if not a or not b:
        return 0.0
    inter = len(a & b)
    union = len(a | b)
    return inter / union if union else 0.0


# =============================================================================
# Data model
# =============================================================================
@dataclass(frozen=True)
class MarketText:
    id: str
    platform: str
    question: str
    text: str
    tokens: frozenset
    raw: Dict[str, Any]


# =============================================================================
# OpenAI strict matching prompt + parsing
# =============================================================================
SYSTEM_PROMPT = """You are a strict market equivalence classifier for prediction markets.

Goal:
Given two lists of markets from different platforms (market1=Polymarket, market2=Opinion), return ONLY the pairs that are EXACT equivalents.

Definition of FULL MATCH:
Two markets are a FULL MATCH only if they refer to the exact same underlying proposition, including:
- Same numeric thresholds (e.g., 9.0 is NOT 10.0; 9.0 equals 9 and 9.00)
- Same time bounds and direction (before/by/after), same month/year/date cutoff
- Same subject/entities (teams, person, company, region)
- Same condition wording (e.g., “released” vs “announced” is NOT the same unless clearly identical criteria)
If uncertain, it is NOT a full match.

Output requirements:
Return ONLY valid JSON with this exact schema:

{
  "pairs": [
    {
      "market1_name": "...",
      "market1_id": "...",
      "market2_name": "...",
      "market2_id": "..."
    }
  ]
}

Rules:
- Output ONLY full matches. Do NOT output non-matches or partial matches.
- Each market1 item can appear at most once.
- Each market2 item can appear at most once.
- If multiple candidates exist, choose the single best exact match; otherwise omit the market1 item entirely.
- Do not include extra keys. Do not include explanations. JSON only.

Before responding, re-check every proposed pair and remove it if ANY numeric token, date/month/year, or key verb differs.
"""

def build_gpt_user_payload(poly_items: List[Dict[str, str]], opinion_items: List[Dict[str, str]]) -> str:
    payload = {
        "market1_platform": POLY_PLATFORM,
        "market2_platform": OPINION_PLATFORM,
        "market1": [{"id": x["id"], "name": x.get("text", "")} for x in poly_items],
        "market2": [{"id": x["id"], "name": x.get("text", "")} for x in opinion_items],
    }
    return "Return ONLY JSON.\n\n" + json.dumps(payload, ensure_ascii=False)

def parse_full_match_pairs(gpt_json: Dict[str, Any]) -> List[Tuple[str, str]]:
    if not isinstance(gpt_json, dict):
        raise ValueError("GPT output is not a JSON object")
    pairs = gpt_json.get("pairs")
    if not isinstance(pairs, list):
        raise ValueError("Missing or invalid 'pairs' list in GPT output")

    out: List[Tuple[str, str]] = []
    for row in pairs:
        if not isinstance(row, dict):
            continue
        pid = str(row.get("market1_id") or "").strip()
        oid = str(row.get("market2_id") or "").strip()
        if pid and oid:
            out.append((pid, oid))
    return out


# =============================================================================
# OpenAI Responses API client (with retries)
# =============================================================================
class OpenAIClient:
    def __init__(self, api_key: str):
        if not api_key:
            raise ValueError("OPENAI_API_KEY is required")
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        })

    @staticmethod
    def _extract_output_text(data: Dict[str, Any]) -> str:
        for item in data.get("output", []):
            for c in item.get("content", []):
                if c.get("type") == "output_text":
                    t = c.get("text")
                    if t:
                        return t
        raise RuntimeError("OpenAI: missing output_text in response JSON")

    def _call(self, poly_items: List[Dict[str, str]], opinion_items: List[Dict[str, str]]) -> Dict[str, Any]:
        url = "https://api.openai.com/v1/responses"
        user = build_gpt_user_payload(poly_items, opinion_items)
        body = {
            "model": OPENAI_MODEL,
            "input": [
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": user},
            ],
            "temperature": 0.0,
        }
        r = self.session.post(url, json=body, timeout=OPENAI_TIMEOUT_S)
        r.raise_for_status()
        data = r.json()
        text = self._extract_output_text(data)

        try:
            return json.loads(text)
        except Exception:
            m = re.search(r"\{.*\}", text, flags=re.DOTALL)
            if not m:
                raise RuntimeError(f"OpenAI output not JSON: {text[:800]}")
            return json.loads(m.group(0))

    def match_batch_with_retries(self, poly_items: List[Dict[str, str]], opinion_items: List[Dict[str, str]]) -> Dict[str, Any]:
        last_err: Optional[Exception] = None
        for attempt in range(1, OPENAI_MAX_RETRIES + 1):
            try:
                return self._call(poly_items, opinion_items)
            except Exception as e:
                last_err = e
                backoff = min(2 ** attempt, 30) + random.uniform(0.0, 1.0)
                log(f"GPT error (attempt {attempt}/{OPENAI_MAX_RETRIES}): {type(e).__name__}: {e}. Backoff {backoff:.1f}s", "WARNING")
                time.sleep(backoff)
        raise last_err if last_err else RuntimeError("GPT retries exhausted")


# =============================================================================
# IO (atomic save + resume)
# =============================================================================
def save_output(path: str, output: Dict[str, Any]) -> None:
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        json.dump(output, f, indent=2)
    os.replace(tmp, path)

def load_existing_output(path: str) -> Tuple[Dict[str, Any], Set[str], Set[str]]:
    """
    Returns (data, used_poly_ids, used_opinion_ids) from existing output.
    """
    if not os.path.exists(path):
        return {}, set(), set()

    try:
        with open(path, "r") as f:
            data = json.load(f)

        used_poly: Set[str] = set()
        used_opi: Set[str] = set()

        for p in data:
            pid = str(p.get("market1_id") or "").strip()
            oid = str(p.get("market2_id") or "").strip()
            if pid:
                used_poly.add(pid)
            if oid:
                used_opi.add(oid)

        return data, used_poly, used_opi
    except Exception:
        return {}, set(), set()

def maybe_flush_output(
    *,
    output_path: str,
    results: List[Dict[str, Any]],
    meta: Dict[str, Any],
    last_flush_time: float,
    new_pairs_since_flush: int,
) -> Tuple[float, int]:
    now = time.time()
    should_flush = (
        (FLUSH_EVERY_N_PAIRS > 0 and new_pairs_since_flush >= FLUSH_EVERY_N_PAIRS)
        or (FLUSH_EVERY_SECONDS > 0 and (now - last_flush_time) >= FLUSH_EVERY_SECONDS)
    )
    if not should_flush:
        return last_flush_time, new_pairs_since_flush

    save_output(output_path, results)
    log(f"Flushed {len(results)} pairs to {output_path}")
    return now, 0


# =============================================================================
# Prefilter Opinion candidates per Polymarket market
# =============================================================================
def prefilter_opinion_candidates(poly_item: MarketText, opinion_all: List[MarketText], topk: int) -> List[MarketText]:
    scored: List[Tuple[float, MarketText]] = []
    pt = set(poly_item.tokens)
    for o in opinion_all:
        s = jaccard(pt, set(o.tokens))
        if s > 0:
            scored.append((s, o))
    scored.sort(key=lambda x: x[0], reverse=True)
    return [o for _, o in scored[:topk]]


# =============================================================================
# Main
# =============================================================================
def main() -> None:
    openai_key = os.environ.get("OPENAI_API_KEY", "").strip()
    if not openai_key:
        raise SystemExit('Missing OPENAI_API_KEY. Example: export OPENAI_API_KEY="..."')

    if not os.path.exists("fetched_markets.json"):
        raise SystemExit("Missing fetched_markets.json. Run find_arbitrage.py first.")

    with open("fetched_markets.json", "r") as f:
        data = json.load(f)

    markets = data.get("markets") or {}
    poly_raw = markets.get(POLY_PLATFORM) or []
    opin_raw = markets.get(OPINION_PLATFORM) or []

    if not poly_raw or not opin_raw:
        raise SystemExit("Need both polymarket and opinion markets in fetched_markets.json")

    log(f"Loaded: polymarket={len(poly_raw):,} opinion={len(opin_raw):,}")

    # Prepare MarketText objects
    poly: List[MarketText] = []
    for m in poly_raw:
        mid = str(m.get("id") or m.get("conditionId") or "").strip()
        q = (m.get("question") or "").strip()
        d = (m.get("description") or "").strip()
        if not mid or not q:
            continue
        full = q if not d else f"{q}\n\n{d}"
        full = clamp_text(full, POLY_MAX_TEXT_CHARS)
        toks = frozenset(tokenize(q + " " + d))
        poly.append(MarketText(id=mid, platform=POLY_PLATFORM, question=q, text=full, tokens=toks, raw=m))

    opinion: List[MarketText] = []
    for m in opin_raw:
        mid = str(m.get("id") or m.get("marketId") or "").strip()
        q = (m.get("question") or m.get("marketTitle") or "").strip()
        d = (m.get("description") or m.get("rules") or "").strip()
        if not mid or not q:
            continue
        full = q if not d else f"{q}\n\n{d}"
        full = clamp_text(full, OPINION_MAX_TEXT_CHARS)
        toks = frozenset(tokenize(q + " " + d))
        opinion.append(MarketText(id=mid, platform=OPINION_PLATFORM, question=q, text=full, tokens=toks, raw=m))

    opinion_by_id: Dict[str, MarketText] = {m.id: m for m in opinion}

    log(f"Prepared text: polymarket={len(poly):,} opinion={len(opinion):,}")

    # Resume support
    output_path = "pairs.json"
    results: List[Dict[str, Any]] = []
    used_poly_ids: Set[str] = set()
    used_opi_ids: Set[str] = set()

    if RESUME_FROM_OUTPUT:
        existing, used_poly_ids, used_opi_ids = load_existing_output(output_path)
        if existing:
            results = list(existing)
        if used_poly_ids or used_opi_ids:
            log(f"Resuming from {output_path}: used_poly={len(used_poly_ids):,} used_opinion={len(used_opi_ids):,}")

    poly_to_process = [m for m in poly if m.id not in used_poly_ids]
    log(f"Remaining polymarket to process: {len(poly_to_process):,}")

    if not poly_to_process:
        log("Nothing to do. Exiting.")
        return

    # Clients
    oai = OpenAIClient(openai_key)

    meta = {
        "model": OPENAI_MODEL,
        "batch_size_polymarket": BATCH_SIZE_POLY,
        "gpt_workers": GPT_WORKERS,
        "prefilter_topk": OPINION_PREFILTER_TOPK,
    }

    # Build batches
    total_batches = (len(poly_to_process) + BATCH_SIZE_POLY - 1) // BATCH_SIZE_POLY
    batches: List[Tuple[int, int, int, List[MarketText]]] = []
    for b in range(total_batches):
        s = b * BATCH_SIZE_POLY
        e = min(len(poly_to_process), s + BATCH_SIZE_POLY)
        batches.append((b, s, e, poly_to_process[s:e]))

    log(f"Submitting {len(batches)} GPT batches with workers={GPT_WORKERS}, batch_size={BATCH_SIZE_POLY}, prefilter_topk={OPINION_PREFILTER_TOPK}")

    results_lock = Lock()
    flush_lock = Lock()
    last_flush_time = time.time()
    new_pairs_since_flush = 0

    def process_batch(b: int, s: int, e: int, batch_poly: List[MarketText]) -> List[Tuple[str, str]]:
        # Union of topK Opinion candidates across the batch
        candidate_set: Dict[str, MarketText] = {}
        for p in batch_poly:
            for o in prefilter_opinion_candidates(p, opinion, OPINION_PREFILTER_TOPK):
                candidate_set[o.id] = o

        opinion_candidates = list(candidate_set.values())
        # Safety fallback: never send too few
        if len(opinion_candidates) < min(10, len(opinion)):
            opinion_candidates = opinion[: min(30, len(opinion))]

        poly_payload = [{"id": p.id, "platform": p.platform, "text": p.text} for p in batch_poly]
        opin_payload = [{"id": o.id, "platform": o.platform, "text": o.text} for o in opinion_candidates]

        out = oai.match_batch_with_retries(poly_payload, opin_payload)
        pairs = parse_full_match_pairs(out)

        # One-to-one enforcement within this batch output (local)
        local_used_poly: Set[str] = set()
        local_used_opi: Set[str] = set()
        deduped: List[Tuple[str, str]] = []
        for pid, oid in pairs:
            if pid in local_used_poly or oid in local_used_opi:
                continue
            local_used_poly.add(pid)
            local_used_opi.add(oid)
            deduped.append((pid, oid))

        log(f"GPT batch {b+1}/{total_batches} done: poly[{s}:{e}] pairs={len(deduped)}")
        return deduped

    with ThreadPoolExecutor(max_workers=GPT_WORKERS) as ex:
        futures = {ex.submit(process_batch, b, s, e, batch_poly): (b, s, e) for (b, s, e, batch_poly) in batches}

        for fut in as_completed(futures):
            b, s, e = futures[fut]
            try:
                pairs = fut.result()
            except Exception as e:
                log(f"GPT batch {b+1}/{total_batches} FAILED: {type(e).__name__}: {e}", "ERROR")
                continue

            # Apply global one-to-one and append to results
            with results_lock:
                for pid, oid in pairs:
                    if pid in used_poly_ids or oid in used_opi_ids:
                        continue
                    p_obj = next((x for x in poly if x.id == pid), None)
                    o_obj = opinion_by_id.get(oid)
                    if not p_obj or not o_obj:
                        continue

                    used_poly_ids.add(pid)
                    used_opi_ids.add(oid)

                    results.append({
                        "market1_platform": POLY_PLATFORM,
                        "market1_id": pid,
                        "market1_name": p_obj.question,
                        "market2_platform": OPINION_PLATFORM,
                        "market2_id": oid,
                        "market2_name": o_obj.question,
                    })

                new_pairs_since_flush += len(pairs)

            with flush_lock:
                last_flush_time, new_pairs_since_flush = maybe_flush_output(
                    output_path=output_path,
                    results=results,
                    meta=meta,
                    last_flush_time=last_flush_time,
                    new_pairs_since_flush=new_pairs_since_flush,
                )

    save_output(output_path, results)
    log(f"Final flush saved {len(results)} pairs to {output_path}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log("Interrupted by user", "WARNING")
        sys.exit(130)