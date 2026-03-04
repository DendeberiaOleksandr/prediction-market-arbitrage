#!/usr/bin/env python3
"""
ARBITRAGE FINDER - Polymarket vs Opinion
Fetches lots of markets, matches by question similarity, validates with live Opinion token prices.
"""

import json
import os
import sys
import time
from datetime import datetime
from typing import Dict, List, Optional

import requests
from opinion_api import OpinionFetcher, OpinionClient


def log(message: str, level: str = "INFO"):
    timestamp = datetime.now().strftime("%H:%M:%S")
    print(f"[{timestamp}] {level}: {message}")


class PolymarketFetcher:
    def __init__(self):
        self.base_url = "https://gamma-api.polymarket.com"

    def fetch_markets(self, limit: int = 100) -> Optional[List[Dict]]:
        try:
            all_markets = []
            offset = 0
            batch_size = 500

            log(f"Fetching up to {limit} markets from Polymarket...")

            while len(all_markets) < limit:
                remaining = limit - len(all_markets)
                current_batch_size = min(batch_size, remaining)

                url = f"{self.base_url}/markets"
                params = {
                    "limit": current_batch_size,
                    "offset": offset,
                    "closed": "false",
                    "archived": "false"
                }

                log(f"  Batch {offset//batch_size + 1}: offset {offset}...")
                response = requests.get(url, params=params, timeout=15)
                response.raise_for_status()

                markets = response.json()
                if not markets:
                    log(f"  End of available markets at offset {offset}")
                    break

                log(f"  Got {len(markets)} markets")

                for market in markets:
                    prices_raw = market.get("outcomePrices", [])
                    outcomes_raw = market.get("outcomes", [])

                    try:
                        prices_list = json.loads(prices_raw) if isinstance(prices_raw, str) else prices_raw
                        price_floats = [float(p) for p in prices_list[:2]] if prices_list else []
                    except Exception:
                        price_floats = []

                    try:
                        outcomes_list = json.loads(outcomes_raw) if isinstance(outcomes_raw, str) else outcomes_raw
                    except Exception:
                        outcomes_list = []

                    all_markets.append({
                        "platform": "polymarket",
                        "id": market.get("id", ""),
                        "conditionId": market.get("conditionId", ""),
                        "question": market.get("question", ""),
                        "description": market.get("description", ""),
                        "end_date": market.get("endDateIso", ""),
                        "volume": float(market.get("volume", 0) or 0),
                        "liquidity": float(market.get("liquidity", 0) or 0),
                        "outcomes": outcomes_list,
                        "prices": price_floats,
                        "raw_data": market
                    })

                offset += len(markets)

                if len(markets) < current_batch_size:
                    log(f"  Reached end (got {len(markets)} < {current_batch_size})")
                    break

                time.sleep(0.3)

            log(f"✓ Total: {len(all_markets)} markets from Polymarket")
            return all_markets

        except Exception as e:
            log(f"✗ Error: {e}", "ERROR")
            return None


class SimpleArbitrageFinder:
    def __init__(self, opinion_api_key: str):
        self.min_volume = 1000
        self.min_liquidity = 500
        self.min_profit_pct = 2.0
        self.min_match_score = 0.4
        self.min_combined_volume = 10000

        # Opinion client for live price fetch during validation
        self.opinion_client = OpinionClient(opinion_api_key)

    def load_fetched_markets(self, filename: str = "fetched_markets.json") -> Optional[Dict[str, List[Dict]]]:
        try:
            with open(filename, 'r') as f:
                data = json.load(f)

            log(f"✓ Loaded {data['total_markets']:,} markets from {filename}")
            log(f"  Fetched at: {data['timestamp']}")
            return data['markets']
        except FileNotFoundError:
            log(f"✗ File {filename} not found", "WARNING")
            return None
        except Exception as e:
            log(f"✗ Error loading markets: {e}", "ERROR")
            return None

    def fetch_all_markets(self, poly_limit=10000, opinion_limit=2000, opinion_api_key="") -> Dict[str, List[Dict]]:
        log("\n" + "=" * 70)
        log("STEP 1: FETCHING MARKETS (WITH PAGINATION)")
        log("=" * 70 + "\n")

        markets: Dict[str, List[Dict]] = {}

        poly_fetcher = PolymarketFetcher()
        poly_markets = poly_fetcher.fetch_markets(limit=poly_limit)
        if poly_markets:
            markets["polymarket"] = poly_markets

        opinion_fetcher = OpinionFetcher(api_key=opinion_api_key)
        opinion_markets = opinion_fetcher.fetch_markets(limit=opinion_limit)
        if opinion_markets:
            markets["opinion"] = opinion_markets

        total = sum(len(m) for m in markets.values())
        log(f"\n{'=' * 70}")
        log(f"TOTAL MARKETS FETCHED: {total:,}")
        log(f"{'=' * 70}")

        self.save_fetched_markets(markets)
        return markets

    def save_fetched_markets(self, markets: Dict[str, List[Dict]], filename: str = "fetched_markets.json"):
        output = {
            "timestamp": datetime.now().isoformat(),
            "platforms": list(markets.keys()),
            "total_markets": sum(len(m) for m in markets.values()),
            "markets": markets
        }
        with open(filename, 'w') as f:
            json.dump(output, f, indent=2)

        log(f"\n✓ Saved all fetched markets to {filename}")
        log("  Run with --cached to reuse without re-fetching")

    def filter_markets(self, markets: Dict[str, List[Dict]]) -> Dict[str, List[Dict]]:
        log("\n" + "=" * 70)
        log("STEP 2: FILTERING MARKETS")
        log("=" * 70 + "\n")

        filtered: Dict[str, List[Dict]] = {}

        for platform, market_list in markets.items():
            platform_filtered = []

            for market in market_list:
                if market.get("volume", 0) < self.min_volume:
                    continue

                if platform == "opinion":
                    # Opinion markets don't have cached prices in bulk fetch; keep them if token IDs exist
                    if not market.get("yesTokenId") or not market.get("noTokenId"):
                        continue
                    platform_filtered.append(market)
                    continue

                # Polymarket: require prices
                prices = market.get("prices", [])
                if len(prices) != 2:
                    continue
                try:
                    p1 = float(prices[0])
                    p2 = float(prices[1])
                    if not (0 < p1 < 1 and 0 < p2 < 1):
                        continue
                except Exception:
                    continue

                platform_filtered.append(market)

            filtered[platform] = platform_filtered
            log(f"{platform}: {len(market_list):,} → {len(platform_filtered):,} markets")

        return filtered

    def quick_match_score(self, q1: str, q2: str) -> float:
        import re
        stop_words = {'will', 'the', 'be', 'to', 'in', 'on', 'at', 'by', 'for', 'of', 'and', 'or', 'is', 'are'}
        words1 = set(re.findall(r'\b[a-z]{3,}\b', q1.lower())) - stop_words
        words2 = set(re.findall(r'\b[a-z]{3,}\b', q2.lower())) - stop_words
        if not words1 or not words2:
            return 0.0
        inter = len(words1 & words2)
        union = len(words1 | words2)
        return inter / union if union else 0.0

    def _ensure_prices(self, market: Dict) -> bool:
        """
        Ensure market['prices'] contains [yes_buy, no_buy].
        - Polymarket already has it.
        - Opinion fetches live via token IDs.
        """
        if market["platform"] == "polymarket":
            return len(market.get("prices", [])) == 2

        if market["platform"] == "opinion":
            if len(market.get("prices", [])) == 2:
                return True
            yes_id = market.get("yesTokenId")
            no_id = market.get("noTokenId")
            if not yes_id or not no_id:
                return False
            prices = self.opinion_client.market_yes_no_buy_prices(yes_id, no_id, polite_sleep_s=0.05)
            if not prices:
                return False
            market["prices"] = prices
            return True

        return False

    def find_candidate_pairs(self, markets: Dict[str, List[Dict]]) -> List[Dict]:
        log("\n" + "=" * 70)
        log("STEP 3: FINDING CANDIDATE PAIRS")
        log("=" * 70 + "\n")

        candidates: List[Dict] = []
        platforms = list(markets.keys())
        if len(platforms) < 2:
            log("Need 2+ platforms", "ERROR")
            return []

        # Only two platforms expected: polymarket + opinion
        for i in range(len(platforms)):
            for j in range(i + 1, len(platforms)):
                p1 = platforms[i]
                p2 = platforms[j]
                mlist1 = markets[p1]
                mlist2 = markets[p2]

                log(f"Comparing {p1} ({len(mlist1):,}) vs {p2} ({len(mlist2):,})...")

                for m1 in mlist1:
                    for m2 in mlist2:
                        score = self.quick_match_score(m1["question"], m2["question"])
                        if score < self.min_match_score:
                            continue

                        # Only compute profit here if both have prices already.
                        # Opinion usually does not at this stage, so we defer profit calc to validation.
                        has1 = len(m1.get("prices", [])) == 2
                        has2 = len(m2.get("prices", [])) == 2
                        if has1 and has2:
                            p1_yes, p1_no = m1["prices"]
                            p2_yes, p2_no = m2["prices"]
                            cost1 = p1_yes + p2_no
                            profit1_pct = ((1 - cost1) / cost1 * 100) if cost1 > 0 else -100
                            cost2 = p1_no + p2_yes
                            profit2_pct = ((1 - cost2) / cost2 * 100) if cost2 > 0 else -100
                            best_profit = max(profit1_pct, profit2_pct)
                        else:
                            best_profit = 0.0  # placeholder; real check happens in validate_arbitrage

                        candidates.append({
                            "market1": m1,
                            "market2": m2,
                            "similarity": score,
                            "best_profit": best_profit
                        })

        # Sort by similarity then (placeholder) best_profit
        candidates.sort(key=lambda x: (x["similarity"], x["best_profit"]), reverse=True)
        log(f"\nFound {len(candidates)} candidates with similarity ≥ {self.min_match_score}")
        return candidates

    def validate_arbitrage(self, candidate: Dict) -> Optional[Dict]:
        m1 = candidate["market1"]
        m2 = candidate["market2"]

        combined_volume = float(m1.get("volume", 0)) + float(m2.get("volume", 0))
        if combined_volume < self.min_combined_volume:
            return None

        # Liquidity filters: Opinion list may not have liquidity; treat missing as 0 and allow you to tune min_liquidity if needed
        if float(m1.get("liquidity", 0) or 0) < self.min_liquidity:
            # If it's Opinion and liquidity missing, you can relax min_liquidity or change logic.
            if m1["platform"] != "opinion":
                return None
        if float(m2.get("liquidity", 0) or 0) < self.min_liquidity:
            if m2["platform"] != "opinion":
                return None

        # Fetch live prices if needed (Opinion)
        if not self._ensure_prices(m1):
            return None
        if not self._ensure_prices(m2):
            return None

        p1_yes, p1_no = m1["prices"]
        p2_yes, p2_no = m2["prices"]

        # sanity
        if not (0 < p1_yes < 1 and 0 < p1_no < 1 and 0 < p2_yes < 1 and 0 < p2_no < 1):
            return None

        opportunities = []

        cost1 = p1_yes + p2_no
        if 0 < cost1 < 1.0:
            profit1_pct = (1 - cost1) / cost1 * 100
            if profit1_pct >= self.min_profit_pct:
                opportunities.append(("BUY YES (m1) + BUY NO (m2)", 1, cost1, profit1_pct))

        cost2 = p1_no + p2_yes
        if 0 < cost2 < 1.0:
            profit2_pct = (1 - cost2) / cost2 * 100
            if profit2_pct >= self.min_profit_pct:
                opportunities.append(("BUY NO (m1) + BUY YES (m2)", 2, cost2, profit2_pct))

        if not opportunities:
            return None

        label, direction, total_cost, profit_pct = max(opportunities, key=lambda x: x[3])

        # Construct output in your existing format
        if direction == 1:
            m1_action, m1_price = "BUY YES", p1_yes
            m2_action, m2_price = "BUY NO", p2_no
        else:
            m1_action, m1_price = "BUY NO", p1_no
            m2_action, m2_price = "BUY YES", p2_yes

        return {
            "direction": direction,
            "market1": {
                "platform": m1["platform"],
                "question": m1["question"],
                "action": m1_action,
                "price": m1_price,
                "volume": float(m1.get("volume", 0) or 0),
                "liquidity": float(m1.get("liquidity", 0) or 0),
                "id": m1.get("id", ""),
            },
            "market2": {
                "platform": m2["platform"],
                "question": m2["question"],
                "action": m2_action,
                "price": m2_price,
                "volume": float(m2.get("volume", 0) or 0),
                "liquidity": float(m2.get("liquidity", 0) or 0),
                "id": m2.get("id", ""),
            },
            "total_cost": float(total_cost),
            "expected_profit": float(1 - total_cost),
            "profit_percentage": float(profit_pct),
            "similarity_score": float(candidate["similarity"]),
            "combined_volume": float(combined_volume),
            "label": label,
        }

    def print_all_candidates(self, opportunities: List[Dict]):
        print("\n" + "=" * 70)
        print(f"📋 FOUND {len(opportunities)} ARBITRAGE CANDIDATES")
        print("=" * 70)
        print("\n⚠️  MANUAL REVIEW REQUIRED - Verify markets are the same event!\n")

        for i, arb in enumerate(opportunities, 1):
            m1 = arb["market1"]
            m2 = arb["market2"]

            print(f"\n{'─' * 70}")
            print(f"CANDIDATE #{i} - Profit: {arb['profit_percentage']:.2f}%")
            print(f"{'─' * 70}")
            print(f"Similarity: {arb['similarity_score']:.2f}")
            print(f"Combined Volume: ${arb['combined_volume']:,.0f}")
            print()
            print(f"Market 1 ({m1['platform'].upper()}):")
            print(f"  Question: {m1['question']}")
            print(f"  Action: {m1['action']} @ ${m1['price']:.4f}")
            print(f"  Volume: ${m1['volume']:,.0f} | Liquidity: ${m1['liquidity']:,.0f}")
            print()
            print(f"Market 2 ({m2['platform'].upper()}):")
            print(f"  Question: {m2['question']}")
            print(f"  Action: {m2['action']} @ ${m2['price']:.4f}")
            print(f"  Volume: ${m2['volume']:,.0f} | Liquidity: ${m2['liquidity']:,.0f}")
            print()
            print("💰 If markets match:")
            print(f"  Cost: ${arb['total_cost']:.4f}")
            print(f"  Profit: {arb['profit_percentage']:.2f}%")

            if i >= 20:
                print(f"\n... and {len(opportunities) - 20} more (see all_candidates.json)")
                break

    def save_all_candidates(self, opportunities: List[Dict], filename: str = "all_candidates.json"):
        output = {
            "timestamp": datetime.now().isoformat(),
            "total_candidates": len(opportunities),
            "candidates": opportunities
        }
        with open(filename, "w") as f:
            json.dump(output, f, indent=2)
        log(f"\n✓ Saved {len(opportunities)} candidates to {filename}")

    def run(self, poly_limit=10000, opinion_limit=2000, use_cached=False, opinion_api_key=""):
        print("\n" + "=" * 70)
        if use_cached:
            print("ARBITRAGE FINDER - USING CACHED MARKETS")
        else:
            print(f"ARBITRAGE FINDER - FETCHING UP TO {poly_limit + opinion_limit:,} MARKETS")
        print("=" * 70)

        if use_cached:
            markets = self.load_fetched_markets()
            if not markets:
                log("Failed to load cached markets, fetching new...", "WARNING")
                markets = self.fetch_all_markets(poly_limit, opinion_limit, opinion_api_key=opinion_api_key)
        else:
            markets = self.fetch_all_markets(poly_limit, opinion_limit, opinion_api_key=opinion_api_key)

        if not markets:
            log("No markets available", "ERROR")
            return None

        filtered = self.filter_markets(markets)
        candidates = self.find_candidate_pairs(filtered)
        if not candidates:
            log("No candidates found", "WARNING")
            return None

        log("\n" + "=" * 70)
        log("STEP 4: VALIDATING CANDIDATES (LIVE OPINION PRICES)")
        log("=" * 70 + "\n")

        valid_opportunities = []
        for candidate in candidates:
            arb = self.validate_arbitrage(candidate)
            if arb:
                valid_opportunities.append(arb)

        if not valid_opportunities:
            log("No valid arbitrage found", "WARNING")
            return None

        valid_opportunities.sort(key=lambda x: x["profit_percentage"], reverse=True)
        self.print_all_candidates(valid_opportunities)
        self.save_all_candidates(valid_opportunities)
        return valid_opportunities[0]


def main():
    """
    API KEY PLACEMENT (recommended):
      export OPINION_API_KEY="your_key_here"
      python find_arbitrage.py
    or on Windows PowerShell:
      $env:OPINION_API_KEY="your_key_here"
      python find_arbitrage.py
    """

    use_cached = "--cached" in sys.argv or "-c" in sys.argv

    opinion_api_key = os.environ.get("OPINION_API_KEY", "").strip()
    if not opinion_api_key:
        print("\n❌ Missing Opinion API key.")
        print("Set it as an environment variable, e.g.:")
        print('  export OPINION_API_KEY="your_key_here"')
        print("Or PowerShell:")
        print('  $env:OPINION_API_KEY="your_key_here"')
        sys.exit(1)

    finder = SimpleArbitrageFinder(opinion_api_key=opinion_api_key)

    # Opinion OpenAPI pages are max 20/page; 2000 means 100 pages.
    arbitrage = finder.run(
        poly_limit=10000,
        opinion_limit=2000,
        use_cached=use_cached,
        opinion_api_key=opinion_api_key,
    )

    if arbitrage:
        print("\n" + "=" * 70)
        print("✅ FOUND CANDIDATES - CHECK all_candidates.json")
        print("=" * 70)
        print("\n⚠️  CRITICAL: Manually verify markets match before trading!")
        print("\n💡 TIP: Markets saved to fetched_markets.json")
        print("   Run with --cached to reuse without re-fetching")
    else:
        print("\n❌ No arbitrage found")
        print("\n💡 TIP: Try adjusting thresholds in the script:")
        print("   - Lower min_match_score (currently 0.4)")
        print("   - Lower min_profit_pct (currently 2.0)")
        print("   - Lower min_volume (currently 1000)")


if __name__ == "__main__":
    main()