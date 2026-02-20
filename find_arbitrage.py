#!/usr/bin/env python3
"""
ARBITRAGE FINDER - Find arbitrage with massive market fetching
Now with pagination to fetch 10k-100k markets!
"""

import json
import sys
import time
from datetime import datetime
from typing import Dict, List, Optional

import requests


def log(message: str, level: str = "INFO"):
    """Simple logging"""
    timestamp = datetime.now().strftime("%H:%M:%S")
    print(f"[{timestamp}] {level}: {message}")


class PolymarketFetcher:
    """Fetch markets from Polymarket with pagination"""

    def __init__(self):
        self.base_url = "https://gamma-api.polymarket.com"

    def fetch_markets(self, limit: int = 100) -> Optional[List[Dict]]:
        """Fetch markets with pagination"""
        try:
            all_markets = []
            offset = 0
            batch_size = 500  # API limit per request

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

                if not markets or len(markets) == 0:
                    log(f"  End of available markets at offset {offset}")
                    break

                log(f"  Got {len(markets)} markets")

                # Parse this batch
                for market in markets:
                    prices_raw = market.get("outcomePrices", [])
                    outcomes_raw = market.get("outcomes", [])

                    try:
                        if isinstance(prices_raw, str):
                            prices_list = json.loads(prices_raw)
                        else:
                            prices_list = prices_raw
                        price_floats = [float(p) for p in prices_list[:2]] if prices_list else []
                    except:
                        price_floats = []

                    try:
                        if isinstance(outcomes_raw, str):
                            outcomes_list = json.loads(outcomes_raw)
                        else:
                            outcomes_list = outcomes_raw
                    except:
                        outcomes_list = []

                    all_markets.append({
                        "platform": "polymarket",
                        "id": market.get("conditionId", ""),
                        "question": market.get("question", ""),
                        "description": market.get("description", ""),
                        "end_date": market.get("endDateIso", ""),
                        "volume": float(market.get("volume", 0)),
                        "liquidity": float(market.get("liquidity", 0)),
                        "outcomes": outcomes_list,
                        "prices": price_floats,
                        "raw_data": market
                    })

                offset += len(markets)

                if len(markets) < current_batch_size:
                    log(f"  Reached end (got {len(markets)} < {current_batch_size})")
                    break

                time.sleep(0.3)  # Be nice to API

            log(f"✓ Total: {len(all_markets)} markets from Polymarket")
            return all_markets

        except Exception as e:
            log(f"✗ Error: {e}", "ERROR")
            return None


class ManifoldFetcher:
    """Fetch markets from Manifold with pagination"""

    def __init__(self):
        self.base_url = "https://api.manifold.markets/v0"

    def fetch_markets(self, limit: int = 100) -> Optional[List[Dict]]:
        """Fetch markets with pagination using 'before' cursor"""
        try:
            all_markets = []
            before_id = None
            batch_size = 1000  # Manifold max

            log(f"Fetching up to {limit} markets from Manifold...")

            batch_num = 1
            while len(all_markets) < limit:
                remaining = limit - len(all_markets)
                current_batch_size = min(batch_size, remaining)

                url = f"{self.base_url}/markets"
                params = {"limit": current_batch_size}
                if before_id:
                    params["before"] = before_id

                log(f"  Batch {batch_num}: requesting {current_batch_size}...")
                response = requests.get(url, params=params, timeout=15)
                response.raise_for_status()

                markets = response.json()

                if not markets or len(markets) == 0:
                    log(f"  No more markets available")
                    break

                log(f"  Got {len(markets)} markets")

                # Parse this batch
                for market in markets:
                    if market.get("outcomeType") != "BINARY":
                        continue

                    all_markets.append({
                        "platform": "manifold",
                        "id": market.get("id", ""),
                        "question": market.get("question", ""),
                        "description": market.get("description", ""),
                        "end_date": market.get("closeTime", ""),
                        "volume": float(market.get("volume", 0)),
                        "liquidity": float(market.get("totalLiquidity", 0)),
                        "outcomes": ["YES", "NO"],
                        "prices": [market.get("probability", 0.5), 1 - market.get("probability", 0.5)],
                        "raw_data": market
                    })

                # Use last market ID as cursor for next batch
                if markets:
                    before_id = markets[-1].get("id")

                if len(markets) < current_batch_size:
                    log(f"  Reached end (got {len(markets)} < {current_batch_size})")
                    break

                batch_num += 1
                time.sleep(0.3)

            log(f"✓ Total: {len(all_markets)} markets from Manifold")
            return all_markets

        except Exception as e:
            log(f"✗ Error: {e}", "ERROR")
            return None


class SimpleArbitrageFinder:
    """Find arbitrage with large-scale market fetching"""

    def __init__(self):
        self.min_volume = 1000
        self.min_liquidity = 500
        self.min_profit_pct = 2.0
        self.min_match_score = 0.4
        self.min_combined_volume = 10000

    def load_fetched_markets(self, filename: str = "fetched_markets.json") -> Optional[Dict[str, List[Dict]]]:
        """Load previously fetched markets from file"""

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

    def fetch_all_markets(self, poly_limit=10000, manifold_limit=10000) -> Dict[str, List[Dict]]:
        """Fetch markets from all platforms"""

        log("\n" + "="*70)
        log("STEP 1: FETCHING MARKETS (WITH PAGINATION)")
        log("="*70 + "\n")

        markets = {}

        # Fetch from Polymarket
        poly_fetcher = PolymarketFetcher()
        poly_markets = poly_fetcher.fetch_markets(limit=poly_limit)
        if poly_markets:
            markets['polymarket'] = poly_markets

        # Fetch from Manifold
        manifold_fetcher = ManifoldFetcher()
        manifold_markets = manifold_fetcher.fetch_markets(limit=manifold_limit)
        if manifold_markets:
            markets['manifold'] = manifold_markets

        total = sum(len(m) for m in markets.values())
        log(f"\n{'='*70}")
        log(f"TOTAL MARKETS FETCHED: {total:,}")
        log(f"{'='*70}")

        # Save all fetched markets to file
        self.save_fetched_markets(markets)

        return markets

    def save_fetched_markets(self, markets: Dict[str, List[Dict]], filename: str = "fetched_markets.json"):
        """Save all fetched markets to JSON for later reuse"""

        output = {
            "timestamp": datetime.now().isoformat(),
            "platforms": list(markets.keys()),
            "total_markets": sum(len(m) for m in markets.values()),
            "markets": markets
        }

        with open(filename, 'w') as f:
            json.dump(output, f, indent=2)

        log(f"\n✓ Saved all fetched markets to {filename}")
        log(f"  You can now experiment with thresholds without re-fetching!")

    def filter_markets(self, markets: Dict[str, List[Dict]]) -> Dict[str, List[Dict]]:
        """Filter by quality"""

        log("\n" + "="*70)
        log("STEP 2: FILTERING MARKETS")
        log("="*70 + "\n")

        filtered = {}

        for platform, market_list in markets.items():
            platform_filtered = []

            for market in market_list:
                if market['volume'] < self.min_volume:
                    continue

                prices = market.get('prices', [])
                if len(prices) != 2:
                    continue

                try:
                    p1 = float(prices[0])
                    p2 = float(prices[1])
                    if p1 <= 0 or p1 >= 1 or p2 <= 0 or p2 >= 1:
                        continue
                except:
                    continue

                platform_filtered.append(market)

            filtered[platform] = platform_filtered
            log(f"{platform}: {len(market_list):,} → {len(platform_filtered):,} markets")

        return filtered

    def quick_match_score(self, q1: str, q2: str) -> float:
        """Calculate word overlap similarity"""
        import re

        stop_words = {'will', 'the', 'be', 'to', 'in', 'on', 'at', 'by', 'for', 'of', 'and', 'or', 'is', 'are'}
        words1 = set(re.findall(r'\b[a-z]{3,}\b', q1.lower())) - stop_words
        words2 = set(re.findall(r'\b[a-z]{3,}\b', q2.lower())) - stop_words

        if not words1 or not words2:
            return 0.0

        intersection = len(words1 & words2)
        union = len(words1 | words2)

        return intersection / union if union > 0 else 0.0

    def find_candidate_pairs(self, markets: Dict[str, List[Dict]]) -> List[Dict]:
        """Find matching pairs"""

        log("\n" + "="*70)
        log("STEP 3: FINDING CANDIDATE PAIRS")
        log("="*70 + "\n")

        candidates = []
        platforms = list(markets.keys())

        if len(platforms) < 2:
            log("Need 2+ platforms", "ERROR")
            return []

        for i in range(len(platforms)):
            for j in range(i + 1, len(platforms)):
                platform1 = platforms[i]
                platform2 = platforms[j]

                markets1 = markets[platform1]
                markets2 = markets[platform2]

                log(f"Comparing {platform1} ({len(markets1):,}) vs {platform2} ({len(markets2):,})...")

                all_comparisons = []

                for m1 in markets1:
                    for m2 in markets2:
                        score = self.quick_match_score(m1['question'], m2['question'])

                        all_comparisons.append((score, m1['question'][:50], m2['question'][:50]))

                        if score < self.min_match_score:
                            continue

                        p1_yes = m1['prices'][0]
                        p1_no = m1['prices'][1]
                        p2_yes = m2['prices'][0]
                        p2_no = m2['prices'][1]

                        cost1 = p1_yes + p2_no
                        profit1_pct = ((1 - cost1) / cost1 * 100) if cost1 > 0 else -100

                        cost2 = p1_no + p2_yes
                        profit2_pct = ((1 - cost2) / cost2 * 100) if cost2 > 0 else -100

                        if profit1_pct >= self.min_profit_pct or profit2_pct >= self.min_profit_pct:
                            candidates.append({
                                'market1': m1,
                                'market2': m2,
                                'similarity': score,
                                'profit1_pct': profit1_pct,
                                'profit2_pct': profit2_pct,
                                'best_profit': max(profit1_pct, profit2_pct)
                            })

                all_comparisons.sort(reverse=True)
                log(f"  Top 5 similarity scores:", "DEBUG")
                for score, q1, q2 in all_comparisons[:5]:
                    log(f"    {score:.3f}: {q1}... vs {q2}...", "DEBUG")

        candidates.sort(key=lambda x: x['best_profit'], reverse=True)
        log(f"\nFound {len(candidates)} candidates with profit ≥ {self.min_profit_pct}%")

        return candidates

    def validate_arbitrage(self, candidate: Dict) -> Optional[Dict]:
        """Validate candidate"""

        m1 = candidate['market1']
        m2 = candidate['market2']

        combined_volume = m1['volume'] + m2['volume']
        if combined_volume < self.min_combined_volume:
            return None

        if m1.get('liquidity', 0) < self.min_liquidity or m2.get('liquidity', 0) < self.min_liquidity:
            return None

        p1_yes = m1['prices'][0]
        p1_no = m1['prices'][1]
        p2_yes = m2['prices'][0]
        p2_no = m2['prices'][1]

        opportunities = []

        cost1 = p1_yes + p2_no
        if cost1 < 1.0:
            profit1 = 1 - cost1
            profit1_pct = (profit1 / cost1 * 100)

            if profit1_pct >= self.min_profit_pct:
                opportunities.append({
                    'direction': 1,
                    'market1': {
                        'platform': m1['platform'],
                        'question': m1['question'],
                        'action': 'BUY YES',
                        'price': p1_yes,
                        'volume': m1['volume'],
                        'liquidity': m1['liquidity'],
                        'id': m1['id']
                    },
                    'market2': {
                        'platform': m2['platform'],
                        'question': m2['question'],
                        'action': 'BUY NO',
                        'price': p2_no,
                        'volume': m2['volume'],
                        'liquidity': m2['liquidity'],
                        'id': m2['id']
                    },
                    'total_cost': cost1,
                    'expected_profit': profit1,
                    'profit_percentage': profit1_pct,
                    'similarity_score': candidate['similarity'],
                    'combined_volume': combined_volume
                })

        cost2 = p1_no + p2_yes
        if cost2 < 1.0:
            profit2 = 1 - cost2
            profit2_pct = (profit2 / cost2 * 100)

            if profit2_pct >= self.min_profit_pct:
                opportunities.append({
                    'direction': 2,
                    'market1': {
                        'platform': m1['platform'],
                        'question': m1['question'],
                        'action': 'BUY NO',
                        'price': p1_no,
                        'volume': m1['volume'],
                        'liquidity': m1['liquidity'],
                        'id': m1['id']
                    },
                    'market2': {
                        'platform': m2['platform'],
                        'question': m2['question'],
                        'action': 'BUY YES',
                        'price': p2_yes,
                        'volume': m2['volume'],
                        'liquidity': m2['liquidity'],
                        'id': m2['id']
                    },
                    'total_cost': cost2,
                    'expected_profit': profit2,
                    'profit_percentage': profit2_pct,
                    'similarity_score': candidate['similarity'],
                    'combined_volume': combined_volume
                })

        if opportunities:
            return max(opportunities, key=lambda x: x['profit_percentage'])

        return None

    def print_all_candidates(self, opportunities: List[Dict]):
        """Print all candidates"""

        print("\n" + "="*70)
        print(f"📋 FOUND {len(opportunities)} ARBITRAGE CANDIDATES")
        print("="*70)
        print("\n⚠️  MANUAL REVIEW REQUIRED - Verify markets are the same event!\n")

        for i, arb in enumerate(opportunities, 1):
            m1 = arb['market1']
            m2 = arb['market2']

            print(f"\n{'─'*70}")
            print(f"CANDIDATE #{i} - Profit: {arb['profit_percentage']:.2f}%")
            print(f"{'─'*70}")
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
            print(f"💰 If markets match:")
            print(f"  Cost: ${arb['total_cost']:.4f}")
            print(f"  Profit: {arb['profit_percentage']:.2f}%")

            if i >= 20:  # Show top 20
                print(f"\n... and {len(opportunities) - 20} more (see all_candidates.json)")
                break

    def save_all_candidates(self, opportunities: List[Dict], filename: str = "all_candidates.json"):
        """Save all to file"""

        output = {
            "timestamp": datetime.now().isoformat(),
            "total_candidates": len(opportunities),
            "candidates": opportunities
        }

        with open(filename, 'w') as f:
            json.dump(output, f, indent=2)

        log(f"\n✓ Saved {len(opportunities)} candidates to {filename}")

    def run(self, poly_limit=10000, manifold_limit=10000, use_cached=False):
        """
        Main execution

        Args:
            poly_limit: Number of markets to fetch from Polymarket
            manifold_limit: Number of markets to fetch from Manifold
            use_cached: If True, load from fetched_markets.json instead of re-fetching
        """

        print("\n" + "="*70)
        if use_cached:
            print("ARBITRAGE FINDER - USING CACHED MARKETS")
        else:
            print(f"ARBITRAGE FINDER - FETCHING UP TO {poly_limit + manifold_limit:,} MARKETS")
        print("="*70)

        # Load or fetch markets
        if use_cached:
            markets = self.load_fetched_markets()
            if not markets:
                log("Failed to load cached markets, fetching new...", "WARNING")
                markets = self.fetch_all_markets(poly_limit, manifold_limit)
        else:
            markets = self.fetch_all_markets(poly_limit, manifold_limit)

        if not markets:
            log("No markets available", "ERROR")
            return None

        filtered = self.filter_markets(markets)
        candidates = self.find_candidate_pairs(filtered)

        if not candidates:
            log("No candidates found", "WARNING")
            return None

        log("\n" + "="*70)
        log("STEP 4: VALIDATING CANDIDATES")
        log("="*70 + "\n")

        valid_opportunities = []

        for candidate in candidates:
            arb = self.validate_arbitrage(candidate)
            if arb:
                valid_opportunities.append(arb)

        if not valid_opportunities:
            log("No valid arbitrage found", "WARNING")
            return None

        valid_opportunities.sort(key=lambda x: x['profit_percentage'], reverse=True)

        self.print_all_candidates(valid_opportunities)
        self.save_all_candidates(valid_opportunities)

        return valid_opportunities[0]


def main():
    """
    Main entry point

    Usage:
      python find_arbitrage.py              # Fetch fresh markets
      python find_arbitrage.py --cached     # Use cached markets from fetched_markets.json
    """

    use_cached = '--cached' in sys.argv or '-c' in sys.argv

    finder = SimpleArbitrageFinder()

    # Fetch 10k from each platform (or load cached)
    arbitrage = finder.run(poly_limit=10000, manifold_limit=10000, use_cached=use_cached)

    if arbitrage:
        print("\n" + "="*70)
        print("✅ FOUND CANDIDATES - CHECK all_candidates.json")
        print("="*70)
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