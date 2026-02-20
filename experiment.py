#!/usr/bin/env python3
"""
EXPERIMENT - Play with thresholds, check live prices, generate reports

Prerequisites: Run find_arbitrage.py first to create fetched_markets.json
"""

import json
import sys
import time
import re
import requests
from typing import Dict, List, Set, Tuple


# ============================================================================
# SMART MATCHING
# ============================================================================

def extract_years(text: str) -> Set[str]:
    return set(re.findall(r'\b(20\d{2})\b', text))


def extract_entities(text: str) -> Set[str]:
    entities = set()
    entities.update(re.findall(r'\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)\b', text))
    entities.update(re.findall(r'\b([A-Z]{2,})\b', text))
    return entities


def smart_match(q1: str, q2: str) -> float:
    """Smart matching with year/entity checking"""

    q1_lower = q1.lower()
    q2_lower = q2.lower()

    # Check for conflicting specific terms
    conflicts = [
        (['january', 'february', 'march', 'april', 'may', 'june', 'july', 'august', 'september', 'october', 'november',
          'december'], 'month'),
        (['world cup', 'worldcup'], 'world cup vs other'),
        (['group', 'qualifier'], 'group/qualifier vs tournament'),
    ]

    for terms, conflict_type in conflicts:
        has_term1 = any(term in q1_lower for term in terms)
        has_term2 = any(term in q2_lower for term in terms)

        # If one has the term and the other doesn't, check if they're actually different
        if has_term1 != has_term2:
            # Get the specific terms that appear
            terms1 = [t for t in terms if t in q1_lower]
            terms2 = [t for t in terms if t in q2_lower]

            # If they have different specific terms, reject
            if terms1 and terms2 and set(terms1) != set(terms2):
                return 0.0

    # Years must match exactly
    years1 = extract_years(q1)
    years2 = extract_years(q2)
    if years1 or years2:
        if years1 != years2:
            return 0.0

    # Thresholds must match ($1t+, etc)
    threshold1 = re.findall(r'\$\d+[tkmbTKMB]\+?', q1)
    threshold2 = re.findall(r'\$\d+[tkmbTKMB]\+?', q2)
    if threshold1 != threshold2:
        return 0.0

    # Must have common entities
    entities1 = extract_entities(q1)
    entities2 = extract_entities(q2)
    if entities1 and entities2 and len(entities1 & entities2) == 0:
        return 0.0

    # Word similarity
    stop_words = {'will', 'the', 'be', 'to', 'in', 'on', 'at', 'by', 'for', 'of', 'and', 'or', 'is', 'are', 'before',
                  'after', 'a', 'an', 'win', 'wins'}
    words1 = set(re.findall(r'\b[a-z]{3,}\b', q1.lower())) - stop_words
    words2 = set(re.findall(r'\b[a-z]{3,}\b', q2.lower())) - stop_words

    if not words1 or not words2:
        return 0.0

    intersection = len(words1 & words2)
    union = len(words1 | words2)
    return intersection / union if union > 0 else 0.0


# ============================================================================
# LIVE PRICE CHECKING
# ============================================================================

def fetch_polymarket_price(condition_id: str) -> dict:
    """Fetch current Polymarket price"""
    try:
        url = "https://gamma-api.polymarket.com/markets"
        params = {"condition_id": condition_id}
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()

        markets = response.json()
        if not markets:
            return {"error": "Not found (inactive/closed)"}

        data = markets[0]
        if data.get("closed") or data.get("archived"):
            return {"error": "Market closed"}

        prices_raw = data.get("outcomePrices", "[]")
        prices = json.loads(prices_raw) if isinstance(prices_raw, str) else prices_raw

        if prices and len(prices) >= 2:
            # Extract URL
            slug = data.get("slug", "")
            market_url = f"https://polymarket.com/event/{slug}" if slug else None

            return {
                "prices": [float(prices[0]), float(prices[1])],
                "url": market_url
            }

        return {"error": "Invalid prices"}
    except Exception as e:
        return {"error": str(e)}


def fetch_manifold_price(market_id: str) -> dict:
    """Fetch current Manifold price"""
    try:
        url = f"https://api.manifold.markets/v0/market/{market_id}"
        response = requests.get(url, timeout=10)
        response.raise_for_status()

        data = response.json()
        if data.get("isResolved"):
            return {"error": "Market resolved"}

        prob = data.get("probability", 0.5)

        # Extract URL
        slug = data.get("slug", "")
        creator_username = data.get("creatorUsername", "")
        market_url = f"https://manifold.markets/{creator_username}/{slug}" if creator_username and slug else None

        return {
            "prices": [prob, 1 - prob],
            "url": market_url
        }
    except Exception as e:
        return {"error": str(e)}


# ============================================================================
# EXPERIMENTER
# ============================================================================

class Experimenter:
    def __init__(self):
        self.min_volume = 1000
        self.min_liquidity = 500
        self.min_profit_pct = 2.0
        self.min_match_score = 0.6
        self.markets = None
        self.opportunities = []

    def load_markets(self):
        """Load cached markets"""
        try:
            with open('fetched_markets.json', 'r') as f:
                data = json.load(f)
            self.markets = data['markets']
            print(f"\n✓ Loaded {data['total_markets']:,} cached markets")
            return True
        except FileNotFoundError:
            print("\n❌ No fetched_markets.json found!")
            print("Run: python find_arbitrage.py first")
            return False

    def find_opportunities(self):
        """Find arbitrage with current thresholds"""

        print(f"\n{'=' * 70}")
        print("FINDING OPPORTUNITIES")
        print(f"{'=' * 70}")
        print(f"Thresholds: volume≥${self.min_volume}, liquidity≥${self.min_liquidity}")
        print(f"            profit≥{self.min_profit_pct}%, similarity≥{self.min_match_score:.0%}")
        print()

        # Filter
        filtered = {}
        for platform, market_list in self.markets.items():
            filtered[platform] = [
                m for m in market_list
                if m['volume'] >= self.min_volume
                   and m.get('liquidity', 0) >= self.min_liquidity
                   and len(m.get('prices', [])) == 2
                   and all(0 < p < 1 for p in m['prices'])
            ]
            print(f"{platform}: {len(market_list)} → {len(filtered[platform])} markets")

        # Match
        self.opportunities = []
        platforms = list(filtered.keys())

        if len(platforms) < 2:
            print("\n❌ Need 2+ platforms")
            return

        print(f"\nComparing {platforms[0]} vs {platforms[1]}...")

        for m1 in filtered[platforms[0]]:
            for m2 in filtered[platforms[1]]:
                score = smart_match(m1['question'], m2['question'])

                if score < self.min_match_score:
                    continue

                # Calculate arbitrage
                p1_yes, p1_no = m1['prices']
                p2_yes, p2_no = m2['prices']

                cost1 = p1_yes + p2_no
                profit1 = ((1 - cost1) / cost1 * 100) if 0 < cost1 < 1 else -100

                cost2 = p1_no + p2_yes
                profit2 = ((1 - cost2) / cost2 * 100) if 0 < cost2 < 1 else -100

                best_profit = max(profit1, profit2)

                if best_profit >= self.min_profit_pct:
                    self.opportunities.append({
                        'market1': m1,
                        'market2': m2,
                        'similarity': score,
                        'profit': best_profit,
                        'direction': 1 if profit1 > profit2 else 2,
                        'cost': cost1 if profit1 > profit2 else cost2,
                        'action1': 'BUY YES' if profit1 > profit2 else 'BUY NO',
                        'action2': 'BUY NO' if profit1 > profit2 else 'BUY YES',
                        'price1': p1_yes if profit1 > profit2 else p1_no,
                        'price2': p2_no if profit1 > profit2 else p2_yes
                    })

        self.opportunities.sort(key=lambda x: (x['similarity'], x['profit']), reverse=True)
        print(f"\n✓ Found {len(self.opportunities)} opportunities (sorted by similarity, then profit)")

    def show_opportunities(self, n=20, min_similarity=None):
        """Show top opportunities"""

        if not self.opportunities:
            print("\nNo opportunities found with current thresholds")
            return

        # Filter by similarity if requested
        to_show = self.opportunities
        if min_similarity:
            to_show = [opp for opp in self.opportunities if opp['similarity'] >= min_similarity]
            if not to_show:
                print(f"\nNo opportunities with similarity ≥ {min_similarity:.0%}")
                return

        print(f"\n{'=' * 70}")
        if min_similarity:
            print(f"OPPORTUNITIES WITH SIMILARITY ≥ {min_similarity:.0%} (SORTED BY SIMILARITY)")
        else:
            print(f"TOP {min(n, len(to_show))} OPPORTUNITIES (SORTED BY SIMILARITY, THEN PROFIT)")
        print(f"{'=' * 70}\n")

        for i, opp in enumerate(to_show[:n], 1):
            m1 = opp['market1']
            m2 = opp['market2']

            print(f"#{i} - {opp['profit']:.2f}% PROFIT | Similarity: {opp['similarity']:.0%}")
            print(f"{'─' * 70}")
            print(f"Q1: {m1['question']}")
            print(f"Q2: {m2['question']}\n")

            print(f"Polymarket: {opp['action1']} @ ${opp['price1']:.3f}")
            print(f"  Volume: ${m1['volume']:,.0f} | Liquidity: ${m1.get('liquidity', 0):,.0f}")
            print(f"  Search: https://polymarket.com/search?q={m1['question'].replace(' ', '+')[:80]}\n")

            print(f"Manifold: {opp['action2']} @ ${opp['price2']:.3f}")
            print(f"  Volume: ${m2['volume']:,.0f} | Liquidity: ${m2.get('liquidity', 0):,.0f}")
            print(f"  Search: https://manifold.markets/search?q={m2['question'].replace(' ', '+')[:80]}\n")

    def check_live_prices(self, n=10):
        """Check live prices for top N opportunities"""

        if not self.opportunities:
            print("\nNo opportunities to check")
            return

        print(f"\n{'=' * 70}")
        print(f"CHECKING LIVE PRICES (TOP {min(n, len(self.opportunities))})")
        print(f"{'=' * 70}\n")

        still_valid = []

        for i, opp in enumerate(self.opportunities[:n], 1):
            m1 = opp['market1']
            m2 = opp['market2']

            print(f"{i}. {m1['question'][:60]}")
            print(f"   Cached profit: {opp['profit']:.2f}%")
            print(f"   Fetching current prices...")

            result1 = fetch_polymarket_price(m1['id'])
            result2 = fetch_manifold_price(m2['id'])

            time.sleep(0.5)

            if 'error' in result1:
                print(f"   ❌ Polymarket: {result1['error']}")
                continue

            if 'error' in result2:
                print(f"   ❌ Manifold: {result2['error']}")
                continue

            # Recalculate
            p1_yes, p1_no = result1['prices']
            p2_yes, p2_no = result2['prices']

            # Show URLs
            url1 = result1.get('url')
            url2 = result2.get('url')

            if url1:
                print(f"   🔗 Polymarket: {url1}")
            if url2:
                print(f"   🔗 Manifold: {url2}")

            cost1 = p1_yes + p2_no
            profit1 = ((1 - cost1) / cost1 * 100) if 0 < cost1 < 1 else -100

            cost2 = p1_no + p2_yes
            profit2 = ((1 - cost2) / cost2 * 100) if 0 < cost2 < 1 else -100

            current_profit = max(profit1, profit2)

            print(f"   Current prices: Poly {p1_yes:.3f}/{p1_no:.3f}, Manifold {p2_yes:.3f}/{p2_no:.3f}")
            print(f"   Current profit: {current_profit:.2f}%")

            if current_profit >= self.min_profit_pct:
                print(f"   ✅ STILL VALID!")
                still_valid.append({**opp, 'current_profit': current_profit})
            else:
                print(f"   ❌ No longer profitable")
            print()

        print(f"{'=' * 70}")
        print(f"SUMMARY: {len(still_valid)}/{min(n, len(self.opportunities))} still valid")
        print(f"{'=' * 70}\n")

        if still_valid:
            print("Current valid opportunities:")
            for i, opp in enumerate(still_valid, 1):
                print(f"  {i}. {opp['current_profit']:.2f}% - {opp['market1']['question'][:60]}")

    def save_opportunities(self, filename="experiment_results.json"):
        """Save current opportunities to file"""

        if not self.opportunities:
            print("\nNo opportunities to save. Run 'Find opportunities' first.")
            return

        output = {
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "total": len(self.opportunities),
            "thresholds": {
                "min_volume": self.min_volume,
                "min_liquidity": self.min_liquidity,
                "min_profit_pct": self.min_profit_pct,
                "min_match_score": self.min_match_score
            },
            "opportunities": self.opportunities
        }

        with open(filename, 'w') as f:
            json.dump(output, f, indent=2)

        print(f"\n✓ Saved {len(self.opportunities)} opportunities to {filename}")

    def adjust_thresholds(self):
        """Interactive threshold adjustment"""

        print(f"\n{'=' * 70}")
        print("ADJUST THRESHOLDS")
        print(f"{'=' * 70}")
        print(f"\nCurrent settings:")
        print(f"  1. min_volume:       ${self.min_volume:,}")
        print(f"  2. min_liquidity:    ${self.min_liquidity:,}")
        print(f"  3. min_profit_pct:   {self.min_profit_pct}%")
        print(f"  4. min_match_score:  {self.min_match_score:.0%}")
        print(f"\nQuick presets:")
        print(f"  5. Aggressive (find more)")
        print(f"  6. Conservative (higher quality)")
        print(f"  0. Back to menu")

        choice = input("\nChoice: ").strip()

        if choice == '1':
            val = input(f"New min_volume (current ${self.min_volume}): ").strip()
            if val:
                self.min_volume = float(val)
                print(f"✓ Set to ${self.min_volume}")
        elif choice == '2':
            val = input(f"New min_liquidity (current ${self.min_liquidity}): ").strip()
            if val:
                self.min_liquidity = float(val)
                print(f"✓ Set to ${self.min_liquidity}")
        elif choice == '3':
            val = input(f"New min_profit_pct (current {self.min_profit_pct}%): ").strip()
            if val:
                self.min_profit_pct = float(val)
                print(f"✓ Set to {self.min_profit_pct}%")
        elif choice == '4':
            val = input(f"New min_match_score (current {self.min_match_score}): ").strip()
            if val:
                self.min_match_score = float(val)
                print(f"✓ Set to {self.min_match_score:.0%}")
        elif choice == '5':
            self.min_volume = 100
            self.min_liquidity = 50
            self.min_profit_pct = 1.0
            self.min_match_score = 0.4
            print("✓ Aggressive mode enabled")
        elif choice == '6':
            self.min_volume = 5000
            self.min_liquidity = 1000
            self.min_profit_pct = 3.0
            self.min_match_score = 0.8
            print("✓ Conservative mode enabled")


# ============================================================================
# MAIN MENU
# ============================================================================

def main():
    exp = Experimenter()

    print("\n" + "=" * 70)
    print("ARBITRAGE EXPERIMENTER")
    print("=" * 70)

    # Load markets
    if not exp.load_markets():
        return

    while True:
        print(f"\n{'=' * 70}")
        print("MENU")
        print(f"{'=' * 70}")
        print("1. Find opportunities (with current thresholds)")
        print("2. Show top 20 opportunities")
        print("3. Show only exact/near-exact matches (≥95% similarity)")
        print("4. Save opportunities to file")
        print("5. Check live prices (top 10)")
        print("6. Adjust thresholds")
        print("7. Show current settings")
        print("0. Exit")

        choice = input("\nChoice: ").strip()

        if choice == '1':
            exp.find_opportunities()
        elif choice == '2':
            exp.show_opportunities(20)
        elif choice == '3':
            exp.show_opportunities(50, min_similarity=0.95)  # Show more, but only exact matches
        elif choice == '4':
            exp.save_opportunities()
        elif choice == '5':
            exp.check_live_prices(10)
        elif choice == '6':
            exp.adjust_thresholds()
        elif choice == '7':
            print(f"\nCurrent thresholds:")
            print(f"  min_volume:      ${exp.min_volume:,}")
            print(f"  min_liquidity:   ${exp.min_liquidity:,}")
            print(f"  min_profit_pct:  {exp.min_profit_pct}%")
            print(f"  min_match_score: {exp.min_match_score:.0%}")
        elif choice == '0':
            print("\nGoodbye!")
            break
        else:
            print("Invalid choice")


if __name__ == "__main__":
    main()