# opinion_api.py
import os
import time
import requests
from typing import Dict, List, Optional, Tuple

# Try the docs proxy host if openapi.opinion.trade doesn't work for you:
DEFAULT_BASE_URL = "https://proxy.opinion.trade:8443/openapi"
#DEFAULT_BASE_URL = "https://openapi.opinion.trade/openapi"


def _debug_enabled() -> bool:
    return os.environ.get("OPINION_DEBUG", "").strip() not in ("", "0", "false", "False")


def olog(msg: str):
    if _debug_enabled():
        print(f"[OPINION] {msg}")


class OpinionClient:
    """
    Minimal Opinion OpenAPI client (read-only).
    Auth header: apikey: <key>
    Response format: {code, msg, result}
    """

    def __init__(self, api_key: str, base_url: str = DEFAULT_BASE_URL):
        if not api_key:
            raise ValueError("Opinion API key is required")
        self.base_url = base_url.rstrip("/")
        self.s = requests.Session()
        self.s.headers.update({
            "apikey": api_key,
            "Accept": "application/json",
        })
        olog(f"Initialized client base_url={self.base_url}")

    def _get(self, path: str, params: Optional[dict] = None, timeout: int = 15) -> dict:
        url = f"{self.base_url}{path}"
        params = params or {}

        olog(f"GET {url} params={params}")

        r = self.s.get(url, params=params, timeout=timeout)

        if r.status_code != 200:
            body_snip = (r.text or "")[:400].replace("\n", " ")
            raise RuntimeError(f"HTTP {r.status_code} for {url} params={params} body='{body_snip}'")

        try:
            data = r.json()
        except Exception as e:
            body_snip = (r.text or "")[:400].replace("\n", " ")
            raise RuntimeError(f"Non-JSON response from {url}: {type(e).__name__}: {e}. body='{body_snip}'") from e

        if not isinstance(data, dict):
            raise RuntimeError(f"Unexpected JSON type from {url}: {type(data).__name__}")

        # ✅ Support BOTH response formats:
        # Format A (docs): { "code": 0, "msg": "...", "result": {...} }
        if "code" in data:
            if data.get("code") != 0:
                raise RuntimeError(
                    f"Opinion API error code={data.get('code')} msg={data.get('msg')} url={url} params={params}")
            return data

        # Format B (actual): { "errno": 0, "errmsg": "...", "result": {...} }
        if "errno" in data:
            if data.get("errno") != 0:
                raise RuntimeError(
                    f"Opinion API error errno={data.get('errno')} errmsg={data.get('errmsg')} url={url} params={params}")
            return data

        # Unknown wrapper
        body_snip = (r.text or "")[:500].replace("\n", " ")
        raise RuntimeError(
            f"Unexpected response shape from {url}. keys={list(data.keys())}. body='{body_snip}'"
        )

    # ---------- Markets ----------
    def list_markets(
        self,
        page: int = 1,
        limit: int = 20,              # max 20 per docs
        status: str = "activated",    # activated|resolved
        market_type: int = 0,         # 0=binary, 1=categorical, 2=all
        sort_by: int = 5,             # docs example uses sortBy=5
    ) -> Tuple[int, List[dict]]:
        params = {
            "page": page,
            "limit": min(int(limit), 20),
            "status": status,
            "marketType": market_type,
            "sortBy": sort_by,
        }
        data = self._get("/market", params=params)
        result = data.get("result") or {}
        total = int(result.get("total") or 0)
        items = result.get("list") or []
        olog(f"list_markets page={page} -> items={len(items)} total={total}")
        return total, items

    # ---------- Tokens / Prices ----------
    def token_latest_price(self, token_id: str) -> Optional[float]:
        data = self._get("/token/latest-price", params={"token_id": token_id}, timeout=10)
        result = data.get("result") or {}
        p = result.get("price")
        return float(p) if p is not None else None

    def token_orderbook(self, token_id: str) -> dict:
        data = self._get("/token/orderbook", params={"token_id": token_id}, timeout=10)
        return data.get("result") or {}

    def token_buy_price(self, token_id: str) -> Optional[float]:
        """
        For 'BUY', we want best ask if available. If no asks, fallback to latest trade price.
        """
        ob = self.token_orderbook(token_id)
        asks = ob.get("asks") or []
        if asks:
            try:
                return float(asks[0]["price"])
            except Exception:
                pass
        return self.token_latest_price(token_id)

    def market_yes_no_buy_prices(
        self,
        yes_token_id: str,
        no_token_id: str,
        polite_sleep_s: float = 0.05
    ) -> Optional[List[float]]:
        yes = self.token_buy_price(yes_token_id)
        if polite_sleep_s:
            time.sleep(polite_sleep_s)
        no = self.token_buy_price(no_token_id)
        if yes is None or no is None:
            return None
        return [float(yes), float(no)]


class OpinionFetcher:
    """
    Fetch binary markets from Opinion OpenAPI.
    NOTE: limit per page is 20 per docs; so large limits will take many pages.
    We do NOT fetch token prices during bulk fetch (too many calls).
    """

    def __init__(self, api_key: str, base_url: str = DEFAULT_BASE_URL):
        self.client = OpinionClient(api_key=api_key, base_url=base_url)

    def fetch_markets(self, limit: int = 1000) -> Optional[List[Dict]]:
        try:
            all_markets: List[Dict] = []
            page = 1
            per_page = 20

            olog(f"fetch_markets(limit={limit}) starting...")

            while len(all_markets) < limit:
                total, items = self.client.list_markets(
                    page=page,
                    limit=per_page,
                    status="activated",
                    market_type=0,
                    sort_by=5,
                )

                if not items:
                    olog("No items returned; stopping.")
                    break

                added = 0
                skipped_no_tokens = 0

                for m in items:
                    if len(all_markets) >= limit:
                        break

                    yes_token = m.get("yesTokenId")
                    no_token = m.get("noTokenId")
                    if not yes_token or not no_token:
                        skipped_no_tokens += 1
                        continue

                    all_markets.append({
                        "platform": "opinion",
                        "id": str(m.get("marketId", "")),
                        "question": m.get("marketTitle", "") or "",
                        "description": m.get("rules", "") or "",
                        "end_date": m.get("cutoffAt", "") or "",
                        "volume": float(m.get("volume", 0) or 0),
                        "liquidity": float(m.get("liquidity", 0) or 0),
                        "outcomes": ["YES", "NO"],
                        "prices": [],
                        "yesTokenId": yes_token,
                        "noTokenId": no_token,
                        "raw_data": m,
                    })
                    added += 1

                olog(
                    f"page={page} items={len(items)} added={added} "
                    f"skipped_no_tokens={skipped_no_tokens} collected={len(all_markets)}/{limit} total={total}"
                )

                page += 1
                time.sleep(0.15)

            olog(f"fetch_markets done. returning {len(all_markets)} markets")
            return all_markets

        except Exception as e:
            # DO NOT swallow: print error and return None
            print(f"[OPINION] ERROR in fetch_markets: {type(e).__name__}: {e}")
            return None