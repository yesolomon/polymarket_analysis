#!/usr/bin/env python3
"""
Polymarket closed Yes/No markets:
- Daily YES/NO probabilities (from CLOB /prices-history at daily fidelity)
- Final outcome proxy (from Gamma outcomePrices near 0/1)
- Total volume (from Gamma volumeNum/volume)
- T_days = (endDate - startDate) / 86400 (blank if missing)
- Titles & descriptions saved for later classification

Outputs in --out:
- markets.jsonl              (raw Gamma market objects for the selected markets)
- daily.csv                  (one row per market per day)
- market_texts.csv           (one row per market: title + description)

Requirements:
  pip install requests

Notes:
- No The Graph / Polygraph dependency in this version.
"""

import argparse
import datetime as dt
import json
import os
import sys
import time
from typing import Any, Dict, List, Optional, Tuple

import requests

DEFAULT_GAMMA_BASE = "https://gamma-api.polymarket.com"
DEFAULT_CLOB_BASE = "https://clob.polymarket.com"

USER_AGENT = "polymarket-downloader/1.0"


class RateLimiter:
    def __init__(self, rps: float):
        self.min_interval = 1.0 / max(rps, 0.1)
        self._last = 0.0

    def wait(self) -> None:
        now = time.time()
        sleep_for = self.min_interval - (now - self._last)
        if sleep_for > 0:
            time.sleep(sleep_for)
        self._last = time.time()


def iso_to_ts(s: Optional[Any]) -> Optional[int]:
    if s is None:
        return None
    if isinstance(s, (int, float)):
        if s > 10_000_000_000:  # ms
            return int(s / 1000)
        return int(s)
    if isinstance(s, str):
        s = s.strip()
        if not s:
            return None
        if " " in s and "T" not in s:
            s = s.replace(" ", "T", 1)
        if s.endswith("+00"):
            s = s + ":00"
        if s.isdigit():
            val = int(s)
            if val > 10_000_000_000:
                return int(val / 1000)
            return val
        try:
            return int(dt.datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp())
        except Exception:
            return None
    return None


def parse_clob_token_ids(raw: Any) -> List[str]:
    if raw is None:
        return []
    if isinstance(raw, list):
        return [str(x) for x in raw]
    if isinstance(raw, str):
        try:
            data = json.loads(raw)
            if isinstance(data, list):
                return [str(x) for x in data]
        except Exception:
            return []
    return []


def ts_to_utc_date(ts: int) -> str:
    return dt.datetime.utcfromtimestamp(ts).strftime("%Y-%m-%d")


def utc_date_range(start_ts: int, end_ts: int) -> List[str]:
    start_date = dt.datetime.utcfromtimestamp(start_ts).date()
    end_date = dt.datetime.utcfromtimestamp(end_ts).date()
    out: List[str] = []
    cur = start_date
    while cur <= end_date:
        out.append(cur.strftime("%Y-%m-%d"))
        cur += dt.timedelta(days=1)
    return out


def _request_with_retry(
    method: str,
    url: str,
    *,
    params: Optional[Dict[str, Any]] = None,
    json_body: Optional[Dict[str, Any]] = None,
    timeout: int = 30,
    headers: Optional[Dict[str, str]] = None,
) -> Any:
    headers = {"User-Agent": USER_AGENT, **(headers or {})}
    backoff = 1.0
    for _attempt in range(6):
        if method == "GET":
            resp = requests.get(url, params=params, headers=headers, timeout=timeout)
        else:
            resp = requests.post(url, params=params, json=json_body, headers=headers, timeout=timeout)

        if resp.status_code < 400:
            if resp.text:
                return resp.json()
            return None

        if resp.status_code in (429, 500, 502, 503, 504):
            time.sleep(backoff)
            backoff = min(backoff * 2, 16)
            continue

        raise RuntimeError(f"{method} {resp.url} failed: {resp.status_code} {resp.text[:300]}")

    raise RuntimeError(f"{method} {url} failed after retries")


def http_get(url: str, params: Optional[Dict[str, Any]] = None, timeout: int = 30) -> Any:
    return _request_with_retry("GET", url, params=params, timeout=timeout)


def http_post(
    url: str,
    params: Optional[Dict[str, Any]],
    json_body: Dict[str, Any],
    timeout: int = 30,
    headers: Optional[Dict[str, str]] = None,
) -> Any:
    return _request_with_retry("POST", url, params=params, json_body=json_body, timeout=timeout, headers=headers)


def fetch_markets(
    gamma_base: str,
    limit: int,
    max_markets: Optional[int],
    rate: RateLimiter,
    *,
    order: Optional[str] = None,
    ascending: Optional[bool] = None,
    end_date_min: Optional[str] = None,
) -> List[Dict[str, Any]]:
    markets: List[Dict[str, Any]] = []
    offset = 0
    while True:
        params: Dict[str, Any] = {"closed": "true", "limit": limit, "offset": offset}
        if order:
            params["order"] = order
        if ascending is not None:
            params["ascending"] = str(ascending).lower()
        if end_date_min:
            params["end_date_min"] = end_date_min

        rate.wait()
        batch = http_get(f"{gamma_base}/markets", params=params)
        if not batch:
            break

        markets.extend(batch)
        offset += limit

        if max_markets and len(markets) >= max_markets:
            markets = markets[:max_markets]
            break

    return markets


def parse_outcomes(outcomes_raw: Any, prices_raw: Any) -> Tuple[List[str], List[float]]:
    if isinstance(outcomes_raw, str):
        outcomes = json.loads(outcomes_raw)
    else:
        outcomes = outcomes_raw
    if isinstance(prices_raw, str):
        prices = [float(x) for x in json.loads(prices_raw)]
    else:
        prices = [float(x) for x in prices_raw]
    return outcomes, prices


def is_yes_no_market(m: Dict[str, Any]) -> bool:
    try:
        outcomes, _ = parse_outcomes(m.get("outcomes"), m.get("outcomePrices", []))
    except Exception:
        return False
    if len(outcomes) != 2:
        return False
    normalized = [o.strip().lower() for o in outcomes]
    return normalized == ["yes", "no"] or normalized == ["no", "yes"]


def extract_times(m: Dict[str, Any]) -> Tuple[Optional[int], Optional[int], Optional[int]]:
    start_ts = iso_to_ts(m.get("startDate")) or iso_to_ts(m.get("createdAt"))
    end_date_ts = iso_to_ts(m.get("endDate"))
    closed_ts = iso_to_ts(m.get("closedTime"))
    return start_ts, end_date_ts, closed_ts


def effective_end_ts(m: Dict[str, Any]) -> Optional[int]:
    # Use same idea as your original: closedTime OR endDate OR updatedAt
    _start, end_date_ts, closed_ts = extract_times(m)
    return closed_ts or end_date_ts or iso_to_ts(m.get("updatedAt"))


def infer_resolution_proxy(m: Dict[str, Any]) -> str:
    """
    Proxy: if final outcomePrices are extremely close to 0/1.
    Returns "YES", "NO", or "".
    """
    try:
        outcomes, prices = parse_outcomes(m.get("outcomes"), m.get("outcomePrices", []))
    except Exception:
        return ""
    if len(outcomes) != 2 or len(prices) != 2:
        return ""
    norm = [o.strip().lower() for o in outcomes]
    if "yes" not in norm or "no" not in norm:
        return ""
    yes_idx = norm.index("yes")
    no_idx = 1 - yes_idx
    yes_p = float(prices[yes_idx])
    no_p = float(prices[no_idx])
    if yes_p >= 0.99 and no_p <= 0.01:
        return "YES"
    if no_p >= 0.99 and yes_p <= 0.01:
        return "NO"
    return ""


def fetch_prices_history_daily(clob_base: str, token_id: str, start_ts: int, end_ts: int, rate: RateLimiter) -> Dict[str, float]:
    """
    Uses CLOB /prices-history with fidelity=1440 (daily).
    Returns dict date->price.
    """
    out: Dict[str, float] = {}
    max_days = 30
    cur = start_ts
    while cur < end_ts:
        window_days = max_days
        success = False
        while window_days >= 1 and not success:
            window_end = min(end_ts, cur + window_days * 86400)
            params = {"market": token_id, "startTs": cur, "endTs": window_end, "fidelity": 1440}
            try:
                rate.wait()
                data = http_get(f"{clob_base}/prices-history", params=params)
                history = data.get("history", []) if isinstance(data, dict) else []
                for item in history:
                    t = int(item.get("t"))
                    p = float(item.get("p"))
                    out[ts_to_utc_date(t)] = p
                success = True
                cur = window_end + 1
            except Exception as e:
                # CLOB sometimes complains about interval length: shrink.
                if "interval is too long" in str(e).lower():
                    window_days = window_days // 2
                    continue
                raise
        if not success:
            # advance one day to avoid infinite loops
            cur += 86400
    return out


# ---------------- Gemini classification: single character output ----------------

def write_market_texts(path: str, markets: List[Dict[str, Any]]) -> None:
    with open(path, "w", encoding="utf-8") as f:
        f.write("market_id,slug,title,description\n")
        for m in markets:
            mid = m.get("id", "")
            slug = m.get("slug", "")
            title = (m.get("question") or "").replace("\n", " ").replace(",", " ")
            desc = (m.get("description") or "").replace("\n", " ").replace(",", " ")
            f.write(f"{mid},{slug},{title},{desc}\n")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Download closed Yes/No Polymarket markets with daily probabilities, final outcome proxy, total volume, and T_days."
    )
    parser.add_argument("--months", type=int, default=6, help="How far back to look (months). Default: 6")
    parser.add_argument("--limit", type=int, default=100, help="Gamma API page size. Default: 100")
    parser.add_argument("--max-markets", type=int, default=20, help="Cap number of markets. Default: 20")
    parser.add_argument("--rps", type=float, default=5.0, help="Requests per second (Gamma/CLOB). Default: 5")
    parser.add_argument("--gamma-base", type=str, default=os.getenv("POLY_GAMMA_BASE", DEFAULT_GAMMA_BASE))
    parser.add_argument("--clob-base", type=str, default=os.getenv("POLY_CLOB_BASE", DEFAULT_CLOB_BASE))
    parser.add_argument("--out", type=str, default=os.getenv("POLY_OUT", "polymarket_output"))

    parser.add_argument("--order", type=str, default="endDate", help="Gamma order field. Default: endDate")
    parser.add_argument("--ascending", type=str, default="false", help="Gamma ascending true/false. Default: false")
    parser.add_argument("--use-api-date-filter", action="store_true", help="Use Gamma end_date_min filter for cutoff")

    args = parser.parse_args()

    os.makedirs(args.out, exist_ok=True)

    rate = RateLimiter(args.rps)

    cutoff = dt.datetime.utcnow() - dt.timedelta(days=int(args.months * 30.4))
    cutoff_ts = int(cutoff.timestamp())
    cutoff_iso = cutoff.replace(microsecond=0).isoformat() + "Z"

    markets = fetch_markets(
        args.gamma_base,
        args.limit,
        args.max_markets,
        rate,
        order=args.order,
        ascending=(args.ascending.lower() == "true"),
        end_date_min=cutoff_iso if args.use_api_date_filter else None,
    )

    filtered: List[Dict[str, Any]] = []
    total = yesno_ok = date_ok = clob_ok = 0

    for m in markets:
        total += 1
        if not is_yes_no_market(m):
            continue
        yesno_ok += 1

        end_ts = effective_end_ts(m)
        if not end_ts or end_ts < cutoff_ts:
            continue
        date_ok += 1

        if len(parse_clob_token_ids(m.get("clobTokenIds"))) != 2:
            continue
        clob_ok += 1

        filtered.append(m)

    print(f"Filter counts: total={total}, yesno_ok={yesno_ok}, date_ok={date_ok}, clob_ok={clob_ok}, final={len(filtered)}")

    # Save raw markets
    summary_path = os.path.join(args.out, "markets.jsonl")
    with open(summary_path, "w", encoding="utf-8") as fsum:
        for m in filtered:
            fsum.write(json.dumps(m) + "\n")

    daily_path = os.path.join(args.out, "daily.csv")
    texts_path = os.path.join(args.out, "market_texts.csv")

    write_market_texts(texts_path, filtered)

    with open(daily_path, "w", encoding="utf-8") as fdaily:
        fdaily.write(
            "market_id,slug,title,date,yes_price,no_price,total_volume,final_outcome_proxy,uma_resolution_status,"
            "T_days,start_ts,end_date_ts,closed_ts\n"
        )

        total_m = len(filtered)
        for idx, m in enumerate(filtered, start=1):
            market_id = m.get("id")
            slug = (m.get("slug") or "").strip()
            title = (m.get("title") or "").replace("\n", " ").replace("\r", " ").strip()
            description = (m.get("description") or "").replace("\n", " ").replace("\r", " ").strip()
            print(f"[{idx}/{total_m}] {slug}")

            # Total volume
            total_volume = m.get("volumeNum")
            if total_volume is None:
                try:
                    total_volume = float(m.get("volume"))
                except Exception:
                    total_volume = ""

            # Outcome proxy + UMA status
            final_outcome_proxy = infer_resolution_proxy(m)
            uma_status = m.get("umaResolutionStatus", "")

            outcomes, _ = parse_outcomes(m.get("outcomes"), m.get("outcomePrices", []))
            clob_ids = parse_clob_token_ids(m.get("clobTokenIds"))
            if len(clob_ids) != 2:
                continue

            # Map YES/NO to clob token ids
            if outcomes[0].strip().lower() == "yes":
                yes_token = clob_ids[0]
                no_token = clob_ids[1]
            else:
                yes_token = clob_ids[1]
                no_token = clob_ids[0]

            start_ts, end_date_ts, closed_ts = extract_times(m)
            end_ts = effective_end_ts(m)
            if not start_ts or not end_ts:
                continue

            now_ts = int(dt.datetime.utcnow().timestamp())
            if end_ts > now_ts:
                end_ts = now_ts
            if end_ts < start_ts:
                continue

            # T_days computed from endDate-startDate (not from closedTime)
            if start_ts and end_date_ts and end_date_ts >= start_ts:
                T_days: Any = (end_date_ts - start_ts) / 86400.0
            else:
                T_days = ""

            title_out = json.dumps(title, ensure_ascii=False)

            # Fetch daily prices
            try:
                yes_hist = fetch_prices_history_daily(args.clob_base, yes_token, start_ts, end_ts, rate)
                no_hist = fetch_prices_history_daily(args.clob_base, no_token, start_ts, end_ts, rate)
            except Exception as e:
                print(f"Warning: price history failed for {slug}: {e}", file=sys.stderr)
                continue

            all_dates = utc_date_range(start_ts, end_ts)
            last_yes: Optional[float] = None
            last_no: Optional[float] = None

            for d in all_dates:
                if d in yes_hist:
                    last_yes = yes_hist[d]
                if d in no_hist:
                    last_no = no_hist[d]

                yes_p = last_yes if last_yes is not None else ""
                no_p = last_no if last_no is not None else ""

                fdaily.write(
                    f"{market_id},{slug},{title_out},{d},"
                    f"{yes_p},{no_p},{total_volume},{final_outcome_proxy},{uma_status},"
                    f"{T_days},{start_ts or ''},{end_date_ts or ''},{closed_ts or ''}\n"
                )

    print(f"Wrote markets: {summary_path}")
    print(f"Wrote daily series: {daily_path}")
    print(f"Wrote market texts: {texts_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
