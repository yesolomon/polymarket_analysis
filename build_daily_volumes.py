#!/usr/bin/env python3
import argparse
import csv
import json
import os
import time
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Tuple, Any, Optional

import requests


class RateLimiter:
    def __init__(self, rps: float):
        self.min_interval = 1.0 / max(rps, 0.1)
        self.last = 0.0

    def wait(self):
        now = time.time()
        elapsed = now - self.last
        if elapsed < self.min_interval:
            time.sleep(self.min_interval - elapsed)
        self.last = time.time()


def request_json(method: str, url: str, *, params=None, timeout=30, headers=None, max_attempts=5, limiter: Optional[RateLimiter] = None):
    backoff = 1.0
    last_err = None
    for attempt in range(1, max_attempts + 1):
        if limiter:
            limiter.wait()
        try:
            if method == "GET":
                resp = requests.get(url, params=params, timeout=timeout, headers=headers)
            else:
                resp = requests.post(url, params=params, timeout=timeout, headers=headers)
            if resp.status_code < 400:
                if not resp.text:
                    return None
                return resp.json()
            last_err = f"HTTP {resp.status_code}: {resp.text[:300]}"
            if resp.status_code in (429, 500, 502, 503, 504):
                time.sleep(backoff)
                backoff = min(backoff * 2, 16)
                continue
            raise RuntimeError(last_err)
        except Exception as e:
            last_err = str(e)
            time.sleep(backoff)
            backoff = min(backoff * 2, 16)
            continue
    raise RuntimeError(f"{method} {url} failed after retries: {last_err}")


def read_needed_dates(daily_path: Path) -> Dict[str, List[str]]:
    needed = defaultdict(set)
    with daily_path.open() as f:
        reader = csv.DictReader(f)
        for row in reader:
            mid = row.get("market_id", "")
            date = row.get("date", "")
            if mid and date:
                needed[mid].add(date)
    # convert to sorted lists for stable output
    return {k: sorted(v) for k, v in needed.items()}


def cache_path(cache_dir: Path, name: str) -> Path:
    cache_dir.mkdir(parents=True, exist_ok=True)
    return cache_dir / name


def fetch_gamma_market(gamma_base: str, market_id: str, timeout: int, limiter: RateLimiter, cache_dir: Path) -> Optional[Dict[str, Any]]:
    cache_file = cache_path(cache_dir, f"gamma_{market_id}.json")
    if cache_file.exists():
        try:
            return json.loads(cache_file.read_text())
        except Exception:
            pass

    url = f"{gamma_base}/markets/{market_id}"
    data = request_json("GET", url, timeout=timeout, limiter=limiter)
    if data is None:
        return None
    try:
        cache_file.write_text(json.dumps(data))
    except Exception:
        pass
    return data


def load_agg_cache(cache_dir: Path, condition_id: str) -> Optional[Tuple[Dict[str, float], Dict[str, int], int]]:
    cache_file = cache_path(cache_dir, f"agg_{condition_id}.json")
    if not cache_file.exists():
        return None
    try:
        data = json.loads(cache_file.read_text())
        return data["vol_by_date"], data["cnt_by_date"], int(data.get("truncated", 0))
    except Exception:
        return None


def write_agg_cache(cache_dir: Path, condition_id: str, vol_by_date: Dict[str, float], cnt_by_date: Dict[str, int], truncated: int):
    cache_file = cache_path(cache_dir, f"agg_{condition_id}.json")
    payload = {
        "vol_by_date": vol_by_date,
        "cnt_by_date": cnt_by_date,
        "truncated": int(truncated),
    }
    try:
        cache_file.write_text(json.dumps(payload))
    except Exception:
        pass


def trades_cache_path(cache_dir: Path, condition_id: str) -> Path:
    return cache_path(cache_dir, f"trades_{condition_id}.jsonl")


def fetch_all_trades(data_base: str, condition_id: str, timeout: int, limiter: RateLimiter, cache_dir: Path) -> Tuple[List[Dict[str, Any]], int]:
    # If jsonl cache exists, load it
    jsonl = trades_cache_path(cache_dir, condition_id)
    if jsonl.exists():
        trades = []
        with jsonl.open() as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    trades.append(json.loads(line))
                except Exception:
                    continue
        return trades, 0

    trades: List[Dict[str, Any]] = []
    limit = 500
    offset = 0
    truncated = 0

    while True:
        if offset > 3000:
            truncated = 1
            break
        url = f"{data_base}/trades"
        params = {"market": condition_id, "limit": limit, "offset": offset}
        try:
            batch = request_json("GET", url, params=params, timeout=timeout, limiter=limiter)
        except Exception as e:
            msg = str(e)
            if "max historical activity offset of 3000 exceeded" in msg:
                truncated = 1
                break
            raise
        if not isinstance(batch, list):
            batch = []
        trades.extend(batch)
        # append to cache as we go
        if batch:
            try:
                with jsonl.open("a") as f:
                    for t in batch:
                        f.write(json.dumps(t) + "\n")
            except Exception:
                pass
        if len(batch) < limit:
            break
        offset += limit

    return trades, truncated


def trade_to_date(trade: Dict[str, Any]) -> Optional[str]:
    ts = trade.get("timestamp")
    if ts is None:
        return None
    try:
        ts = int(float(ts))
    except Exception:
        return None
    return time.strftime("%Y-%m-%d", time.gmtime(ts))


def aggregate_trades(trades: List[Dict[str, Any]]) -> Tuple[Dict[str, float], Dict[str, int]]:
    vol_by_date: Dict[str, float] = defaultdict(float)
    cnt_by_date: Dict[str, int] = defaultdict(int)
    for t in trades:
        date = trade_to_date(t)
        if not date:
            continue
        price = t.get("price")
        size = t.get("size")
        try:
            price = float(price)
            size = float(size)
        except Exception:
            continue
        vol_by_date[date] += price * size
        cnt_by_date[date] += 1
    return dict(vol_by_date), dict(cnt_by_date)


def min_trade_date(trades: List[Dict[str, Any]]) -> Optional[str]:
    dates = []
    for t in trades:
        d = trade_to_date(t)
        if d:
            dates.append(d)
    return min(dates) if dates else None


def main() -> int:
    parser = argparse.ArgumentParser(description="Build daily trade volumes from Polymarket Data API")
    parser.add_argument("--daily", default="daily.csv")
    parser.add_argument("--out", default="daily_volumes.csv")
    parser.add_argument("--gamma-base", default="https://gamma-api.polymarket.com")
    parser.add_argument("--data-base", default="https://data-api.polymarket.com")
    parser.add_argument("--rps", type=float, default=5.0)
    parser.add_argument("--timeout", type=int, default=30)
    parser.add_argument("--cache-dir", default="cache")
    args = parser.parse_args()

    daily_path = Path(args.daily)
    out_path = Path(args.out)
    cache_dir = Path(args.cache_dir)

    needed = read_needed_dates(daily_path)
    market_ids = sorted(needed.keys())
    total = len(market_ids)

    limiter = RateLimiter(args.rps)

    ok = 0
    gamma_fail = 0
    trades_fail = 0
    truncated_count = 0

    with out_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["market_id", "date", "daily_volume", "trade_count", "truncated"])

        for i, mid in enumerate(market_ids, start=1):
            print(f"[{i}/{total}] {mid}")
            gamma = None
            try:
                gamma = fetch_gamma_market(args.gamma_base, mid, args.timeout, limiter, cache_dir)
            except Exception as e:
                print(f"  WARNING: gamma fetch failed for {mid}: {e}")
            if not gamma or not gamma.get("conditionId"):
                gamma_fail += 1
                for d in needed[mid]:
                    writer.writerow([mid, d, 0.0, 0, 0])
                continue

            condition_id = str(gamma.get("conditionId"))

            # Load cached aggregation if present
            cached = load_agg_cache(cache_dir, condition_id)
            if cached:
                vol_by_date, cnt_by_date, truncated = cached
            else:
                try:
                    trades, truncated = fetch_all_trades(args.data_base, condition_id, args.timeout, limiter, cache_dir)
                    vol_by_date, cnt_by_date = aggregate_trades(trades)
                    write_agg_cache(cache_dir, condition_id, vol_by_date, cnt_by_date, truncated)
                except Exception as e:
                    print(f"  WARNING: trades fetch failed for {mid} (condition {condition_id}): {e}")
                    trades_fail += 1
                    for d in needed[mid]:
                        writer.writerow([mid, d, 0.0, 0, 0])
                    continue

            if truncated:
                truncated_count += 1

            # If truncated, mark dates earlier than the earliest fetched trade as truncated=1
            min_date = None
            if truncated:
                min_date = min_trade_date(trades) if 'trades' in locals() else None

            for d in needed[mid]:
                is_trunc = 0
                if truncated and min_date is not None and d <= min_date:
                    is_trunc = 1
                writer.writerow([
                    mid,
                    d,
                    0.0 if is_trunc else float(vol_by_date.get(d, 0.0)),
                    0 if is_trunc else int(cnt_by_date.get(d, 0)),
                    int(is_trunc),
                ])
            ok += 1

    print(f"Done. markets={total} ok={ok} gamma_fail={gamma_fail} trades_fail={trades_fail} truncated={truncated_count}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
