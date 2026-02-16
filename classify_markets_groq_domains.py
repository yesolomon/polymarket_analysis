#!/usr/bin/env python3
import argparse
import csv
import json
import os
import time
from datetime import datetime
from typing import Dict, Any, Optional, Tuple, List

import requests

DEFAULT_GROQ_API_BASE = "https://api.groq.com/openai/v1"
DEFAULT_GROQ_MODEL = "llama-3.3-70b-versatile"
USER_AGENT = "market-classifier/1.0"


def is_ddmmyyyy(s: str) -> bool:
    try:
        datetime.strptime(s, "%d/%m/%Y")
        return True
    except Exception:
        return False


def parse_ts(val: Any) -> Optional[int]:
    if val is None or val == "":
        return None
    try:
        return int(float(val))
    except Exception:
        return None


def _request_once(method: str, url: str, *, headers=None, json_body=None, timeout=60) -> Any:
    if method == "POST":
        resp = requests.post(url, json=json_body, headers=headers, timeout=timeout)
    else:
        resp = requests.get(url, headers=headers, timeout=timeout)

    if resp.status_code < 400:
        return resp.json() if resp.text else None

    raise RuntimeError(f"HTTP {resp.status_code}: {resp.text[:500]}")


def parse_response(text: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    try:
        data = json.loads(text)
        t = str(data.get("type", "")).strip()
        d = str(data.get("domain", "")).strip()
        date_str = str(data.get("date", "")).strip()
        if t not in ("1", "2", "U"):
            return None, None, None
        if d not in ("finance", "sports", "politics", "misc"):
            return None, None, None
        if date_str and not is_ddmmyyyy(date_str):
            return None, None, None
        if date_str == "" and t != "U":
            return None, None, None
        if date_str != "" and t == "U":
            return None, None, None
        return t, d, date_str
    except Exception:
        return None, None, None


def classify_market(api_base: str, api_key: str, model: str, title: str, description: str, max_attempts: int) -> Dict[str, str]:
    system_text = (
        "You are a strict information-extraction engine.\n\n"
        "Input: the TITLE and DESCRIPTION of a Polymarket market.\n\n"
        "Output: JSON ONLY with exactly these keys:\n"
        '  {"type": "1"|"2"|"U", "domain": "finance"|"sports"|"politics"|"misc", "date": "DD/MM/YYYY"|"", "reason": ""}\n\n'
        "Rules for TYPE (must follow exactly):\n"
        "1) First, locate all explicit time expressions in the text (dates, months+year, year-only, quarters, relative deadlines, ranges).\n"
        "2) If there is NO explicit date/deadline anywhere in title/description, then:\n"
        '     type="U" and date="".\n'
        "3) Otherwise determine whether the resolution day is known in advance as a SINGLE calendar date:\n"
        '   - Type="1" ONLY if the market explicitly states (or unambiguously implies) a single specific calendar date '
        '(e.g., "on 05 Nov 2026", "November 5, 2026", "2026-11-05") AND there are no range/deadline markers.\n'
        '   - Type="2" if the market can resolve over a range of dates OR on a single date not knowable in advance. This includes ANY of:\n'
        '       * "by", "before", "until", "through", "between X and Y", "from X to Y"\n'
        '       * "within N days", "in the next N days", "at any point"\n'
        '       * "one day after / N days after / after launch / after listing" (unknown trigger date)\n'
        '       * "in 2026", "in 2027", "this year", quarters like "Q1 2026" (not a single day)\n'
        '   - If the text contains both a fixed single-day event date AND a range/deadline clause, treat it as Type="2".\n\n'
        "Rules for DATE:\n"
        '- If type="U": date="".\n'
        '- If type="1": date must be the single calendar date (DD/MM/YYYY) found in the text.\n'
        '- If type="2": date must be the deadline/end of the stated range:\n'
        '    * "by <date>" => that date\n'
        '    * "before <date>" => that date (deadline)\n'
        '    * "until <date>" => that date\n'
        '    * "between <start> and <end>" => use <end>\n'
        '    * "from <start> to <end>" => use <end>\n'
        '    * "end of month <Month YYYY>" => last day of that month\n'
        '    * "end of year <YYYY>" or "in <YYYY>" or "during <YYYY>" => 31/12/YYYY\n'
        '    * Quarter: "Q1 YYYY" => 31/03/YYYY, "Q2" => 30/06, "Q3" => 30/09, "Q4" => 31/12\n'
        '    * If only a month+year is given (e.g., "by July 2026"), interpret as last day of that month.\n'
        "- Never use any dataset fields or external knowledge. Use title/description only.\n\n"
        "Rules for DOMAIN:\n"
        "- finance: crypto, token, FDV, price targets, ETFs, stocks, rates, inflation, CPI, earnings, macro, commodities.\n"
        "- sports: leagues/teams/matches/tournaments/athletes/scoring.\n"
        "- politics: elections, candidates, parties, governments, legislation, wars/diplomacy when framed as political outcomes.\n"
        "- misc: everything else.\n"
        "Choose ONE domain only.\n\n"
        "Formatting constraints:\n"
        "- Output must be valid JSON (double quotes, no trailing commas).\n"
        '- date must be exactly DD/MM/YYYY or "".\n'
        "- reason must be a very short string (<= 120 chars) describing what time expression you used; do NOT include extra keys.\n"
    )
    user_text = f"Title: {title}\nDescription: {description}\n"

    url = f"{api_base}/chat/completions"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "User-Agent": USER_AGENT,
    }

    for attempt in range(1, max_attempts + 1):
        payload = {
            "model": model,
            "messages": [
                {"role": "system", "content": system_text},
                {"role": "user", "content": user_text},
            ],
            "temperature": 0,
            "max_tokens": 64,
            "response_format": {"type": "json_object"}
        }
        resp = _request_once("POST", url, headers=headers, json_body=payload, timeout=60)
        text = ""
        try:
            text = resp["choices"][0]["message"]["content"].strip()
        except Exception:
            text = ""

        t, d, date_str = parse_response(text)
        if t and d and date_str is not None:
            return {"type": t, "domain": d, "date": date_str, "status": "ok", "error": ""}

        if attempt < max_attempts:
            time.sleep(0.5)
            continue

    return {"type": "", "domain": "", "date": "", "status": "error", "error": "invalid_response"}


def load_market_texts(texts_path: str) -> Dict[str, Dict[str, str]]:
    texts = {}
    with open(texts_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            slug = row.get("slug", "")
            if not slug:
                continue
            texts[slug] = {
                "title": row.get("title", ""),
                "description": row.get("description", ""),
            }
    return texts


def load_slugs_from_daily(daily_path: str) -> List[str]:
    slugs = []
    with open(daily_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            slug = row.get("slug", "")
            if slug and slug not in slugs:
                slugs.append(slug)
    return slugs




def main() -> int:
    parser = argparse.ArgumentParser(description="Classify markets (Type + Domain) using Groq")
    parser.add_argument("--daily", default="polymarket_output/daily.csv")
    parser.add_argument("--texts", default="polymarket_output/market_texts.csv")
    parser.add_argument("--out", default="polymarket_output/market_metadata.csv")
    parser.add_argument("--groq-api-key", default=os.getenv("GROQ_API_KEY", ""))
    parser.add_argument("--groq-api-base", default=os.getenv("GROQ_API_BASE", DEFAULT_GROQ_API_BASE))
    parser.add_argument("--groq-model", default=os.getenv("GROQ_MODEL", DEFAULT_GROQ_MODEL))
    parser.add_argument("--max-attempts", type=int, default=1)
    parser.add_argument("--delay", type=float, default=0.5)
    parser.add_argument("--failure-delay", type=float, default=2.0)
    args = parser.parse_args()

    if not args.groq_api_key:
        print("ERROR: GROQ_API_KEY is required.")
        return 2

    texts = load_market_texts(args.texts)
    slugs = load_slugs_from_daily(args.daily)
    slugs = [s for s in slugs if s in texts]
    total = len(slugs)
    with open(args.out, "w", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["slug", "type", "domain", "occurrence_or_deadline_ddmmyyyy", "status", "error"])
        f.flush()

        for idx, slug in enumerate(slugs, start=1):
            title = texts[slug]["title"]
            desc = texts[slug]["description"]
            try:
                result = classify_market(args.groq_api_base, args.groq_api_key, args.groq_model, title, desc, args.max_attempts)
            except Exception as e:
                print(f"ERROR for {slug}: {e}")
                result = {"type": "", "domain": "", "date": "", "status": "error", "error": "request_failed"}

            t = result.get("type", "")
            d = result.get("domain", "")
            date_str = result.get("date", "")
            status = result.get("status", "error")
            error = result.get("error", "")

            writer.writerow([slug, t, d, date_str, status, error])
            f.flush()
            print(f"Completed {idx}/{total} — {slug}")
            time.sleep(args.delay)
            if status == "error" or error == "request_failed":
                time.sleep(args.failure_delay)

    print(f"Wrote {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
