# Polymarket Data Processing

This repo contains a small pipeline to download Polymarket market data, build daily probability series, and classify markets with an LLM.

## Data Outputs (polymarket_output)

These are the **current, canonical** data files you should use:

- `polymarket_output/daily.csv`
  - Daily probability time series for each market (one row per market per day).
- `polymarket_output/market_texts.csv`
  - Title + description for each market.
- `polymarket_output/market_metadata.csv`
  - LLM-extracted metadata (type, domain, and occurrence/deadline date) for each market.
- `polymarket_output/daily_volumes.csv`
  - Daily notional trade volume and trade counts per market per day, computed from trades.

## Notebooks

- `polymarket_analysis.ipynb` — calibration analysis notebook.

## Dataset Size (current)

- `daily.csv`: **96,065** rows across **2,758** markets.
- `market_metadata.csv`: **2,758** rows total — **2,397 ok**, **361 error**.
  - `ok` means the LLM returned valid metadata; `error` means it did not (invalid/empty response).

## How Data Is Built

1. **Download closed markets and daily series**
   - Script: `polymarket_closed_markets_daily.py`
   - Pulls closed Yes/No markets from Polymarket’s Gamma API and uses the CLOB price history endpoint to build daily series.
   - Command used:
     ```
     python3 /Users/elchanansolomon/Desktop/Codex/polymarket_closed_markets_daily.py \
       --max-markets 10000 \
       --months 36 \
       --limit 100 \
       --rps 5 \
       --out /Users/elchanansolomon/Desktop/Codex/polymarket_output
     ```
   - Time range rule:
     - Computes a cutoff as `UTC now - (36 * 30.4 days)`.
     - Keeps markets whose effective end time (closedTime/endDate) is **>= cutoff**.
   - Writes `daily.csv` and `market_texts.csv`.

2. **Classify markets with Groq**
   - Script: `classify_markets_groq_domains.py`
   - Uses an LLM to extract:
     - **Type** (1/2/U)
       - Type 1: resolves on a single calendar date known in advance.
       - Type 2: resolves over a window or on a date not known in advance.
       - U: no explicit date/deadline in title/description.
     - **Domain** (finance/sports/politics/misc)
    - **Date** (DD/MM/YYYY), extracted from title/description only.
  - Overwrites results to `market_metadata.csv`.

3. **Augment daily volumes**
   - Script: `build_daily_volumes.py`
   - For each market_id in `daily.csv`, fetches the Gamma market to get `conditionId`, then pulls trades from the Polymarket Data API.
   - Aggregates daily notional volume as `sum(size * price)` per UTC day.
   - Writes `daily_volumes.csv` with columns: `market_id,date,daily_volume,trade_count,truncated`.
     - `truncated=1` indicates the trades API pagination hit its offset cap; those days are incomplete (and may be zeroed).

## Recommended Run Order

1. `polymarket_closed_markets_daily.py`
2. `classify_markets_groq_domains.py`
3. `build_daily_volumes.py`

## Notes

- The classification date is extracted from the **title/description only**, not from Polymarket’s end/close timestamps.
- Market time series may end before a market’s close if there were no trades/prices on later days.

## HTML Viewer

`market_viewer.html` is a single‑file dashboard for exploring one market at a time.
It reads data embedded in the HTML (generated from `daily.csv`, `market_texts.csv`, and `market_metadata.csv`).

**What you see and how it’s computed:**

- **Market (dropdown)**  
  The market slug. Selecting a slug updates all fields and the chart.

- **Type**  
  From `market_metadata.csv` (`type`), produced by the LLM:  
  `1` = resolves on a single known calendar date; `2` = resolves over a range/unknown date; `U` = no explicit date/deadline.

- **Domain**  
  From `market_metadata.csv` (`domain`): finance / sports / politics / misc.

- **Occurrence/Deadline**  
  From `market_metadata.csv` (`occurrence_or_deadline_ddmmyyyy`).  
  This is the date used as the **x‑axis reference**.

- **Metadata Status / Error**  
  From `market_metadata.csv` (`status`, `error`).  
  `ok` means the LLM returned valid metadata; `error` means it did not.

- **Final Outcome**  
  From `daily.csv` (`final_outcome_proxy`). This is a proxy YES/NO outcome captured at download time.

- **Total Volume**  
  From `daily.csv` (`total_volume`). This is total market volume (not daily).

- **Title / Description**  
  From `market_texts.csv` (fallback to `daily.csv` title if needed).

**Chart (YES probability over time):**

- **Y‑axis**: `yes_price` from `daily.csv`.
- **X‑axis**: **days before the occurrence/deadline date**, computed as:  
  `days_left = (occurrence/deadline date) - (row date)` in days.  
  The occurrence/deadline date is parsed in **DD/MM/YYYY** format.
- If the metadata date is missing, the chart falls back to days-left computed against the market’s last
  available close/end timestamp, or to index order if that is missing.
