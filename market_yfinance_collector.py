# save as: market_yfinance_collector.py
from __future__ import annotations

import csv
import os
import random
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Tuple, Optional

import pandas as pd
import yfinance as yf

JST = timezone(timedelta(hours=9))

ASSETS: Dict[str, str] = {
    "USDJPY": "JPY=X",
    "BTC": "BTC-USD",
    "Gold": "GC=F",
    "US10Y": "^TNX",
    "Oil": "CL=F",
    "VIX": "^VIX",
}

OUT_CSV = Path("market_yfinance_log.csv")
RETRY_CSV = Path("retry_trials.csv")

CSV_ENCODING = "utf-8-sig"
CSV_LINETERMINATOR = "\n"
CSV_QUOTING = csv.QUOTE_ALL

YF_PERIOD = "7d"
YF_INTERVAL = "1d"

# リトライ設定（検証しやすいように明示）
RETRIES = 4
BASE_SLEEP_SEC = 20          # 1回失敗したら最低これだけ待つ
JITTER_SEC = 10              # 追加で0〜これだけランダム待ち
BACKOFF_MULT = 2.0           # 失敗が続くと待ち時間を伸ばす


def now_jst_str() -> str:
    return datetime.now(JST).strftime("%Y-%m-%d %H:%M:%S")


def _append_retry_row(row: dict) -> None:
    header_needed = not RETRY_CSV.exists() or RETRY_CSV.stat().st_size == 0
    with RETRY_CSV.open("a", encoding=CSV_ENCODING, newline="") as f:
        w = csv.DictWriter(
            f,
            fieldnames=[
                "timestamp_jst",
                "asset",
                "ticker",
                "attempt",
                "ok",
                "fail_reason",
                "rows",
                "last_date",
            ],
            quoting=CSV_QUOTING,
            lineterminator=CSV_LINETERMINATOR,
        )
        if header_needed:
            w.writeheader()
        w.writerow(row)


def _sleep(attempt: int) -> None:
    # 0回目失敗→BASE、1回目失敗→BASE*2、2回目失敗→BASE*4 …（+ jitter）
    sec = BASE_SLEEP_SEC * (BACKOFF_MULT ** max(0, attempt - 1))
    sec += random.uniform(0, JITTER_SEC)
    time.sleep(sec)


def fetch_one(ticker: str) -> Tuple[Optional[float], str, str]:
    """
    return: (price or None, fail_reason, date_str)
    """
    try:
        df = yf.download(
            ticker,
            period=YF_PERIOD,
            interval=YF_INTERVAL,
            progress=False,
        )
        if df is None or df.empty:
            return None, "EmptyDF", ""

        # Closeを優先
        if "Close" in df.columns:
            s = df["Close"]
            if isinstance(s, pd.DataFrame):
                s = s.iloc[:, 0]
            s = pd.to_numeric(s, errors="coerce").dropna()
        else:
            s = pd.to_numeric(df.iloc[:, 0], errors="coerce").dropna()

        if s.empty:
            return None, "NoNumericData", ""

        price = float(s.iloc[-1])
        if not (price > 0):
            return None, "NonPositive", ""

        date_str = str(s.index[-1])[:10]
        return price, "", date_str

    except Exception as e:
        return None, type(e).__name__, ""


def collect() -> int:
    ts = now_jst_str()
    print("=== yfinance market fetch ===")
    print(f"timestamp_jst: {ts}")

    # 固定スキーマ（ここを変えない限り、CSVは壊れません）
    row: dict = {"timestamp_jst": ts}
    for name, ticker in ASSETS.items():
        row[f"{name}_ticker"] = ticker
        row[name] = 0.0
        row[f"{name}_date"] = ""
        row[f"{name}_ok"] = 0
        row[f"{name}_fail"] = ""

    for name, ticker in ASSETS.items():
        last_fail = ""
        last_date = ""
        got: Optional[float] = None

        for attempt in range(1, RETRIES + 1):
            price, fail, date_str = fetch_one(ticker)

            ok = int(price is not None and price > 0)
            _append_retry_row(
                {
                    "timestamp_jst": ts,
                    "asset": name,
                    "ticker": ticker,
                    "attempt": attempt,
                    "ok": ok,
                    "fail_reason": fail,
                    "rows": 0,
                    "last_date": date_str,
                }
            )

            if ok:
                got = float(price)
                last_date = date_str
                last_fail = ""
                break

            last_fail = fail or "UnknownFail"
            last_date = date_str or ""
            _sleep(attempt)

        if got is not None:
            row[name] = got
            row[f"{name}_date"] = last_date
            row[f"{name}_ok"] = 1
            row[f"{name}_fail"] = ""
            print(f"[yfinance] {name}({ticker}): {got} ({last_date}) ✅")
        else:
            row[name] = 0.0
            row[f"{name}_date"] = ""
            row[f"{name}_ok"] = 0
            row[f"{name}_fail"] = last_fail or "EmptyDF"
            print(f"[missing] {name}({ticker}): 0.0 () ❌ fail={row[f'{name}_fail']}")

    # 出力（クォート固定・列固定）
    header_needed = not OUT_CSV.exists() or OUT_CSV.stat().st_size == 0
    with OUT_CSV.open("a", encoding=CSV_ENCODING, newline="") as f:
        fieldnames = list(row.keys())
        w = csv.DictWriter(
            f,
            fieldnames=fieldnames,
            quoting=CSV_QUOTING,
            lineterminator=CSV_LINETERMINATOR,
        )
        if header_needed:
            w.writeheader()
        w.writerow(row)

    print(f"=== saved -> {OUT_CSV.name} ===")
    return 0


if __name__ == "__main__":
    raise SystemExit(collect())
