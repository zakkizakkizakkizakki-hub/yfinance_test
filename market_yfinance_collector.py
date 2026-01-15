# save as: market_yfinance_collector.py
from __future__ import annotations

import os
import time
import random
from datetime import datetime, timedelta, timezone
from typing import Dict, Tuple

import pandas as pd
import yfinance as yf

# =====================
# CONFIG
# =====================
JST = timezone(timedelta(hours=9))

OUT_CSV = "market_yfinance_log.csv"

ASSETS = {
    "USDJPY": "JPY=X",
    "BTC": "BTC-USD",
    "Gold": "GC=F",
    "US10Y": "^TNX",
    "Oil": "CL=F",
    "VIX": "^VIX",
}

CSV_ENCODING = "utf-8-sig"
MAX_RETRIES = 3
BASE_DELAY_SEC = 3.0


def now_jst_str() -> str:
    return datetime.now(JST).strftime("%Y-%m-%d %H:%M:%S")


def sleep_jitter(base: float) -> None:
    time.sleep(base + random.uniform(0.5, 2.0))


def fetch_one(ticker: str) -> Tuple[float, int, str, str]:
    """
    return: (value, missing_flag, date_str, fail_reason)
    """
    last_fail = ""
    for _ in range(MAX_RETRIES):
        try:
            sleep_jitter(BASE_DELAY_SEC)

            df = yf.download(
                ticker,
                period="7d",
                interval="1d",
                progress=False,
                threads=False,  # GitHub上で暴走しにくくする
            )

            if df is None or df.empty:
                last_fail = "EmptyDF_or_NoClose"
                continue

            # Close列の取り出し（MultiIndex対策）
            if "Close" in df.columns:
                close = df["Close"]
                if isinstance(close, pd.DataFrame):
                    close = close.iloc[:, 0]
                close = pd.to_numeric(close, errors="coerce").dropna()
            else:
                close = pd.to_numeric(df.iloc[:, 0], errors="coerce").dropna()

            if close.empty:
                last_fail = "EmptyDF_or_NoClose"
                continue

            val = float(close.iloc[-1])
            if not (val > 0):
                last_fail = "NonPositive"
                continue

            date_str = str(close.index[-1])[:10]
            return val, 0, date_str, ""

        except Exception as e:
            last_fail = type(e).__name__
            continue

    return 0.0, 1, "", last_fail


def main() -> None:
    ts = now_jst_str()
    row: Dict[str, object] = {"timestamp_jst": ts}

    print("=== yfinance market fetch ===")
    print(f"timestamp_jst: {ts}")

    for name, ticker in ASSETS.items():
        val, miss, date_str, fail = fetch_one(ticker)

        row[name] = val
        row[f"{name}_missing"] = miss
        row[f"{name}_date"] = date_str if date_str else None
        row[f"{name}_fail"] = fail if fail else None

        ok = "✅" if miss == 0 else "❌"
        print(f"[yfinance] {name}({ticker}): {val} ({date_str}) {ok} fail={fail}")

    df = pd.DataFrame([row])

    header = not (os.path.exists(OUT_CSV) and os.path.getsize(OUT_CSV) > 0)
    df.to_csv(
        OUT_CSV,
        mode="a",
        index=False,
        header=header,
        encoding=CSV_ENCODING,
    )

    print(f"=== saved -> {OUT_CSV} ===")


if __name__ == "__main__":
    main()
