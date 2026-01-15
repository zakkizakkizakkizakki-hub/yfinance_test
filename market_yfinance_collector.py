# save as: market_yfinance_collector.py
from __future__ import annotations

import csv
import os
import time
import random
from datetime import datetime, timedelta, timezone
from typing import Dict, Tuple, Optional

import pandas as pd
import yfinance as yf

JST = timezone(timedelta(hours=9))

# 出力CSV（ファイル名は固定：毎回追記）
OUT_CSV = "market_yfinance_log.csv"

CSV_ENCODING = "utf-8-sig"
CSV_QUOTING = csv.QUOTE_ALL
CSV_LINETERMINATOR = "\n"

# 取得対象
ASSETS: Dict[str, str] = {
    "USDJPY": "JPY=X",
    "BTC": "BTC-USD",
    "Gold": "GC=F",
    "US10Y": "^TNX",
    "Oil": "CL=F",
    "VIX": "^VIX",
}

# yfinance設定
YF_PERIOD = "7d"
YF_INTERVAL = "1d"
RETRIES = 4
BASE_SLEEP = 3.0  # seconds

def now_jst_str() -> str:
    return datetime.now(JST).strftime("%Y-%m-%d %H:%M:%S")

def _sleep_jitter(base: float) -> None:
    time.sleep(base + random.uniform(0.5, 2.0))

def _extract_last_close(df: pd.DataFrame) -> Tuple[Optional[float], str]:
    """
    Returns (price, date_str). price None if cannot parse.
    """
    if df is None or df.empty:
        return None, ""

    # yfinanceは列がMultiIndexになる場合がある
    if "Close" in df.columns:
        s = df["Close"]
        if isinstance(s, pd.DataFrame):
            s = s.iloc[:, 0]
    else:
        s = df.iloc[:, 0]

    s = pd.to_numeric(s, errors="coerce").dropna()
    if s.empty:
        return None, ""
    price = float(s.iloc[-1])
    date_str = str(s.index[-1])[:10]
    return price, date_str

def fetch_one(ticker: str) -> Tuple[float, int, str, str]:
    """
    returns: (value, missing_flag, date_str, fail_reason)
    """
    last_err = ""
    for _ in range(RETRIES):
        try:
            _sleep_jitter(BASE_SLEEP)
            df = yf.download(
                ticker,
                period=YF_PERIOD,
                interval=YF_INTERVAL,
                progress=False,
            )
            price, d = _extract_last_close(df)
            if price is None or not (price > 0):
                last_err = "EmptyDF"
                continue
            return float(price), 0, d, ""
        except Exception as e:
            last_err = type(e).__name__
            continue

    return 0.0, 1, "", last_err or "Unknown"

def collect() -> None:
    print("=== yfinance market fetch ===")
    ts = now_jst_str()
    print(f"timestamp_jst: {ts}")

    row = {"timestamp_jst": ts}

    for name, ticker in ASSETS.items():
        v, miss, d, fail = fetch_one(ticker)
        row[name] = float(v)
        row[f"{name}_missing"] = int(miss)
        row[f"{name}_date"] = d
        row[f"{name}_fail"] = fail

        ok_mark = "✅" if miss == 0 else "❌"
        src = "yfinance" if miss == 0 else "missing"
        print(f"[{src}] {name}({ticker}): {v} ({d}) {ok_mark} fail={fail}")

    out_df = pd.DataFrame([row])
    header = not (os.path.exists(OUT_CSV) and os.path.getsize(OUT_CSV) > 0)
    out_df.to_csv(
        OUT_CSV,
        mode="a",
        index=False,
        header=header,
        encoding=CSV_ENCODING,
        quoting=CSV_QUOTING,
        lineterminator=CSV_LINETERMINATOR,  # ★ここ重要（line_terminator じゃない）
    )
    print(f"=== saved -> {OUT_CSV} ===")

if __name__ == "__main__":
    collect()
