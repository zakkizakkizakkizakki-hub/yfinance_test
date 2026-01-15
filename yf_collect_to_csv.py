# yf_collect_to_csv.py
from __future__ import annotations

import os
import csv
from datetime import datetime, timezone, timedelta
from typing import Dict, Tuple, Optional

import pandas as pd
import yfinance as yf

JST = timezone(timedelta(hours=9))

# 収集したい銘柄（あなたの目的の6つ）
ASSETS: Dict[str, str] = {
    "USDJPY": "JPY=X",
    "BTC": "BTC-USD",
    "Gold": "GC=F",
    "US10Y": "^TNX",
    "Oil": "CL=F",
    "VIX": "^VIX",
}

OUT_DIR = "data"
OUT_CSV = os.path.join(OUT_DIR, "market_yfinance_log.csv")

YF_PERIOD = "7d"
YF_INTERVAL = "1d"


def now_jst_str() -> str:
    return datetime.now(JST).strftime("%Y-%m-%d %H:%M:%S")


def fetch_last_close(ticker: str) -> Tuple[Optional[float], str]:
    """
    取れたら (price, date_yyyy_mm_dd)
    取れなければ (None, "")
    """
    df = yf.download(ticker, period=YF_PERIOD, interval=YF_INTERVAL, progress=False)
    if df is None or df.empty:
        return None, ""

    s = df["Close"]
    if isinstance(s, pd.DataFrame):
        s = s.iloc[:, 0]

    s = pd.to_numeric(s, errors="coerce").dropna()
    if s.empty:
        return None, ""

    price = float(s.iloc[-1])
    date_str = str(s.index[-1])[:10]
    return price, date_str


def main() -> None:
    os.makedirs(OUT_DIR, exist_ok=True)

    row = {
        "timestamp_jst": now_jst_str(),
    }

    # 取得
    for name, ticker in ASSETS.items():
        price, d = fetch_last_close(ticker)
        row[f"{name}"] = price if price is not None else 0.0
        row[f"{name}_date"] = d
        row[f"{name}_missing"] = 0 if price is not None else 1

    # CSV追記（ヘッダは初回だけ）
    file_exists = os.path.exists(OUT_CSV) and os.path.getsize(OUT_CSV) > 0
    with open(OUT_CSV, "a", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=list(row.keys()), quoting=csv.QUOTE_ALL)
        if not file_exists:
            w.writeheader()
        w.writerow(row)

    # 画面にも出す（Actionsログ用）
    print("=== yfinance -> csv appended ===")
    print(f"saved: {OUT_CSV}")
    print(row)


if __name__ == "__main__":
    main()
