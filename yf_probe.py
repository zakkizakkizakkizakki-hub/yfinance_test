# yf_probe.py
from __future__ import annotations
import sys
import platform
from datetime import datetime, timezone, timedelta

import pandas as pd
import yfinance as yf

JST = timezone(timedelta(hours=9))

TICKERS = {
    "USDJPY": "JPY=X",
    "BTC": "BTC-USD",
    "Gold": "GC=F",
    "US10Y": "^TNX",
    "Oil": "CL=F",
    "VIX": "^VIX",
}

def now_jst_str() -> str:
    return datetime.now(JST).strftime("%Y-%m-%d %H:%M:%S")

def fetch_one(ticker: str) -> tuple[float, str, str]:
    """
    return: (price, date_str, fail_reason)
    """
    try:
        df = yf.download(
            ticker,
            period="7d",
            interval="1d",
            progress=False,
            threads=False,   # 並列を切って負荷を下げる（切り分け用）
        )
        if df is None or df.empty:
            return 0.0, "", "EmptyDF"

        s = df["Close"]
        if isinstance(s, pd.DataFrame):
            s = s.iloc[:, 0]
        s = pd.to_numeric(s, errors="coerce").dropna()
        if s.empty:
            return 0.0, "", "NoClose"

        price = float(s.iloc[-1])
        date_str = str(s.index[-1])[:10]
        return price, date_str, ""
    except Exception as e:
        return 0.0, "", f"{type(e).__name__}: {e}"

def main() -> None:
    print("=== yfinance probe ===")
    print("timestamp_jst:", now_jst_str())
    print("python:", sys.version.replace("\n", " "))
    print("platform:", platform.platform())

    rows = []
    for name, ticker in TICKERS.items():
        price, d, fail = fetch_one(ticker)
        ok = (price > 0 and fail == "")
        print(f"[yfinance] {name}({ticker}): {price} ({d}) {'✅' if ok else '❌'} fail={fail}")
        rows.append({
            "timestamp_jst": now_jst_str(),
            "name": name,
            "ticker": ticker,
            "price": price,
            "date": d,
            "ok": int(ok),
            "fail": fail,
        })

    out = pd.DataFrame(rows)
    out.to_csv("market_yfinance.csv", index=False, encoding="utf-8-sig")
    print("=== saved -> market_yfinance.csv ===")

    # ここでは「落とさない」：切り分け用にログを残すだけ
    # （本番は monitor で落とす設計のままでOK）

if __name__ == "__main__":
    main()
