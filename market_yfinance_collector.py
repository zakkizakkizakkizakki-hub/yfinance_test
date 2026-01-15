# save as: market_yfinance_collector.py
from __future__ import annotations

import os
import time
import csv
import random
from datetime import datetime, timezone, timedelta
from typing import Dict, Tuple, Optional

import pandas as pd
import yfinance as yf


# =========================
# Config
# =========================
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

YF_PERIOD = "7d"
YF_INTERVAL = "1d"

RETRIES = 3
BASE_DELAY = 4  # seconds

CSV_ENCODING = "utf-8-sig"
CSV_QUOTING = csv.QUOTE_ALL
CSV_LINETERMINATOR = "\n"


# =========================
# Utils
# =========================
def now_jst_str() -> str:
    return datetime.now(JST).strftime("%Y-%m-%d %H:%M:%S")


def _sleep_backoff(i: int) -> None:
    # 1回目: 4-6秒 / 2回目: 8-12秒 / 3回目: 16-24秒
    base = BASE_DELAY * (2 ** i)
    time.sleep(base + random.uniform(0.5, 2.0))


def _file_has_data(path: str) -> bool:
    return os.path.exists(path) and os.path.getsize(path) > 0


def _extract_last_close(df: pd.DataFrame, ticker: str) -> Tuple[Optional[float], str]:
    """
    yfinance download結果から ticker の終値を取り出す
    成功: (price, date_str)
    失敗: (None, "")
    """
    if df is None or df.empty:
        return None, ""

    # 複数ティッカー指定時は columns が MultiIndex になる
    # 例: df["Close"][ticker]
    try:
        if isinstance(df.columns, pd.MultiIndex):
            if ("Close", ticker) in df.columns:
                s = df[("Close", ticker)]
            elif "Close" in df.columns.get_level_values(0):
                # Close層からtickerを探す
                s = df["Close"][ticker]
            else:
                return None, ""
        else:
            # 単一ティッカー時
            if "Close" not in df.columns:
                return None, ""
            s = df["Close"]

        s = pd.to_numeric(s, errors="coerce").dropna()
        if s.empty:
            return None, ""

        price = float(s.iloc[-1])
        date_str = str(s.index[-1])[:10]
        if price <= 0:
            return None, ""

        return price, date_str
    except Exception:
        return None, ""


# =========================
# Core
# =========================
def fetch_all_once() -> pd.DataFrame:
    tickers = " ".join(ASSETS.values())

    # yfinanceは1銘柄ずつ叩くとレート制限を踏みやすいので、
    # まとめて1回で取得する方針にする
    df = yf.download(
        tickers=tickers,
        period=YF_PERIOD,
        interval=YF_INTERVAL,
        progress=False,
        group_by="column",
        threads=False,
    )
    return df


def collect() -> None:
    ts = now_jst_str()
    print("=== yfinance market fetch ===")
    print(f"timestamp_jst: {ts}")

    last_err = ""
    df_all = None

    for i in range(RETRIES):
        try:
            _sleep_backoff(i)
            df_all = fetch_all_once()
            if df_all is None or df_all.empty:
                last_err = "EmptyDF"
                continue
            break
        except Exception as e:
            last_err = type(e).__name__
            df_all = None
            continue

    row: Dict[str, object] = {"timestamp_jst": ts}

    for name, ticker in ASSETS.items():
        price, date_str = _extract_last_close(df_all, ticker)
        if price is None:
            row[name] = 0.0
            row[f"{name}_missing"] = 1
            row[f"{name}_date"] = ""
            row[f"{name}_fail"] = last_err or "EmptyDF_or_NoClose"
            print(f"[yfinance] {name}({ticker}): 0.0 (missing) ❌ fail={row[f'{name}_fail']}")
        else:
            row[name] = float(price)
            row[f"{name}_missing"] = 0
            row[f"{name}_date"] = date_str
            row[f"{name}_fail"] = ""
            print(f"[yfinance] {name}({ticker}): {price} ({date_str}) ✅ fail=")

    out_df = pd.DataFrame([row])
    header = not _file_has_data(OUT_CSV)

    out_df.to_csv(
        OUT_CSV,
        mode="a",
        index=False,
        header=header,
        encoding=CSV_ENCODING,
        quoting=CSV_QUOTING,
        lineterminator=CSV_LINETERMINATOR,
    )

    print(f"=== saved -> {OUT_CSV} ===")


if __name__ == "__main__":
    collect()
