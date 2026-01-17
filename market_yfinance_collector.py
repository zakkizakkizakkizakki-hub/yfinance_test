# save as: market_yfinance_collector.py
from __future__ import annotations

import os
import time
import random
from datetime import datetime, timedelta, timezone
from typing import Dict, Tuple, Optional

import pandas as pd
import yfinance as yf

# =========================
# Config
# =========================
JST = timezone(timedelta(hours=9))

OUT_CSV = "market_yfinance_log.csv"
ENCODING = "utf-8-sig"
CSV_QUOTING = 1  # csv.QUOTE_ALL 相当（pandas側で数値指定）
LINE_TERMINATOR = "\n"  # pandasは lineterminator

ASSETS = {
    "USDJPY": "JPY=X",
    "BTC": "BTC-USD",
    "Gold": "GC=F",
    "US10Y": "^TNX",
    "Oil": "CL=F",
    "VIX": "^VIX",
}

# yfinance取得設定
YF_PERIOD = "7d"
YF_INTERVAL = "1d"

# Retry / backoff
MAX_TRIES = 4
BASE_SLEEP = 5.0         # seconds
BACKOFF_MULT = 3.0       # 5s -> 15s -> 45s -> 135s
JITTER_MIN = 0.5
JITTER_MAX = 3.0

# “異常値検知”のための最低限ルール（値が0以下なら欠損扱い）
# ここは監視を強めたいなら後で拡張できます
def _is_valid_price(x: Optional[float]) -> bool:
    try:
        if x is None:
            return False
        v = float(x)
        return v > 0.0
    except Exception:
        return False


def now_jst_str() -> str:
    return datetime.now(JST).strftime("%Y-%m-%d %H:%M:%S")


def _sleep_with_backoff(attempt: int) -> None:
    # attempt: 1..MAX_TRIES-1 (次の試行前)
    base = BASE_SLEEP * (BACKOFF_MULT ** (attempt - 1))
    time.sleep(base + random.uniform(JITTER_MIN, JITTER_MAX))


def _pick_last_close(df: pd.DataFrame) -> Tuple[Optional[float], str]:
    """
    yfinance download の DataFrame から最後の Close を拾う。
    返り値: (price, date_str)
    """
    if df is None or df.empty:
        return None, ""

    if "Close" in df.columns:
        s = df["Close"]
        if isinstance(s, pd.DataFrame):
            # まれに複数列になるケース
            s = s.iloc[:, 0]
    else:
        # 念のため
        s = df.iloc[:, 0]

    s = pd.to_numeric(s, errors="coerce").dropna()
    if s.empty:
        return None, ""

    price = float(s.iloc[-1])
    idx = s.index[-1]
    # indexがTimestampなら日付を取る
    try:
        date_str = str(pd.to_datetime(idx).date())
    except Exception:
        date_str = ""
    return price, date_str


def fetch_one_ticker(ticker: str) -> Tuple[float, int, str, str]:
    """
    返り値: (price, missing_flag, date_str, fail_reason)
    """
    last_err = ""
    for i in range(1, MAX_TRIES + 1):
        try:
            df = yf.download(
                ticker,
                period=YF_PERIOD,
                interval=YF_INTERVAL,
                progress=False,
                threads=False,  # 余計な並列を避ける（レート制限悪化を避けたい）
            )
            price, date_str = _pick_last_close(df)

            if _is_valid_price(price):
                # OK
                return float(price), 0, date_str, ""

            last_err = "EmptyDF_or_NoClose"
        except Exception as e:
            last_err = type(e).__name__

        if i < MAX_TRIES:
            _sleep_with_backoff(i)

    # 全滅
    return 0.0, 1, "", last_err or "UnknownFail"


def _ensure_schema(df: pd.DataFrame) -> pd.DataFrame:
    """
    CSV仕様（25列）を固定化。
    """
    cols = ["timestamp_jst"]
    for a in ASSETS.keys():
        cols += [a, f"{a}_missing", f"{a}_date", f"{a}_fail"]

    for c in cols:
        if c not in df.columns:
            df[c] = ""

    return df[cols]


def append_csv(row: Dict[str, object]) -> None:
    out_df = pd.DataFrame([row])
    out_df = _ensure_schema(out_df)

    header = not (os.path.exists(OUT_CSV) and os.path.getsize(OUT_CSV) > 0)
    out_df.to_csv(
        OUT_CSV,
        mode="a",
        index=False,
        header=header,
        encoding=ENCODING,
        quoting=CSV_QUOTING,
        lineterminator=LINE_TERMINATOR,  # ← 正しい引数名
    )


def collect() -> None:
    print("=== yfinance market fetch ===")
    ts = now_jst_str()
    print(f"timestamp_jst: {ts}")

    row: Dict[str, object] = {"timestamp_jst": ts}

    for asset, ticker in ASSETS.items():
        price, miss, d, fail = fetch_one_ticker(ticker)
        row[asset] = float(price)
        row[f"{asset}_missing"] = int(miss)
        row[f"{asset}_date"] = d if d else float("nan")
        row[f"{asset}_fail"] = fail if fail else float("nan")

        mark = "✅" if miss == 0 else "❌"
        print(f"[yfinance] {asset}({ticker}): {price} ({d}) {mark} fail={fail}")

    append_csv(row)
    print(f"=== saved -> {OUT_CSV} ===")


if __name__ == "__main__":
    collect()
