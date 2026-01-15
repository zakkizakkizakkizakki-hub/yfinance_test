# save as: market_yfinance_collector.py
from __future__ import annotations

import os
import time
import random
from datetime import datetime, timedelta, timezone
from typing import Dict, Optional, Tuple

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

RETRIES = 4
BASE_DELAY_SEC = 3.0
MAX_DELAY_SEC = 20.0

CSV_ENCODING = "utf-8-sig"
CSV_LINETERMINATOR = "\n"   # ← 正しい指定先は lineterminator

# =========================
# Utils
# =========================
def now_jst() -> datetime:
    return datetime.now(JST)

def now_jst_str() -> str:
    return now_jst().strftime("%Y-%m-%d %H:%M:%S")

def _sleep_backoff(attempt: int) -> None:
    delay = min(MAX_DELAY_SEC, BASE_DELAY_SEC * (2 ** attempt))
    jitter = random.uniform(0.2, 1.2)
    time.sleep(delay + jitter)

def _file_has_data(path: str) -> bool:
    return os.path.exists(path) and os.path.getsize(path) > 0

def _safe_read_csv(path: str) -> pd.DataFrame:
    if not _file_has_data(path):
        return pd.DataFrame()
    try:
        return pd.read_csv(path, encoding=CSV_ENCODING)
    except Exception:
        try:
            return pd.read_csv(path, encoding=CSV_ENCODING, engine="python", on_bad_lines="skip")
        except Exception:
            return pd.DataFrame()

def _extract_last_close(df: pd.DataFrame) -> Tuple[Optional[float], str, str]:
    if df is None or df.empty:
        return None, "EmptyDF", ""

    if "Close" in df.columns:
        close = df["Close"]
        if isinstance(close, pd.DataFrame):
            close = close.iloc[:, 0]
        series = pd.to_numeric(close, errors="coerce").dropna()
    else:
        series = pd.to_numeric(df.iloc[:, 0], errors="coerce").dropna()

    if series.empty:
        return None, "NoCloseData", ""

    v = float(series.iloc[-1])
    if not (v > 0):
        return None, "NonPositive", ""

    d = str(series.index[-1])[:10]
    return v, "", d

def fetch_yfinance_once(ticker: str) -> Tuple[Optional[float], str, str]:
    try:
        df = yf.download(
            ticker,
            period=YF_PERIOD,
            interval=YF_INTERVAL,
            progress=False,
            threads=False,      # Actionsでの安定性優先
            auto_adjust=False,  # yfinance側のデフォルト変更ログを抑えたい場合
        )
        v, err, d = _extract_last_close(df)
        return v, err, d
    except Exception as e:
        return None, type(e).__name__, ""

def fetch_with_retries(ticker: str) -> Tuple[float, int, str, str]:
    """
    returns: (value, ok, date, fail_reason)
    """
    last_err = ""
    last_date = ""
    for attempt in range(RETRIES):
        v, err, d = fetch_yfinance_once(ticker)
        last_err = err
        last_date = d
        if v is not None:
            return float(v), 1, last_date, ""
        _sleep_backoff(attempt)

    # 取得できない場合でも「落とさず」0で記録して理由を残す（切り分け用）
    return 0.0, 0, last_date, last_err or "Unknown"

def collect() -> None:
    print("=== yfinance market fetch ===")
    print(f"timestamp_jst: {now_jst_str()}")

    hist = _safe_read_csv(OUT_CSV)

    row: Dict[str, object] = {"timestamp_jst": now_jst_str()}
    extra: Dict[str, object] = {}

    for name, ticker in ASSETS.items():
        value, ok, d, fail = fetch_with_retries(ticker)
        row[name] = value
        extra[f"{name}_ok"] = ok
        extra[f"{name}_date"] = d
        extra[f"{name}_fail"] = fail

        mark = "✅" if ok else "❌"
        print(f"[{'yfinance' if ok else 'missing'}] {name}({ticker}): {value} ({d}) {mark} fail={fail}")

    out_row = {**row, **extra}
    out_df = pd.DataFrame([out_row])

    header = not _file_has_data(OUT_CSV)
    out_df.to_csv(
        OUT_CSV,
        mode="a",
        index=False,
        header=header,
        encoding=CSV_ENCODING,
        lineterminator=CSV_LINETERMINATOR,  # ✅ 正しい引数名
    )

    print(f"=== saved -> {OUT_CSV} ===")

if __name__ == "__main__":
    collect()
