# save as: market_collector.py
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

# 主列（この列名は壊さない）
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

# 異常値検知（切り分け用に“フラグを立てるだけ”。値は捨てない）
ANOMALY_PCT = {
    "USDJPY": 0.05,
    "BTC":    0.20,
    "Gold":   0.05,
    "US10Y":  0.20,
    "Oil":    0.20,
    "VIX":    0.40,
}

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
        return pd.read_csv(path, encoding="utf-8-sig")
    except Exception:
        try:
            return pd.read_csv(path, encoding="utf-8-sig", engine="python", on_bad_lines="skip")
        except Exception:
            return pd.DataFrame()

def _last_good_value(df: pd.DataFrame, col: str) -> Optional[float]:
    if df.empty or col not in df.columns:
        return None
    s = pd.to_numeric(df[col], errors="coerce")
    s = s[(s.notna()) & (s > 0)]
    if s.empty:
        return None
    return float(s.iloc[-1])

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
            threads=False,  # GitHub Actionsでの安定性優先
        )
        v, err, d = _extract_last_close(df)
        return v, err, d
    except Exception as e:
        return None, type(e).__name__, ""

def fetch_with_retries(name: str, ticker: str, prev_value: Optional[float]) -> Dict[str, object]:
    last_err = ""
    last_date = ""

    for attempt in range(RETRIES):
        v, err, d = fetch_yfinance_once(ticker)
        last_err = err
        last_date = d

        if v is None:
            _sleep_backoff(attempt)
            continue

        value = float(v)

        anomaly = 0
        anomaly_reason = ""
        if prev_value is not None and prev_value > 0:
            pct = abs(value - prev_value) / prev_value
            th = ANOMALY_PCT.get(name, 0.30)
            if pct >= th:
                anomaly = 1
                anomaly_reason = f"jump_pct={pct:.3f}>=th={th:.3f} (prev={prev_value}, now={value})"

        return {
            "value": value,
            "ok": 1,
            "source": "yfinance",
            "date": last_date,
            "fail": "",
            "anomaly": anomaly,
            "anomaly_reason": anomaly_reason,
        }

    return {
        "value": 0.0,
        "ok": 0,
        "source": "missing",
        "date": last_date,
        "fail": last_err or "Unknown",
        "anomaly": 0,
        "anomaly_reason": "",
    }

def collect() -> None:
    print("=== yfinance market fetch ===")
    print(f"timestamp_jst: {now_jst_str()}")

    hist = _safe_read_csv(OUT_CSV)

    # 主列（維持）
    row: Dict[str, object] = {"timestamp_jst": now_jst_str()}
    # 追加列（切り分け用）
    extra: Dict[str, object] = {}

    for name, ticker in ASSETS.items():
        prev = _last_good_value(hist, name)
        r = fetch_with_retries(name, ticker, prev)

        row[name] = float(r["value"])

        extra[f"{name}_ok"] = int(r["ok"])
        extra[f"{name}_source"] = str(r["source"])
        extra[f"{name}_date"] = str(r["date"])
        extra[f"{name}_fail"] = str(r["fail"])
        extra[f"{name}_anomaly"] = int(r["anomaly"])
        extra[f"{name}_anomaly_reason"] = str(r["anomaly_reason"])

        mark = "✅" if r["ok"] else "❌"
        print(f"[{r['source']}] {name}({ticker}): {r['value']} ({r['date']}) {mark} fail={r['fail']}")
        if r["anomaly"]:
            print(f"  [warn] anomaly: {name} {r['anomaly_reason']}")

    out_row = {**row, **extra}
    out_df = pd.DataFrame([out_row])

    header = not _file_has_data(OUT_CSV)
    out_df.to_csv(
        OUT_CSV,
        mode="a",
        index=False,
        header=header,
        encoding="utf-8-sig",
        line_terminator="\n",
    )

    print(f"=== saved -> {OUT_CSV} ===")

if __name__ == "__main__":
    collect()
