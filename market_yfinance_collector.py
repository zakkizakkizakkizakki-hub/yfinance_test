# save as: market_yfinance_collector.py
from __future__ import annotations

import os
import csv
import time
import random
import shutil
from datetime import datetime, timedelta, timezone
from typing import Dict, Tuple, Optional, List

import pandas as pd
import yfinance as yf

# =========================
# Config
# =========================
JST = timezone(timedelta(hours=9))

OUT_CSV = "market_yfinance_log.csv"
CSV_ENCODING = "utf-8-sig"
CSV_QUOTING = csv.QUOTE_ALL
CSV_LINETERMINATOR = "\n"  # NOTE: pandas uses "lineterminator" (not line_terminator)

ASSETS: Dict[str, str] = {
    "USDJPY": "JPY=X",
    "BTC": "BTC-USD",
    "Gold": "GC=F",
    "US10Y": "^TNX",
    "Oil": "CL=F",
    "VIX": "^VIX",
}

# yfinance fetch parameters
YF_PERIOD = "7d"
YF_INTERVAL = "1d"

# retry behavior
RETRY_MAX = 4                 # total attempts per symbol
BASE_SLEEP_SEC = 8            # base wait between retries
JITTER_SEC = (0.5, 2.5)       # random add
BATCH_SLEEP_SEC = 4           # small wait between symbols

# =========================
# CSV Schema (固定: 31列)
# =========================
def expected_columns() -> List[str]:
    cols = ["timestamp_jst"]
    for name in ASSETS.keys():
        cols += [
            name,                        # price
            f"{name}_missing",           # 0/1
            f"{name}_source",            # yfinance/missing
            f"{name}_date",              # YYYY-MM-DD
            f"{name}_fail",              # reason string
        ]
    return cols


# =========================
# Utilities
# =========================
def now_jst_str() -> str:
    return datetime.now(JST).strftime("%Y-%m-%d %H:%M:%S")


def file_has_data(path: str) -> bool:
    return os.path.exists(path) and os.path.getsize(path) > 0


def quarantine_if_header_mismatch(path: str, cols_expected: List[str]) -> None:
    """
    CSVが既に存在する場合、ヘッダを検査する。
    - 期待ヘッダと一致しない → 退避（quarantine）して新規作成に切り替える
    """
    if not file_has_data(path):
        return

    try:
        with open(path, "r", encoding=CSV_ENCODING, newline="") as f:
            first_line = f.readline()
        if not first_line:
            return

        # CSVとしてヘッダを正しく分割（QUOTE_ALLでも安全）
        header = next(csv.reader([first_line]))
        if header == cols_expected:
            return

        ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        bad = f"{os.path.splitext(path)[0]}.bad_{ts}{os.path.splitext(path)[1]}"
        shutil.move(path, bad)
        print(f"[preflight] header mismatch -> quarantined: {path} -> {bad}")
        print(f"[preflight] expected cols={len(cols_expected)}, got={len(header)}")

    except Exception as e:
        # 読めない/壊れてる等も隔離
        ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        bad = f"{os.path.splitext(path)[0]}.bad_{ts}{os.path.splitext(path)[1]}"
        try:
            shutil.move(path, bad)
            print(f"[preflight] header read failed -> quarantined: {path} -> {bad}")
        except Exception:
            pass
        print(f"[preflight] reason: {type(e).__name__}: {e}")


def sleep_jitter(base: float) -> None:
    time.sleep(base + random.uniform(*JITTER_SEC))


# =========================
# yfinance fetch
# =========================
def fetch_one(ticker: str) -> Tuple[Optional[float], str, str]:
    """
    Returns: (price, fail_reason, date_str)
    - price is None on failure
    """
    last_err = ""
    for attempt in range(1, RETRY_MAX + 1):
        try:
            if attempt > 1:
                # Exponential-ish backoff
                wait = BASE_SLEEP_SEC * (attempt - 1)
                sleep_jitter(wait)

            df = yf.download(
                ticker,
                period=YF_PERIOD,
                interval=YF_INTERVAL,
                progress=False,
            )

            if df is None or df.empty:
                last_err = "EmptyDF"
                continue

            # Close優先（複数列のことがあるので安全に）
            if "Close" in df.columns:
                s = df["Close"]
                if isinstance(s, pd.DataFrame):
                    s = s.iloc[:, 0]
            else:
                s = df.iloc[:, 0]

            series = pd.to_numeric(s, errors="coerce").dropna()
            if series.empty:
                last_err = "NoDataAfterDrop"
                continue

            price = float(series.iloc[-1])
            if not (price > 0):
                last_err = "NonPositive"
                continue

            # date
            idx = series.index[-1]
            date_str = str(idx)[:10]

            # rounding (表示用)
            if ticker == "JPY=X":
                price = round(price, 6)
            else:
                price = round(price, 6)

            return price, "", date_str

        except Exception as e:
            # yfinance が RateLimit のときもここに来る（例: YFRateLimitError）
            last_err = type(e).__name__
            continue

    return None, last_err or "UnknownError", ""


# =========================
# Main
# =========================
def collect() -> int:
    print("=== yfinance market fetch ===")

    cols = expected_columns()
    quarantine_if_header_mismatch(OUT_CSV, cols)

    row: Dict[str, object] = {"timestamp_jst": now_jst_str()}

    for name, ticker in ASSETS.items():
        sleep_jitter(BATCH_SLEEP_SEC)

        price, fail, date_str = fetch_one(ticker)

        if price is None:
            row[name] = 0.0
            row[f"{name}_missing"] = 1
            row[f"{name}_source"] = "missing"
            row[f"{name}_date"] = ""
            row[f"{name}_fail"] = fail or "EmptyDF"
            print(f"[missing] {name}({ticker}): 0.0 () ❌ fail={row[f'{name}_fail']}")
        else:
            row[name] = float(price)
            row[f"{name}_missing"] = 0
            row[f"{name}_source"] = "yfinance"
            row[f"{name}_date"] = date_str
            row[f"{name}_fail"] = ""
            print(f"[yfinance] {name}({ticker}): {price} ({date_str}) ✅ fail=")

    # 31列固定で出力
    out_df = pd.DataFrame([row]).reindex(columns=cols)

    header = not file_has_data(OUT_CSV)
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
    return 0


if __name__ == "__main__":
    raise SystemExit(collect())
