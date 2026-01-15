# save as: market_yfinance_collector.py
from __future__ import annotations

import os
import csv
import time
import random
from datetime import datetime, timedelta, timezone
from typing import Dict, Tuple, Optional

import pandas as pd
import yfinance as yf


JST = timezone(timedelta(hours=9))

# 既存CSV仕様（壊さない）
OUT_CSV = "market_yfinance_log.csv"
CSV_ENCODING = "utf-8-sig"
CSV_QUOTING = csv.QUOTE_ALL
CSV_LINETERMINATOR = "\n"  # ← pandas引数は lineterminator

ASSETS: Dict[str, str] = {
    "USDJPY": "JPY=X",
    "BTC": "BTC-USD",
    "Gold": "GC=F",
    "US10Y": "^TNX",
    "Oil": "CL=F",
    "VIX": "^VIX",
}

# yfinance settings
YF_PERIOD = "7d"
YF_INTERVAL = "1d"
YF_RETRIES = 4
BASE_DELAY_SEC = 4


def now_jst_str() -> str:
    return datetime.now(JST).strftime("%Y-%m-%d %H:%M:%S")


def _sleep_backoff(i: int) -> None:
    # exponential-ish backoff with jitter
    base = BASE_DELAY_SEC * (1.6 ** i)
    time.sleep(base + random.uniform(0.5, 2.0))


def _file_has_data(path: str) -> bool:
    return os.path.exists(path) and os.path.getsize(path) > 0


def _extract_last_close(df: pd.DataFrame, ticker: str) -> Tuple[Optional[float], str, str]:
    """
    Returns: (price, date_str, fail_reason)
    - price: last Close (float) or None
    - date_str: YYYY-MM-DD or ""
    - fail_reason: "" if ok else reason
    """
    if df is None or df.empty:
        return None, "", "EmptyDF"

    # yf.download with multiple tickers often yields MultiIndex columns:
    # columns like ('Close','BTC-USD') or ('BTC-USD','Close') depending on options.
    try:
        # Case A: MultiIndex columns
        if isinstance(df.columns, pd.MultiIndex):
            # Try common shapes
            # 1) ('Close', 'TICKER')
            if ("Close", ticker) in df.columns:
                s = df[("Close", ticker)]
            # 2) ('TICKER', 'Close')
            elif (ticker, "Close") in df.columns:
                s = df[(ticker, "Close")]
            else:
                # last resort: search any column that contains Close for this ticker
                candidates = [c for c in df.columns if "Close" in c and ticker in c]
                if not candidates:
                    return None, "", "NoCloseColumn"
                s = df[candidates[0]]
        else:
            # Case B: single ticker: normal columns
            if "Close" not in df.columns:
                return None, "", "NoCloseColumn"
            s = df["Close"]

        s = pd.to_numeric(s, errors="coerce").dropna()
        if s.empty:
            return None, "", "NoDataAfterDrop"

        price = float(s.iloc[-1])
        if not (price > 0):
            return None, "", "NonPositive"

        idx = s.index[-1]
        date_str = str(idx)[:10]
        return price, date_str, ""
    except Exception as e:
        return None, "", f"ParseErr:{type(e).__name__}"


def _download_batch(tickers: list[str]) -> Tuple[pd.DataFrame, str]:
    """
    Returns (df, fail_reason). fail_reason empty if success-ish (even if some tickers empty).
    """
    last_err = ""
    for i in range(YF_RETRIES):
        try:
            if i > 0:
                _sleep_backoff(i - 1)

            df = yf.download(
                tickers=tickers,
                period=YF_PERIOD,
                interval=YF_INTERVAL,
                group_by="column",
                threads=False,      # ← GitHub Actionsで暴れやすいのを抑える
                progress=False,
                auto_adjust=False,  # ← 警告抑制・挙動固定
            )
            return df, ""
        except Exception as e:
            last_err = type(e).__name__
            continue

    return pd.DataFrame(), last_err or "DownloadFailed"


def collect() -> None:
    print("=== yfinance market fetch ===")
    print(f"timestamp_jst: {now_jst_str()}")

    # 1) batch download（リクエスト回数を最小化）
    tickers = list(ASSETS.values())
    df_all, batch_err = _download_batch(tickers)

    # 2) 既存CSV仕様を維持しつつ、追加で理由列も出す（後方互換）
    row: Dict[str, object] = {"timestamp_jst": now_jst_str()}

    # 追加のログ列（壊さない：末尾に増えるだけ）
    row["yf_batch_fail"] = batch_err

    for name, ticker in ASSETS.items():
        price, date_str, fail = _extract_last_close(df_all, ticker)

        missing = 0
        if price is None:
            missing = 1
            price = 0.0

        # 既存列（維持）
        row[name] = float(price)
        row[f"{name}_date"] = date_str
        row[f"{name}_missing"] = int(missing)

        # 追加列（理由ログ）
        # 既存仕様は壊さず、monitor側が見たいときだけ見る
        row[f"{name}_fail"] = fail if fail else (batch_err if batch_err else "")

        status = "✅" if missing == 0 else "❌"
        print(f"[yfinance] {name}({ticker}): {row[name]} ({date_str}) {status} fail={row[f'{name}_fail']}")

    out_df = pd.DataFrame([row])

    header = not _file_has_data(OUT_CSV)
    out_df.to_csv(
        OUT_CSV,
        mode="a",
        index=False,
        header=header,
        encoding=CSV_ENCODING,
        quoting=CSV_QUOTING,
        lineterminator=CSV_LINETERMINATOR,  # ← 正しい引数名
    )

    print(f"=== saved -> {OUT_CSV} ===")


if __name__ == "__main__":
    collect()
