# save as: market_yfinance_collector.py
from __future__ import annotations

import csv
import json
import os
import random
import shutil
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Tuple

import pandas as pd
import requests
import yfinance as yf

# =========================
# Config
# =========================
JST = timezone(timedelta(hours=9))

OUT_CSV = "market_yfinance_log.csv"
RETRY_TRIALS_CSV = "retry_trials.csv"
YAHOO_HTTP_PROBE_JSONL = "yahoo_http_probe.jsonl"

CSV_ENCODING = "utf-8-sig"
CSV_QUOTING = csv.QUOTE_ALL
CSV_LINETERMINATOR = "\n"

ASSETS: Dict[str, str] = {
    "USDJPY": "JPY=X",
    "BTC": "BTC-USD",
    "Gold": "GC=F",
    "US10Y": "^TNX",
    "Oil": "CL=F",
    "VIX": "^VIX",
}

# yfinance 取得設定
YF_PERIOD = "5d"
YF_INTERVAL = "1d"

# リトライ設定
MAX_RETRIES = 4
BASE_SLEEP = 15.0  # seconds
JITTER_MAX = 2.0   # seconds

# =========================
# Helpers
# =========================
def now_jst() -> datetime:
    return datetime.now(JST)

def now_jst_str() -> str:
    return now_jst().strftime("%Y-%m-%d %H:%M:%S")

def utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def _ts_suffix() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

def make_run_id() -> str:
    """
    3ファイルを必ず同じIDで紐づけるための run_id。
    GitHub Actions なら GITHUB_RUN_ID / GITHUB_RUN_ATTEMPT も取り込む。
    """
    gh_run_id = os.getenv("GITHUB_RUN_ID", "").strip()
    gh_attempt = os.getenv("GITHUB_RUN_ATTEMPT", "").strip()
    base = f"gh{gh_run_id}-a{gh_attempt}" if gh_run_id else "local"
    return f"{base}-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{uuid.uuid4().hex[:8]}"

def _expected_columns() -> List[str]:
    cols = ["run_id", "timestamp_jst"]
    for a in ASSETS.keys():
        cols += [
            a,
            f"{a}_missing",
            f"{a}_source",
            f"{a}_date",
            f"{a}_fail",
        ]
    # 2 + 6*5 = 32 columns
    return cols

EXPECTED_COLS = _expected_columns()

def _read_first_line(path: str) -> str:
    with open(path, "r", encoding=CSV_ENCODING, errors="ignore") as f:
        return f.readline().strip("\r\n")

def _quarantine(path: str, reason: str) -> str:
    bad = f"{os.path.splitext(path)[0]}.bad_{_ts_suffix()}.csv"
    shutil.move(path, bad)
    print(f"[quarantine] {path} -> {bad}  reason={reason}")
    return bad

def _ensure_csv_header_or_quarantine(path: str) -> None:
    """
    - 既存CSVのヘッダが想定と違う / パース不能なら隔離して新規にする
    """
    if not os.path.exists(path):
        return
    if os.path.getsize(path) == 0:
        return

    # QUOTE_ALLのヘッダ文字列（厳密一致）をまず確認
    first = _read_first_line(path)
    expected = ",".join([f"\"{c}\"" for c in EXPECTED_COLS])
    if first != expected:
        # 厳密一致しない場合でも、CSVとして読んで列名一致ならOKにする
        try:
            dfh = pd.read_csv(path, encoding=CSV_ENCODING, nrows=1)
            if list(dfh.columns) != EXPECTED_COLS:
                _quarantine(path, "header_mismatch")
        except Exception as e:
            _quarantine(path, f"read_fail:{type(e).__name__}")
        return

    # 本文が壊れていないか最小コストで確認
    try:
        pd.read_csv(path, encoding=CSV_ENCODING)
    except Exception as e:
        _quarantine(path, f"parse_fail:{type(e).__name__}")

def _sleep_with_jitter(sec: float) -> None:
    time.sleep(sec + random.uniform(0.0, JITTER_MAX))

def _append_csv_row(path: str, row: dict, columns: List[str]) -> None:
    df = pd.DataFrame([row], columns=columns)
    header = (not os.path.exists(path)) or os.path.getsize(path) == 0
    df.to_csv(
        path,
        mode="a",
        index=False,
        header=header,
        encoding=CSV_ENCODING,
        quoting=CSV_QUOTING,
        lineterminator=CSV_LINETERMINATOR,
    )

def _write_retry_trial(run_id: str, attempt: int, ok: int, fail: int, err: str, sleep_sec: float, symbols: List[str]) -> None:
    row = {
        "run_id": run_id,
        "timestamp_jst": now_jst_str(),
        "attempt": attempt,
        "symbols": " ".join(symbols),
        "ok_count": ok,
        "fail_count": fail,
        "error": err,
        "sleep_sec": round(sleep_sec, 3),
    }
    cols = ["run_id", "timestamp_jst", "attempt", "symbols", "ok_count", "fail_count", "error", "sleep_sec"]
    _append_csv_row(RETRY_TRIALS_CSV, row, cols)

def _probe_yahoo_http(run_id: str, symbols: List[str]) -> None:
    """
    Yahoo Financeの quote API に対して、この実行環境からのHTTP応答を “証拠” として残す。
    """
    url = "https://query1.finance.yahoo.com/v7/finance/quote"
    params = {"symbols": ",".join(symbols)}
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json,text/plain,*/*",
    }

    rec = {
        "run_id": run_id,
        "ts_utc": utc_iso(),
        "url": url,
        "symbols": symbols,
    }

    try:
        r = requests.get(url, params=params, headers=headers, timeout=20)
        rec["status_code"] = r.status_code
        rec["content_type"] = r.headers.get("Content-Type", "")
        rec["content_length"] = r.headers.get("Content-Length", "")
        rec["cache_control"] = r.headers.get("Cache-Control", "")
        rec["server"] = r.headers.get("Server", "")
        rec["final_url"] = str(r.url)
        rec["body_head"] = r.text[:200]  # 巨大化防止
    except Exception as e:
        rec["error"] = f"{type(e).__name__}: {e}"

    with open(YAHOO_HTTP_PROBE_JSONL, "a", encoding="utf-8") as f:
        f.write(json.dumps(rec, ensure_ascii=False) + "\n")

def _extract_last_close(df: pd.DataFrame, ticker: str) -> Tuple[float | None, str]:
    """
    yfinance download結果から ticker の終値（Close）を抜く。
    取れなければ (None, reason)。
    """
    try:
        if df is None or df.empty:
            return None, "EmptyDF"

        if isinstance(df.columns, pd.MultiIndex):
            if ("Close", ticker) in df.columns:
                s = df[("Close", ticker)]
            elif (ticker, "Close") in df.columns:
                s = df[(ticker, "Close")]
            else:
                return None, "CloseNotFoundForTicker"
        else:
            if "Close" in df.columns:
                s = df["Close"]
            else:
                return None, "CloseMissing"

        s = pd.to_numeric(s, errors="coerce").dropna()
        if s.empty:
            return None, "NoNumericClose"

        v = float(s.iloc[-1])
        if not (v > 0):
            return None, "NonPositive"
        return v, ""
    except Exception as e:
        return None, f"ExtractErr:{type(e).__name__}"

def _yf_download_multi(tickers: List[str]) -> Tuple[pd.DataFrame | None, str]:
    """
    1回のdownloadでまとめて取得（リクエスト数を減らす）。
    """
    try:
        df = yf.download(
            tickers=tickers,
            period=YF_PERIOD,
            interval=YF_INTERVAL,
            group_by="column",
            threads=False,
            auto_adjust=False,
            progress=False,
        )
        return df, ""
    except Exception as e:
        return None, f"{type(e).__name__}: {e}"

@dataclass
class AssetResult:
    price: float
    missing: int
    source: str
    date: str
    fail: str

def collect() -> int:
    run_id = make_run_id()

    print("=== yfinance market fetch ===")
    print(f"run_id      : {run_id}")
    print(f"timestamp_jst: {now_jst_str()}")

    # CSVの安全装置（列ズレ/破損なら隔離）
    _ensure_csv_header_or_quarantine(OUT_CSV)

    tickers = list(ASSETS.values())

    # 証拠取り：Yahooのquote APIへのHTTP応答を1回ログ
    _probe_yahoo_http(run_id, tickers)

    results: Dict[str, AssetResult] = {k: AssetResult(0.0, 1, "yfinance", "", "NoResult") for k in ASSETS.keys()}

    last_err = ""
    for attempt in range(1, MAX_RETRIES + 1):
        df, err = _yf_download_multi(tickers)
        last_err = err

        ok = 0
        fail = 0

        for name, ticker in ASSETS.items():
            v, why = _extract_last_close(df, ticker) if df is not None else (None, err or "DownloadFailed")
            if v is None:
                results[name] = AssetResult(0.0, 1, "yfinance", "", why or "Unknown")
                fail += 1
            else:
                date_str = ""
                try:
                    date_str = str(df.index[-1])[:10] if (df is not None and not df.empty) else ""
                except Exception:
                    date_str = ""
                results[name] = AssetResult(float(v), 0, "yfinance", date_str, "")
                ok += 1

        sleep_sec = 0.0 if ok == len(ASSETS) else (BASE_SLEEP * (2 ** (attempt - 1)))
        _write_retry_trial(run_id, attempt, ok, fail, last_err, sleep_sec, tickers)

        if ok == len(ASSETS):
            break
        if attempt == MAX_RETRIES:
            break

        _sleep_with_jitter(sleep_sec)

    # 32列固定で追記（列順強制）
    row: Dict[str, object] = {"run_id": run_id, "timestamp_jst": now_jst_str()}
    for a in ASSETS.keys():
        r = results.get(a, AssetResult(0.0, 1, "missing", "", "NoResult"))
        row[a] = float(r.price)
        row[f"{a}_missing"] = int(r.missing)
        row[f"{a}_source"] = str(r.source)
        row[f"{a}_date"] = str(r.date)
        row[f"{a}_fail"] = str(r.fail)

    _append_csv_row(OUT_CSV, row, EXPECTED_COLS)

    # 表示（人間が見る用）
    for name, ticker in ASSETS.items():
        r = results[name]
        mark = "✅" if r.missing == 0 else "❌"
        date_disp = r.date if r.date else ""
        print(f"[yfinance] {name}({ticker}): {r.price} ({date_disp}) {mark} fail={r.fail}")

    print(f"=== saved -> {OUT_CSV} ===")
    print(f"=== saved -> {RETRY_TRIALS_CSV} (append) ===")
    print(f"=== saved -> {YAHOO_HTTP_PROBE_JSONL} (append) ===")

    # collectorは落とさない（監視が落とす設計）
    return 0

if __name__ == "__main__":
    raise SystemExit(collect())
