# save as: market_yfinance_collector.py
from __future__ import annotations

import csv
import json
import os
import random
import shutil
import time
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

# yfinance settings
YF_PERIOD = "5d"
YF_INTERVAL = "1d"
MAX_RETRIES = 4
BASE_SLEEP = 15.0  # seconds (exponential backoff)

# =========================
# Helpers
# =========================
def now_jst() -> datetime:
    return datetime.now(JST)

def now_jst_str() -> str:
    return now_jst().strftime("%Y-%m-%d %H:%M:%S")

def _ts_suffix() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

def _get_run_id() -> str:
    # Prefer env (GitHub Actions) but work locally too.
    env = os.environ
    # Example: 20260116T090000Z_1234567890_attempts
    base = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    gha = env.get("GITHUB_RUN_ID", "")
    gha_attempt = env.get("GITHUB_RUN_ATTEMPT", "")
    if gha:
        return f"{base}_gha{gha}_a{gha_attempt or '1'}"
    return f"{base}_local"

def _expected_columns_market() -> List[str]:
    # run_id + timestamp_jst + (6 assets * 5 fields) = 1 + 1 + 30 = 32 columns
    cols = ["run_id", "timestamp_jst"]
    for a in ASSETS.keys():
        cols += [a, f"{a}_missing", f"{a}_source", f"{a}_date", f"{a}_fail"]
    return cols

EXPECTED_MARKET_COLS = _expected_columns_market()

def _read_first_line(path: str) -> str:
    with open(path, "r", encoding=CSV_ENCODING, errors="ignore") as f:
        return f.readline().strip("\r\n")

def _quarantine(path: str, reason: str) -> str:
    bad = f"{os.path.splitext(path)[0]}.bad_{_ts_suffix()}.csv"
    shutil.move(path, bad)
    print(f"[quarantine] {path} -> {bad}  reason={reason}")
    return bad

def _ensure_market_csv_ok_or_quarantine(path: str) -> None:
    """
    - 既存CSVのヘッダが想定と違う / パース不能なら隔離して新規にする
    - これで monitor 側の ParserError（列ズレ）を強制的に潰す
    """
    if not os.path.exists(path):
        return
    if os.path.getsize(path) == 0:
        return

    # QUOTE_ALL で書いている想定のヘッダ文字列
    expected_quoted = ",".join([f"\"{c}\"" for c in EXPECTED_MARKET_COLS])
    first = _read_first_line(path)

    if first != expected_quoted:
        # QUOTE_ALLでない可能性や、列順が違う可能性があるので pandas で確認
        try:
            dfh = pd.read_csv(path, encoding=CSV_ENCODING, nrows=1, on_bad_lines="skip")
            if list(dfh.columns) != EXPECTED_MARKET_COLS:
                _quarantine(path, "header_mismatch")
        except Exception as e:
            _quarantine(path, f"read_fail:{type(e).__name__}")
        return

    # 本文が壊れている可能性（ParserError）を検出
    try:
        pd.read_csv(path, encoding=CSV_ENCODING, on_bad_lines="error")
    except Exception as e:
        _quarantine(path, f"parse_fail:{type(e).__name__}")

def _sleep_with_jitter(sec: float) -> None:
    time.sleep(sec + random.uniform(0.0, 2.0))

def _append_retry_trial(run_id: str, attempt: int, ok: int, fail: int, err: str, sleep_next: float, symbols: List[str]) -> None:
    row = {
        "run_id": run_id,
        "timestamp_jst": now_jst_str(),
        "attempt": attempt,
        "symbols": " ".join(symbols),
        "ok_count": ok,
        "fail_count": fail,
        "error": err,
        "sleep_next_sec": round(float(sleep_next), 3),
    }
    df = pd.DataFrame([row])
    header = (not os.path.exists(RETRY_TRIALS_CSV)) or os.path.getsize(RETRY_TRIALS_CSV) == 0
    df.to_csv(
        RETRY_TRIALS_CSV,
        mode="a",
        index=False,
        header=header,
        encoding=CSV_ENCODING,
        quoting=CSV_QUOTING,
        lineterminator=CSV_LINETERMINATOR,
    )

def _append_http_probe(run_id: str, attempt: int, symbols: List[str], sleep_next: float) -> None:
    """
    「どのHTTP応答だったか」を証拠として残す。
    - yfinance内部の全通信は捕捉できないが、少なくともこの環境からの “Yahoo quote endpoint” の返りは記録できる。
    """
    url = "https://query1.finance.yahoo.com/v7/finance/quote"
    params = {"symbols": ",".join(symbols)}
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json,text/plain,*/*",
    }

    rec = {
        "run_id": run_id,
        "attempt": attempt,
        "ts_utc": datetime.now(timezone.utc).isoformat(),
        "sleep_next_sec": round(float(sleep_next), 3),
        "url": url,
        "symbols": symbols,
    }

    try:
        r = requests.get(url, params=params, headers=headers, timeout=20)
        rec["status_code"] = r.status_code
        rec["content_type"] = r.headers.get("Content-Type", "")
        rec["content_length"] = r.headers.get("Content-Length", "")
        rec["cache_control"] = r.headers.get("Cache-Control", "")
        rec["body_head"] = r.text[:200]
    except Exception as e:
        rec["error"] = f"{type(e).__name__}: {e}"

    with open(YAHOO_HTTP_PROBE_JSONL, "a", encoding="utf-8") as f:
        f.write(json.dumps(rec, ensure_ascii=False) + "\n")

def _yf_download_multi(tickers: List[str]) -> Tuple[pd.DataFrame | None, str]:
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

def _extract_last_close(df: pd.DataFrame, ticker: str) -> Tuple[float | None, str]:
    """
    戻り:
      (price, reason)
      price=None のとき reason が失敗理由
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

@dataclass
class AssetResult:
    price: float
    missing: int
    source: str
    date: str
    fail: str

def collect() -> int:
    run_id = _get_run_id()
    print("=== yfinance market fetch ===")
    print(f"run_id       : {run_id}")
    print(f"timestamp_jst: {now_jst_str()}")

    # 既存CSVの列ズレ/破損を隔離して作り直す
    _ensure_market_csv_ok_or_quarantine(OUT_CSV)

    tickers = list(ASSETS.values())
    results: Dict[str, AssetResult] = {}
    last_err = ""

    for attempt in range(1, MAX_RETRIES + 1):
        # 次に待つ時間（失敗時のみ使う）。ログに残して追跡できるように「先に計算」
        sleep_next = 0.0 if attempt == MAX_RETRIES else (BASE_SLEEP * (2 ** (attempt - 1)))

        # 失敗回でも追えるように：試行ごとにHTTP応答を記録
        _append_http_probe(run_id, attempt, tickers, sleep_next)

        df, err = _yf_download_multi(tickers)
        last_err = err

        ok = 0
        fail = 0

        for name, ticker in ASSETS.items():
            if df is None:
                v, why = None, err or "DownloadFailed"
            else:
                v, why = _extract_last_close(df, ticker)

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

        # 試行ログ（何回目で/何秒待つ予定で/何が起きたか）
        _append_retry_trial(run_id, attempt, ok, fail, last_err or "", sleep_next if ok != len(ASSETS) else 0.0, tickers)

        if ok == len(ASSETS):
            break  # 成功

        if attempt == MAX_RETRIES:
            break  # もう打ち止め

        _sleep_with_jitter(sleep_next)

    # Market log: 列順固定で必ず追記（run_idでリンク）
    row: Dict[str, object] = {"run_id": run_id, "timestamp_jst": now_jst_str()}
    for a in ASSETS.keys():
        r = results.get(a, AssetResult(0.0, 1, "missing", "", "NoResult"))
        row[a] = float(r.price)
        row[f"{a}_missing"] = int(r.missing)
        row[f"{a}_source"] = str(r.source)
        row[f"{a}_date"] = str(r.date)
        row[f"{a}_fail"] = str(r.fail)

    out_df = pd.DataFrame([row], columns=EXPECTED_MARKET_COLS)
    header = (not os.path.exists(OUT_CSV)) or os.path.getsize(OUT_CSV) == 0

    out_df.to_csv(
        OUT_CSV,
        mode="a",
        index=False,
        header=header,
        encoding=CSV_ENCODING,
        quoting=CSV_QUOTING,
        lineterminator=CSV_LINETERMINATOR,
    )

    # 人間用ログ
    for name, ticker in ASSETS.items():
        r = results[name]
        mark = "✅" if r.missing == 0 else "❌"
        date_disp = r.date if r.date else ""
        print(f"[yfinance] {name}({ticker}): {r.price} ({date_disp}) {mark} fail={r.fail}")

    print(f"=== saved -> {OUT_CSV} ===")
    print(f"=== linked logs -> {RETRY_TRIALS_CSV}, {YAHOO_HTTP_PROBE_JSONL} ===")

    # collectorは落とさない（監視は monitor が担当）
    return 0

if __name__ == "__main__":
    raise SystemExit(collect())
