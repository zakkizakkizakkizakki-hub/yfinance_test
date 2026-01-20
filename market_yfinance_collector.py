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
from typing import Dict, List, Tuple, Optional

import pandas as pd
import requests
import yfinance as yf

# =========================
# Config
# =========================
JST = timezone(timedelta(hours=9))

OUT_CSV = "market_yfinance_log.csv"          # 31列固定（ヘッダ固定）
RETRY_TRIALS_CSV = "retry_trials.csv"        # リトライ履歴（run_id付き）
YAHOO_HTTP_PROBE_JSONL = "yahoo_http_probe.jsonl"  # HTTP証拠（run_id付き）

CSV_ENCODING = "utf-8-sig"
CSV_QUOTING = csv.QUOTE_ALL
CSV_LINETERMINATOR = "\n"  # pandasは lineterminator

ASSETS: Dict[str, str] = {
    "USDJPY": "JPY=X",
    "BTC": "BTC-USD",
    "Gold": "GC=F",
    "US10Y": "^TNX",
    "Oil": "CL=F",
    "VIX": "^VIX",
}

# yfinance settings
YF_PERIOD = os.getenv("YF_PERIOD", "5d")
YF_INTERVAL = os.getenv("YF_INTERVAL", "1d")
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "4"))

# 待ち時間（秒）: 例) 15 -> 15, 30, 60, 120...
BASE_SLEEP = float(os.getenv("BASE_SLEEP", "15"))
JITTER_MAX = float(os.getenv("JITTER_MAX", "2.0"))

HTTP_TIMEOUT = int(os.getenv("HTTP_TIMEOUT", "20"))

# =========================
# Helpers
# =========================
def now_jst() -> datetime:
    return datetime.now(JST)

def now_jst_str() -> str:
    return now_jst().strftime("%Y-%m-%d %H:%M:%S")

def _run_id() -> str:
    # 例: RID20260116T000102Z_1a2b3c4d
    utc = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    rnd = "".join(random.choice("0123456789abcdef") for _ in range(8))
    return f"RID{utc}_{rnd}"

def _ts_suffix() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

def _expected_columns() -> List[str]:
    cols = ["timestamp_jst"]
    for a in ASSETS.keys():
        cols += [
            a,
            f"{a}_missing",
            f"{a}_source",
            f"{a}_date",
            f"{a}_fail",
        ]
    # 1 + 6*5 = 31 columns
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
    market_yfinance_log.csv を「31列ヘッダ固定」で守る。
    - ヘッダ不一致 / パース不能なら隔離して新規作成へ
    """
    if not os.path.exists(path):
        return
    if os.path.getsize(path) == 0:
        return

    # QUOTE_ALL想定のヘッダ行
    expected_header = ",".join([f"\"{c}\"" for c in EXPECTED_COLS])

    try:
        first = _read_first_line(path)
        if first != expected_header:
            # csvとして読んで列名一致も試す
            dfh = pd.read_csv(path, encoding=CSV_ENCODING, nrows=1)
            if list(dfh.columns) != EXPECTED_COLS:
                _quarantine(path, "header_mismatch")
                return
        # 本文が壊れていないか軽く確認（失敗なら隔離）
        pd.read_csv(path, encoding=CSV_ENCODING)
    except Exception as e:
        _quarantine(path, f"parse_fail:{type(e).__name__}")

def _sleep_with_jitter(sec: float) -> float:
    jitter = random.uniform(0.0, JITTER_MAX)
    total = sec + jitter
    time.sleep(total)
    return total

def _append_retry_trial(
    run_id: str,
    attempt: int,
    ok_count: int,
    fail_count: int,
    last_err: str,
    planned_sleep: float,
    actual_sleep: float,
    symbols: List[str],
) -> None:
    row = {
        "run_id": run_id,
        "timestamp_jst": now_jst_str(),
        "attempt": attempt,
        "symbols": " ".join(symbols),
        "ok_count": ok_count,
        "fail_count": fail_count,
        "error": last_err,
        "planned_sleep_sec": round(planned_sleep, 3),
        "actual_sleep_sec": round(actual_sleep, 3),
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

def _append_yahoo_http_probe(run_id: str, symbols: List[str]) -> None:
    """
    Yahoo側のHTTP応答（ステータス/ヘッダ）を“証拠”として残す。
    """
    url = "https://query1.finance.yahoo.com/v7/finance/quote"
    params = {"symbols": ",".join(symbols)}
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json,text/plain,*/*",
    }
    rec = {
        "run_id": run_id,
        "ts_utc": datetime.now(timezone.utc).isoformat(),
        "url": url,
        "symbols": symbols,
    }
    try:
        r = requests.get(url, params=params, headers=headers, timeout=HTTP_TIMEOUT)
        rec["status_code"] = r.status_code
        rec["content_type"] = r.headers.get("Content-Type", "")
        rec["content_length"] = r.headers.get("Content-Length", "")
        rec["cache_control"] = r.headers.get("Cache-Control", "")
        rec["server"] = r.headers.get("Server", "")
        rec["body_head"] = (r.text or "")[:200]
    except Exception as e:
        rec["error"] = f"{type(e).__name__}: {e}"

    with open(YAHOO_HTTP_PROBE_JSONL, "a", encoding="utf-8") as f:
        f.write(json.dumps(rec, ensure_ascii=False) + "\n")

def _yf_download_multi(tickers: List[str]) -> Tuple[Optional[pd.DataFrame], str]:
    """
    まとめて1回のdownload（リクエスト数を抑える）
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
        return None, type(e).__name__

def _extract_last_close(df: pd.DataFrame, ticker: str) -> Tuple[Optional[float], str]:
    """
    yfinance download結果から ticker の Close を抜く。
    取れない場合は (None, reason)。
    """
    try:
        if df is None or df.empty:
            return None, "EmptyDF"

        if isinstance(df.columns, pd.MultiIndex):
            # 典型: ('Close','JPY=X')
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
    run_id = _run_id()
    tickers = list(ASSETS.values())

    print("=== yfinance market fetch ===")
    print(f"run_id       : {run_id}")
    print(f"timestamp_jst: {now_jst_str()}")
    print(f"tickers      : {', '.join(tickers)}")
    print(f"retries      : {MAX_RETRIES} (BASE_SLEEP={BASE_SLEEP}s, jitter<= {JITTER_MAX}s)")

    # CSVを守る（列ズレ/破損なら隔離）
    _ensure_csv_header_or_quarantine(OUT_CSV)

    # 証拠取り（YahooのHTTP応答）
    _append_yahoo_http_probe(run_id, tickers)

    results: Dict[str, AssetResult] = {}
    last_err = ""

    for attempt in range(1, MAX_RETRIES + 1):
        df, err = _yf_download_multi(tickers)
        last_err = err

        ok = 0
        fail = 0

        # 日付は df.index 最後（取れたときだけ）
        date_str = ""
        if df is not None and not df.empty:
            try:
                date_str = str(df.index[-1])[:10]
            except Exception:
                date_str = ""

        for name, ticker in ASSETS.items():
            v, why = (None, err or "DownloadFailed") if df is None else _extract_last_close(df, ticker)
            if v is None:
                results[name] = AssetResult(0.0, 1, "yfinance", "", why or "Unknown")
                fail += 1
            else:
                results[name] = AssetResult(float(v), 0, "yfinance", date_str, "")
                ok += 1

        # リトライログ
        planned_sleep = 0.0 if ok == len(ASSETS) else (BASE_SLEEP * (2 ** (attempt - 1)))
        actual_sleep = 0.0
        _append_retry_trial(run_id, attempt, ok, fail, last_err or "", planned_sleep, actual_sleep, tickers)

        # 全部成功なら終了
        if ok == len(ASSETS):
            break

        # 最終試行なら終了
        if attempt == MAX_RETRIES:
            break

        # 待って再試行（この待ち時間もログに残す）
        actual_sleep = _sleep_with_jitter(planned_sleep)
        _append_retry_trial(run_id, attempt, ok, fail, last_err or "", planned_sleep, actual_sleep, tickers)

    # 31列固定で追記（run_idは timestamp_jst に埋め込む：列数を壊さないため）
    # 例: 2026-01-16 08:52:53|RID2026..._abcd1234
    row: Dict[str, object] = {"timestamp_jst": f"{now_jst_str()}|{run_id}"}
    for a in ASSETS.keys():
        r = results.get(a, AssetResult(0.0, 1, "missing", "", "NoResult"))
        row[a] = float(r.price)
        row[f"{a}_missing"] = int(r.missing)
        row[f"{a}_source"] = str(r.source)
        row[f"{a}_date"] = str(r.date)
        row[f"{a}_fail"] = str(r.fail)

    out_df = pd.DataFrame([row], columns=EXPECTED_COLS)

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

    # 画面表示
    for name, ticker in ASSETS.items():
        r = results.get(name, AssetResult(0.0, 1, "missing", "", "NoResult"))
        mark = "✅" if r.missing == 0 else "❌"
        print(f"[{r.source}] {name}({ticker}): {r.price} ({r.date}) {mark} fail={r.fail}")

    print(f"=== saved -> {OUT_CSV} ===")
    print(f"=== retry log -> {RETRY_TRIALS_CSV} ===")
    print(f"=== http probe -> {YAHOO_HTTP_PROBE_JSONL} ===")

    # collectorは落とさない（監視で落とす設計）
    return 0

if __name__ == "__main__":
    raise SystemExit(collect())
