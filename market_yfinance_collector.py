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

# yfinance 取得設定（※待ち時間は retry_trials.csv に必ず記録）
YF_PERIOD = "5d"
YF_INTERVAL = "1d"
MAX_RETRIES = 4
BASE_SLEEP = 15.0  # seconds (指数バックオフの基点)
JITTER_MAX = 2.0   # seconds

# =========================
# Helpers
# =========================
def now_jst() -> datetime:
    return datetime.now(JST)

def now_jst_str() -> str:
    return now_jst().strftime("%Y-%m-%d %H:%M:%S")

def _utc_ts() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

def _make_run_id() -> str:
    # 例: 20260116_123456_834217
    return f"{_utc_ts()}_{random.randint(100000, 999999)}"

def _expected_columns() -> List[str]:
    # run_id を追加（3ファイル紐づけの要）
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

def _ts_suffix() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

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
    - 既存CSVが「列仕様ズレ」or「パース不能」なら隔離して作り直す
    - 列順は EXPECTED_COLS に固定
    """
    if not os.path.exists(path) or os.path.getsize(path) == 0:
        return

    # QUOTE_ALL のヘッダ期待形
    expected_header = ",".join([f"\"{c}\"" for c in EXPECTED_COLS])
    first = _read_first_line(path)

    if first != expected_header:
        # QUOTE_ALLじゃない可能性もあるので、CSVとして読んで列名確認
        try:
            dfh = pd.read_csv(path, encoding=CSV_ENCODING, nrows=1)
            if list(dfh.columns) != EXPECTED_COLS:
                _quarantine(path, "header_mismatch")
        except Exception as e:
            _quarantine(path, f"read_fail:{type(e).__name__}")
        return

    # 本文が壊れている可能性の確認（ParserErrorなど）
    try:
        pd.read_csv(path, encoding=CSV_ENCODING)
    except Exception as e:
        _quarantine(path, f"parse_fail:{type(e).__name__}")

def _sleep_with_jitter(sec: float) -> None:
    time.sleep(sec + random.uniform(0.0, JITTER_MAX))

def _append_retry_trial(
    run_id: str,
    attempt: int,
    ok_count: int,
    fail_count: int,
    fail_reason: str,
    planned_sleep_sec: float,
    symbols: List[str],
) -> None:
    row = {
        "run_id": run_id,
        "timestamp_jst": now_jst_str(),
        "attempt": int(attempt),
        "symbols": " ".join(symbols),
        "ok_count": int(ok_count),
        "fail_count": int(fail_count),
        "fail_reason": str(fail_reason or ""),
        "planned_sleep_sec": float(round(planned_sleep_sec, 3)),
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
    Yahooの quote API の「HTTPステータス/ヘッダ」を証拠ログ化（jsonl）。
    """
    url = "https://query1.finance.yahoo.com/v7/finance/quote"
    params = {"symbols": ",".join(symbols)}
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json,text/plain,*/*",
        "Accept-Language": "en-US,en;q=0.9",
    }

    rec = {
        "run_id": run_id,
        "ts_utc": datetime.now(timezone.utc).isoformat(),
        "url": url,
        "symbols": symbols,
    }

    try:
        r = requests.get(url, params=params, headers=headers, timeout=20)
        rec["status_code"] = int(r.status_code)
        rec["content_type"] = r.headers.get("Content-Type", "")
        rec["content_length"] = r.headers.get("Content-Length", "")
        # body全部保存は巨大化するので先頭だけ
        rec["body_head"] = (r.text or "")[:300]
    except Exception as e:
        rec["error"] = f"{type(e).__name__}: {e}"

    with open(YAHOO_HTTP_PROBE_JSONL, "a", encoding="utf-8") as f:
        f.write(json.dumps(rec, ensure_ascii=False) + "\n")

def _yf_download_multi(tickers: List[str]) -> Tuple[Optional[pd.DataFrame], str]:
    """
    まとめて1回でdownload（リクエスト数を減らす）
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
    download結果から ticker の終値(Close)を抽出。
    取れなければ (None, reason)
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

def _safe_last_date(df: Optional[pd.DataFrame]) -> str:
    try:
        if df is None or df.empty:
            return ""
        return str(df.index[-1])[:10]
    except Exception:
        return ""

@dataclass
class AssetResult:
    price: float
    missing: int
    source: str
    date: str
    fail: str

def collect() -> int:
    run_id = _make_run_id()
    print("=== yfinance market fetch ===")
    print(f"run_id       : {run_id}")
    print(f"timestamp_jst: {now_jst_str()}")

    # CSVの安全装置（列ズレ/破損なら隔離）
    _ensure_csv_header_or_quarantine(OUT_CSV)

    tickers = list(ASSETS.values())

    # 証拠取り（このrunのHTTP応答）
    _append_yahoo_http_probe(run_id, tickers)

    results: Dict[str, AssetResult] = {}
    last_err = ""

    for attempt in range(1, MAX_RETRIES + 1):
        df, err = _yf_download_multi(tickers)
        last_err = err or ""

        ok = 0
        fail = 0

        for name, ticker in ASSETS.items():
            if df is None:
                v, why = None, (err or "DownloadFailed")
            else:
                v, why = _extract_last_close(df, ticker)

            if v is None:
                results[name] = AssetResult(0.0, 1, "yfinance", "", why or "Unknown")
                fail += 1
            else:
                results[name] = AssetResult(float(v), 0, "yfinance", _safe_last_date(df), "")
                ok += 1

        planned_sleep = 0.0 if ok == len(ASSETS) else (BASE_SLEEP * (2 ** (attempt - 1)))
        _append_retry_trial(
            run_id=run_id,
            attempt=attempt,
            ok_count=ok,
            fail_count=fail,
            fail_reason=(last_err or ("" if ok == len(ASSETS) else "PartialFail")),
            planned_sleep_sec=planned_sleep,
            symbols=tickers,
        )

        if ok == len(ASSETS):
            break
        if attempt == MAX_RETRIES:
            break

        _sleep_with_jitter(planned_sleep)

    # 32列固定で書き出し（列順強制）
    row: Dict[str, object] = {"run_id": run_id, "timestamp_jst": now_jst_str()}
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

    # 人間用ログ
    for name, ticker in ASSETS.items():
        r = results[name]
        mark = "✅" if r.missing == 0 else "❌"
        print(f"[yfinance] {name}({ticker}): {r.price} ({r.date}) {mark} fail={r.fail}")

    print(f"=== saved -> {OUT_CSV} / {RETRY_TRIALS_CSV} / {YAHOO_HTTP_PROBE_JSONL} ===")
    # collectorは落とさない（監視が落とす設計）
    return 0

if __name__ == "__main__":
    raise SystemExit(collect())
