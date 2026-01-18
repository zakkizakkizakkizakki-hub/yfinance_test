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
from typing import Dict, List, Optional, Tuple

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
CSV_LINETERMINATOR = "\n"  # pandasは lineterminator

ASSETS: Dict[str, str] = {
    "USDJPY": "JPY=X",
    "BTC": "BTC-USD",
    "Gold": "GC=F",
    "US10Y": "^TNX",
    "Oil": "CL=F",
    "VIX": "^VIX",
}

YF_PERIOD = "5d"
YF_INTERVAL = "1d"

MAX_RETRIES = 4
BASE_SLEEP = 15.0  # seconds (exponential backoff base)
HTTP_TIMEOUT = 20

# =========================
# Time / IDs
# =========================
def now_utc() -> datetime:
    return datetime.now(timezone.utc)

def now_jst() -> datetime:
    return datetime.now(JST)

def now_jst_str() -> str:
    return now_jst().strftime("%Y-%m-%d %H:%M:%S")

def new_run_id() -> str:
    # 例: 20260116_085259Z_123456
    return now_utc().strftime("%Y%m%d_%H%M%SZ") + f"_{random.randint(100000, 999999)}"

def _ts_suffix() -> str:
    return now_utc().strftime("%Y%m%d_%H%M%S")

# =========================
# CSV schema (固定)
# =========================
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
    - 既存CSVが壊れている/列が違う場合は隔離して作り直す（仕様固定）
    """
    if not os.path.exists(path):
        return
    if os.path.getsize(path) == 0:
        return

    # QUOTE_ALL前提のヘッダ文字列（厳格チェック）
    expected_header = ",".join([f"\"{c}\"" for c in EXPECTED_COLS])
    first = _read_first_line(path)

    if first != expected_header:
        # CSVとして読んで列名比較も試す（QUOTE_ALLでない可能性への保険）
        try:
            dfh = pd.read_csv(path, encoding=CSV_ENCODING, nrows=1, engine="python")
            if list(dfh.columns) != EXPECTED_COLS:
                _quarantine(path, "header_mismatch")
        except Exception as e:
            _quarantine(path, f"read_fail:{type(e).__name__}")
        return

    # 本文パースも確認（ParserError等なら隔離）
    try:
        pd.read_csv(path, encoding=CSV_ENCODING, engine="python")
    except Exception as e:
        _quarantine(path, f"parse_fail:{type(e).__name__}")

# =========================
# Logging helpers
# =========================
def _sleep_with_jitter(sec: float) -> None:
    # 0〜2秒の揺らぎを足す（ログにはsecをそのまま残す）
    time.sleep(max(0.0, sec) + random.uniform(0.0, 2.0))

def _append_retry_trial(
    run_id: str,
    attempt: int,
    ok_count: int,
    fail_count: int,
    err: str,
    sleep_sec: float,
    tickers: List[str],
) -> None:
    row = {
        "run_id": run_id,
        "timestamp_jst": now_jst_str(),
        "attempt": int(attempt),
        "symbols": " ".join(tickers),
        "ok_count": int(ok_count),
        "fail_count": int(fail_count),
        "error": str(err or ""),
        "sleep_sec": float(round(sleep_sec, 3)),
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

def _append_http_probe(
    run_id: str,
    phase: str,
    attempt: int,
    url: str,
    params: dict,
    headers: dict,
    status_code: Optional[int],
    resp_headers: dict,
    body_head: str,
    error: str = "",
) -> None:
    rec = {
        "run_id": run_id,
        "ts_utc": now_utc().isoformat(),
        "timestamp_jst": now_jst_str(),
        "phase": phase,          # e.g. "pre_download" / "after_fail"
        "attempt": int(attempt),
        "url": url,
        "params": params,
        "request_headers": {"User-Agent": headers.get("User-Agent", ""), "Accept": headers.get("Accept", "")},
        "status_code": status_code,
        "content_type": resp_headers.get("Content-Type", ""),
        "content_length": resp_headers.get("Content-Length", ""),
        "cache_control": resp_headers.get("Cache-Control", ""),
        "server": resp_headers.get("Server", ""),
        "set_cookie": resp_headers.get("Set-Cookie", ""),
        "body_head": body_head[:200],
        "error": error,
    }
    with open(YAHOO_HTTP_PROBE_JSONL, "a", encoding="utf-8") as f:
        f.write(json.dumps(rec, ensure_ascii=False) + "\n")

def _probe_yahoo_quote_api(run_id: str, phase: str, attempt: int, tickers: List[str]) -> None:
    """
    Yahooのquote APIへのHTTP応答を「証拠」として残す。
    """
    url = "https://query1.finance.yahoo.com/v7/finance/quote"
    params = {"symbols": ",".join(tickers)}
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json,text/plain,*/*",
    }
    try:
        r = requests.get(url, params=params, headers=headers, timeout=HTTP_TIMEOUT)
        _append_http_probe(
            run_id=run_id,
            phase=phase,
            attempt=attempt,
            url=url,
            params=params,
            headers=headers,
            status_code=r.status_code,
            resp_headers=dict(r.headers),
            body_head=(r.text or "")[:200],
        )
    except Exception as e:
        _append_http_probe(
            run_id=run_id,
            phase=phase,
            attempt=attempt,
            url=url,
            params=params,
            headers=headers,
            status_code=None,
            resp_headers={},
            body_head="",
            error=f"{type(e).__name__}: {e}",
        )

# =========================
# yfinance extraction
# =========================
def _yf_download_multi(tickers: List[str]) -> Tuple[Optional[pd.DataFrame], str]:
    """
    まとめて1回のdownloadにする（リクエスト数を減らす）。
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
    dfからtickerのCloseを抜く。取れなければ (None, reason)
    """
    try:
        if df is None or df.empty:
            return None, "EmptyDF"

        # MultiIndex のときが多い
        if isinstance(df.columns, pd.MultiIndex):
            if ("Close", ticker) in df.columns:
                s = df[("Close", ticker)]
            elif (ticker, "Close") in df.columns:
                s = df[(ticker, "Close")]
            else:
                return None, "CloseNotFoundForTicker"
        else:
            # 単一tickerの場合
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

# =========================
# Main
# =========================
def collect() -> int:
    run_id = new_run_id()

    print("=== yfinance market fetch ===")
    print(f"run_id       : {run_id}")
    print(f"timestamp_jst: {now_jst_str()}")

    # CSV安全装置（列ズレ/破損なら隔離）
    _ensure_csv_header_or_quarantine(OUT_CSV)

    tickers = list(ASSETS.values())

    results: Dict[str, AssetResult] = {}
    last_df: Optional[pd.DataFrame] = None
    last_err: str = ""

    for attempt in range(1, MAX_RETRIES + 1):
        # 毎回、証拠としてHTTP応答を記録（pre_download）
        _probe_yahoo_quote_api(run_id, phase="pre_download", attempt=attempt, tickers=tickers)

        df, err = _yf_download_multi(tickers)
        last_df, last_err = df, err

        ok = 0
        fail = 0

        for name, ticker in ASSETS.items():
            if df is None:
                results[name] = AssetResult(0.0, 1, "yfinance", "", err or "DownloadFailed")
                fail += 1
                continue

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

        # 次のsleepを計算（成功なら0）
        all_ok = (ok == len(ASSETS))
        sleep_sec = 0.0 if all_ok else (BASE_SLEEP * (2 ** (attempt - 1)))

        # retry_trialsに必ず記録（sleep_sec含む）
        _append_retry_trial(
            run_id=run_id,
            attempt=attempt,
            ok_count=ok,
            fail_count=fail,
            err=last_err,
            sleep_sec=sleep_sec,
            tickers=tickers,
        )

        if all_ok:
            break

        if attempt < MAX_RETRIES:
            # 失敗した直後にもHTTP証拠を残す（after_fail）
            _probe_yahoo_quote_api(run_id, phase="after_fail", attempt=attempt, tickers=tickers)
            _sleep_with_jitter(sleep_sec)

    # 仕様固定で1行追記（列順はEXPECTED_COLSで強制）
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

    # 表示（目視用）
    for name, ticker in ASSETS.items():
        r = results[name]
        mark = "✅" if r.missing == 0 else "❌"
        date_disp = r.date if r.date else ""
        print(f"[yfinance] {name}({ticker}): {r.price} ({date_disp}) {mark} fail={r.fail}")

    print(f"=== saved -> {OUT_CSV} ===")
    print(f"=== saved -> {RETRY_TRIALS_CSV} ===")
    print(f"=== saved -> {YAHOO_HTTP_PROBE_JSONL} ===")

    # collector自体は落とさない（監視が落とす設計）
    return 0

if __name__ == "__main__":
    raise SystemExit(collect())
