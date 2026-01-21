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

CSV_ENCODING = "utf-8-sig"
CSV_QUOTING = csv.QUOTE_ALL
CSV_LINETERMINATOR = "\n"  # pandasは lineterminator

OUT_CSV = "market_yfinance_log.csv"
RETRY_TRIALS_CSV = "retry_trials.csv"
YAHOO_HTTP_PROBE_JSONL = "yahoo_http_probe.jsonl"

# 取得対象（あなたの要件）
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
MAX_RETRIES = 4
BASE_SLEEP = 15.0  # seconds（失敗時の基礎待ち時間）
JITTER_MAX = 2.0   # seconds（微小ゆらぎ）

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
    GitHub Actions なら run_id/run_attempt を含めて一意化。
    ローカル実行でも一意になるようにUUIDも付与。
    """
    gh_run_id = os.getenv("GITHUB_RUN_ID", "")
    gh_attempt = os.getenv("GITHUB_RUN_ATTEMPT", "")
    base = f"gha-{gh_run_id}-{gh_attempt}" if gh_run_id else "local"
    return f"{base}-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{uuid.uuid4().hex[:8]}"

def _expected_columns_with_runid() -> List[str]:
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

EXPECTED_COLS = _expected_columns_with_runid()

def _read_first_line(path: str) -> str:
    with open(path, "r", encoding=CSV_ENCODING, errors="ignore") as f:
        return f.readline().strip("\r\n")

def _quarantine(path: str, reason: str) -> str:
    bad = f"{os.path.splitext(path)[0]}.bad_{_ts_suffix()}.csv"
    shutil.move(path, bad)
    print(f"[quarantine] {path} -> {bad}  reason={reason}")
    return bad

def _ensure_csv_ok_or_quarantine(path: str) -> None:
    """
    - 既存CSVが壊れている/列が合わない → 隔離して作り直し
    - 旧フォーマット（run_id無し）のCSVでも、壊れてなければ隔離しない
      ※ただし追記は新フォーマット(run_id有り)になるので、ここで隔離して揃える
        （列ズレ事故を防ぐため）
    """
    if not os.path.exists(path) or os.path.getsize(path) == 0:
        return

    # まずパースできるか
    try:
        df = pd.read_csv(path, encoding=CSV_ENCODING)
    except Exception as e:
        _quarantine(path, f"parse_fail:{type(e).__name__}")
        return

    cols = list(df.columns)

    # run_id ありの新仕様に統一したいので、旧仕様(31列)なら隔離して新規作成
    # 旧仕様: ["timestamp_jst", ... 30列 ...]
    if cols == EXPECTED_COLS:
        return

    if cols and cols[0] == "timestamp_jst":
        _quarantine(path, "old_schema_no_run_id")
        return

    # それ以外（列数違い等）は隔離
    _quarantine(path, "schema_mismatch")

def _sleep_with_jitter(sec: float) -> float:
    s = sec + random.uniform(0.0, JITTER_MAX)
    time.sleep(s)
    return s

def _append_csv(path: str, df: pd.DataFrame) -> None:
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

def _probe_yahoo_http(run_id: str, attempt: int, symbols: List[str]) -> dict:
    """
    Yahoo側のquote APIに対する「HTTP応答」を証拠として残す。
    - ここで取れるのは “この環境から見た Yahooの応答” という事実
    - yfinance内部の全通信を完全に捕捉するものではない
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
        "ts_utc": utc_iso(),
        "url": url,
        "symbols": symbols,
    }

    try:
        t0 = time.time()
        r = requests.get(url, params=params, headers=headers, timeout=20)
        rec["elapsed_ms"] = int((time.time() - t0) * 1000)
        rec["status_code"] = r.status_code
        rec["content_type"] = r.headers.get("Content-Type", "")
        rec["content_length"] = r.headers.get("Content-Length", "")
        # ヘッダ全保存は肥大化するので主要だけ
        rec["cache_control"] = r.headers.get("Cache-Control", "")
        rec["set_cookie"] = "present" if ("Set-Cookie" in r.headers) else "absent"
        rec["body_head"] = r.text[:200]
    except Exception as e:
        rec["error"] = f"{type(e).__name__}: {e}"

    with open(YAHOO_HTTP_PROBE_JSONL, "a", encoding="utf-8") as f:
        f.write(json.dumps(rec, ensure_ascii=False) + "\n")

    return rec

def _yf_download_multi(tickers: List[str]) -> Tuple[pd.DataFrame | None, str]:
    """
    まとめて1回で取得（リクエスト数を減らす）
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

def _extract_last_close(df: pd.DataFrame, ticker: str) -> Tuple[float | None, str]:
    """
    yfinance download結果から ticker の終値（Close）を抜く。
    取れなければ (None, reason)。
    """
    try:
        if df is None or df.empty:
            return None, "EmptyDF"

        if isinstance(df.columns, pd.MultiIndex):
            # ('Close','JPY=X') 形式を優先
            if ("Close", ticker) in df.columns:
                s = df[("Close", ticker)]
            else:
                return None, "CloseNotFoundForTicker"
        else:
            if "Close" not in df.columns:
                return None, "CloseMissing"
            s = df["Close"]

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
    run_id = make_run_id()
    print("=== yfinance market fetch ===")
    print(f"run_id      : {run_id}")
    print(f"timestamp_jst: {now_jst_str()}")

    # 既存CSVが壊れてる/旧仕様 → 隔離して事故防止
    _ensure_csv_ok_or_quarantine(OUT_CSV)

    tickers = list(ASSETS.values())

    results: Dict[str, AssetResult] = {}
    last_probe: dict | None = None
    last_yf_err = ""

    # 試行ごとの「待ち時間(予定/実測)」を記録する
    planned_sleep = 0.0
    actual_sleep = 0.0

    for attempt in range(1, MAX_RETRIES + 1):
        # 証拠取り：毎回ログ（attemptごとにHTTP応答が違う可能性があるため）
        last_probe = _probe_yahoo_http(run_id, attempt, tickers)

        df, yf_err = _yf_download_multi(tickers)
        last_yf_err = yf_err or ""

        ok = 0
        fail = 0

        for name, ticker in ASSETS.items():
            if df is None:
                v, why = None, (yf_err or "DownloadFailed")
            else:
                v, why = _extract_last_close(df, ticker)

            if v is None:
                results[name] = AssetResult(0.0, 1, "yfinance", "", why or "Unknown")
                fail += 1
            else:
                # 日付は足の最終日
                date_str = ""
                try:
                    date_str = str(df.index[-1])[:10] if (df is not None and not df.empty) else ""
                except Exception:
                    date_str = ""
                results[name] = AssetResult(float(v), 0, "yfinance", date_str, "")
                ok += 1

        # このattemptの記録（retry_trials.csv）
        # 失敗している場合のみ次回までのsleepを計算
        if ok == len(ASSETS):
            planned_sleep = 0.0
        else:
            planned_sleep = BASE_SLEEP * (2 ** (attempt - 1))

        retry_row = {
            "run_id": run_id,
            "timestamp_jst": now_jst_str(),
            "attempt": attempt,
            "symbols": " ".join(tickers),
            "ok_count": ok,
            "fail_count": fail,
            "yfinance_error": (last_yf_err[:200] if last_yf_err else ""),
            "planned_sleep_sec": round(float(planned_sleep), 3),
            "actual_sleep_sec": round(float(actual_sleep), 3),  # 前回sleepの実測
            "probe_status_code": last_probe.get("status_code", "") if isinstance(last_probe, dict) else "",
            "probe_content_type": last_probe.get("content_type", "") if isinstance(last_probe, dict) else "",
            "probe_content_length": last_probe.get("content_length", "") if isinstance(last_probe, dict) else "",
        }
        _append_csv(RETRY_TRIALS_CSV, pd.DataFrame([retry_row]))

        if ok == len(ASSETS):
            break

        if attempt == MAX_RETRIES:
            break

        # 次の試行まで待つ（実測を次の行に載せる）
        actual_sleep = _sleep_with_jitter(planned_sleep)

    # market_yfinance_log.csv（1run=1行）
    row: Dict[str, object] = {
        "run_id": run_id,
        "timestamp_jst": now_jst_str(),
    }
    for a in ASSETS.keys():
        r = results.get(a, AssetResult(0.0, 1, "missing", "", "NoResult"))
        row[a] = float(r.price)
        row[f"{a}_missing"] = int(r.missing)
        row[f"{a}_source"] = str(r.source)
        row[f"{a}_date"] = str(r.date)
        row[f"{a}_fail"] = str(r.fail)

    out_df = pd.DataFrame([row], columns=EXPECTED_COLS)
    _append_csv(OUT_CSV, out_df)

    # 表示（人間が見る用）
    for name, ticker in ASSETS.items():
        r = results.get(name, AssetResult(0.0, 1, "missing", "", "NoResult"))
        mark = "✅" if r.missing == 0 else "❌"
        print(f"[yfinance] {name}({ticker}): {r.price} ({r.date}) {mark} fail={r.fail}")

    print(f"=== saved -> {OUT_CSV} ===")
    print(f"=== saved -> {RETRY_TRIALS_CSV} ===")
    print(f"=== saved -> {YAHOO_HTTP_PROBE_JSONL} ===")

    # collectorは落とさない（監視はmonitorに集約）
    return 0

if __name__ == "__main__":
    raise SystemExit(collect())
