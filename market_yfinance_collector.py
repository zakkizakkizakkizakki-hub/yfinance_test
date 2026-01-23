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
UTC = timezone.utc

OUT_CSV = "market_yfinance_log.csv"
RETRY_TRIALS_CSV = "retry_trials.csv"
YAHOO_HTTP_PROBE_JSONL = "yahoo_http_probe.jsonl"

QUAR_DIR = ".quarantine"

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
YF_PERIOD = "5d"
YF_INTERVAL = "1d"
MAX_RETRIES = 4
BASE_SLEEP = 15.0  # seconds (backoffは 15,30,60... + jitter)

# =========================
# Helpers
# =========================
def now_jst() -> datetime:
    return datetime.now(JST)

def now_jst_str() -> str:
    return now_jst().strftime("%Y-%m-%d %H:%M:%S")

def utc_compact() -> str:
    return datetime.now(UTC).strftime("%Y%m%d_%H%M%S")

def make_run_id() -> str:
    # 例: 20260116_032455Z_493812
    return f"{utc_compact()}Z_{random.randint(100000, 999999)}"

def expected_columns() -> List[str]:
    # run_id + timestamp + (6 assets * 5 cols) = 1 + 1 + 30 = 32 columns
    cols = ["run_id", "timestamp_jst"]
    for a in ASSETS.keys():
        cols += [a, f"{a}_missing", f"{a}_source", f"{a}_date", f"{a}_fail"]
    return cols

EXPECTED_COLS = expected_columns()

def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)

def read_first_line(path: str) -> str:
    with open(path, "r", encoding=CSV_ENCODING, errors="ignore") as f:
        return f.readline().strip("\r\n")

def quarantine(path: str, reason: str) -> str:
    ensure_dir(QUAR_DIR)
    base = os.path.basename(path)
    bad = os.path.join(QUAR_DIR, f"{os.path.splitext(base)[0]}.bad_{utc_compact()}_{reason}.csv")
    shutil.move(path, bad)
    print(f"[quarantine] {path} -> {bad}  reason={reason}")
    return bad

def ensure_csv_header_or_quarantine(path: str) -> None:
    """
    既存CSVが:
    - ヘッダ不一致（列数/列名違い）
    - パース不能（途中の列ズレ等）
    の場合は隔離して新規作成に切り替える。
    """
    if not os.path.exists(path):
        return
    if os.path.getsize(path) == 0:
        return

    # QUOTE_ALLヘッダを想定
    expected_header = ",".join([f"\"{c}\"" for c in EXPECTED_COLS])
    first = read_first_line(path)

    if first != expected_header:
        # QUOTE_ALLでない/順序違いもあるので、CSVとして解釈して最終判定
        try:
            dfh = pd.read_csv(path, encoding=CSV_ENCODING, nrows=1)
            if list(dfh.columns) != EXPECTED_COLS:
                quarantine(path, "header_mismatch")
        except Exception as e:
            quarantine(path, f"read_fail_{type(e).__name__}")
        return

    # 本文が壊れていないかを確認
    try:
        pd.read_csv(path, encoding=CSV_ENCODING)
    except Exception as e:
        quarantine(path, f"parse_fail_{type(e).__name__}")

def sleep_with_jitter(sec: float) -> None:
    time.sleep(sec + random.uniform(0.0, 2.0))

def append_df_csv(path: str, df: pd.DataFrame) -> None:
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

def write_retry_trial(run_id: str, attempt: int, ok: int, fail: int, err: str, sleep_sec: float, symbols: List[str]) -> None:
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
    append_df_csv(RETRY_TRIALS_CSV, pd.DataFrame([row]))

def probe_yahoo_http(run_id: str, attempt: int, symbols: List[str]) -> None:
    """
    Yahooの quote API に対するHTTP応答を「証拠」として残す。
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
        "ts_utc": datetime.now(UTC).isoformat(),
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
        rec["body_head"] = r.text[:200]
    except Exception as e:
        rec["error"] = f"{type(e).__name__}: {e}"

    with open(YAHOO_HTTP_PROBE_JSONL, "a", encoding="utf-8") as f:
        f.write(json.dumps(rec, ensure_ascii=False) + "\n")

def yf_download_multi(tickers: List[str]) -> Tuple[pd.DataFrame | None, str]:
    """
    まとめて1回のdownloadで取得（リクエスト数削減）。
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

def extract_last_close(df: pd.DataFrame, ticker: str) -> Tuple[float | None, str]:
    """
    download結果から ticker の終値（Close）を抜く。
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
    run_id = make_run_id()
    print("=== yfinance market fetch ===")
    print(f"run_id       : {run_id}")
    print(f"timestamp_jst: {now_jst_str()}")

    # CSV安全装置（列ズレ/破損なら隔離）
    ensure_csv_header_or_quarantine(OUT_CSV)

    tickers = list(ASSETS.values())

    results: Dict[str, AssetResult] = {}
    last_err = ""

    for attempt in range(1, MAX_RETRIES + 1):
        # attemptごとに「Yahoo HTTP応答」を証拠保存
        probe_yahoo_http(run_id, attempt, tickers)

        df, err = yf_download_multi(tickers)
        last_err = err

        ok = 0
        fail = 0

        for name, ticker in ASSETS.items():
            v, why = extract_last_close(df, ticker) if df is not None else (None, err or "DownloadFailed")
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

        # 次の待ち時間（成功なら0）
        sleep_sec = 0.0 if ok == len(ASSETS) else (BASE_SLEEP * (2 ** (attempt - 1)))

        # retry_trials.csv（run_id付き）
        write_retry_trial(run_id, attempt, ok, fail, last_err or "", sleep_sec, tickers)

        if ok == len(ASSETS):
            break
        if attempt == MAX_RETRIES:
            break

        sleep_with_jitter(sleep_sec)

    # 32列固定で追記
    row: Dict[str, object] = {"run_id": run_id, "timestamp_jst": now_jst_str()}
    for a in ASSETS.keys():
        r = results.get(a, AssetResult(0.0, 1, "missing", "", "NoResult"))
        row[a] = float(r.price)
        row[f"{a}_missing"] = int(r.missing)
        row[f"{a}_source"] = str(r.source)
        row[f"{a}_date"] = str(r.date)
        row[f"{a}_fail"] = str(r.fail)

    out_df = pd.DataFrame([row], columns=EXPECTED_COLS)
    append_df_csv(OUT_CSV, out_df)

    # 表示（人間が見る用）
    for name, ticker in ASSETS.items():
        r = results.get(name, AssetResult(0.0, 1, "missing", "", "NoResult"))
        mark = "✅" if r.missing == 0 else "❌"
        date_disp = r.date if r.date else ""
        print(f"[yfinance] {name}({ticker}): {r.price} ({date_disp}) {mark} fail={r.fail}")

    print(f"=== saved -> {OUT_CSV} ===")
    print(f"=== probe -> {YAHOO_HTTP_PROBE_JSONL} ===")
    print(f"=== retry -> {RETRY_TRIALS_CSV} ===")

    # collectorは落とさない（監視はmonitorが落とす）
    return 0

if __name__ == "__main__":
    raise SystemExit(collect())
