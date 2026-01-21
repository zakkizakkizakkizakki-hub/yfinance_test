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
CSV_LINETERMINATOR = "\n"  # pandasは lineterminator

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
BASE_SLEEP = 15.0  # seconds (planned), exponential backoff
TIMEOUT_SEC = 20

# =========================
# run_id
# =========================
def make_run_id() -> str:
    # GitHub Actionsなら環境変数が入る。ローカルでも動くようにfallback。
    rid = os.getenv("GITHUB_RUN_ID", "local")
    ratt = os.getenv("GITHUB_RUN_ATTEMPT", "0")
    job = os.getenv("GITHUB_JOB", "job")
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    rnd = f"{random.randint(0, 9999):04d}"
    return f"{rid}.{ratt}.{job}.{ts}.{rnd}"

RUN_ID = make_run_id()

# =========================
# Helpers
# =========================
def now_jst() -> datetime:
    return datetime.now(JST)

def now_jst_str() -> str:
    return now_jst().strftime("%Y-%m-%d %H:%M:%S")

def _ts_suffix() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

def _expected_columns() -> List[str]:
    # run_id 追加（これで3ファイル連結が確実）
    cols = ["run_id", "timestamp_jst"]
    for a in ASSETS.keys():
        cols += [a, f"{a}_missing", f"{a}_source", f"{a}_date", f"{a}_fail"]
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
    - CSVが無ければ何もしない（後で追記時に作られる）
    - 既存CSVのヘッダが想定(32列)と違う / パース不能なら隔離して新規にする
    """
    if not os.path.exists(path):
        return
    if os.path.getsize(path) == 0:
        return

    first = _read_first_line(path)

    # QUOTE_ALLで書くのでヘッダは "col","col"... の形になる
    expected_quoted = ",".join([f"\"{c}\"" for c in EXPECTED_COLS])
    if first != expected_quoted:
        # QUOTE_ALLじゃない/古い31列の可能性があるので実際にパースして確認
        try:
            dfh = pd.read_csv(path, encoding=CSV_ENCODING, nrows=1)
            if list(dfh.columns) != EXPECTED_COLS:
                _quarantine(path, "header_mismatch")
        except Exception as e:
            _quarantine(path, f"read_fail:{type(e).__name__}")
        return

    # 本文が壊れていないか（ParserError等）を確認
    try:
        pd.read_csv(path, encoding=CSV_ENCODING)
    except Exception as e:
        _quarantine(path, f"parse_fail:{type(e).__name__}")

def _sleep_with_jitter(sec: float) -> float:
    # 実際に待った秒数を返す（ログ用）
    actual = sec + random.uniform(0.0, 2.0)
    time.sleep(actual)
    return actual

def _append_retry_trial(
    run_id: str,
    attempt_no: int,
    ok: int,
    fail: int,
    planned_sleep_sec: float,
    actual_sleep_sec: float,
    err_type: str,
    err_msg: str,
    symbols: List[str],
) -> None:
    row = {
        "run_id": run_id,
        "timestamp_jst": now_jst_str(),
        "attempt_no": attempt_no,
        "symbols": " ".join(symbols),
        "ok_count": ok,
        "fail_count": fail,
        "planned_sleep_sec": round(planned_sleep_sec, 3),
        "actual_sleep_sec": round(actual_sleep_sec, 3),
        "err_type": err_type,
        "err_msg": err_msg,
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

def _append_yahoo_http_probe(run_id: str, attempt_no: int, symbols: List[str]) -> None:
    """
    Yahooのquote APIに対するHTTP応答を“証拠”として残す。
    ここで残るのは「この環境でこのURLに投げたらこう返った」という事実だけ。
    """
    url = "https://query1.finance.yahoo.com/v7/finance/quote"
    params = {"symbols": ",".join(symbols)}
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json,text/plain,*/*",
    }

    rec = {
        "run_id": run_id,
        "attempt_no": attempt_no,
        "ts_utc": datetime.now(timezone.utc).isoformat(),
        "url": url,
        "symbols": symbols,
    }

    t0 = time.time()
    try:
        r = requests.get(url, params=params, headers=headers, timeout=TIMEOUT_SEC)
        elapsed_ms = int((time.time() - t0) * 1000)
        rec["elapsed_ms"] = elapsed_ms
        rec["status_code"] = r.status_code
        rec["content_type"] = r.headers.get("Content-Type", "")
        rec["content_length"] = r.headers.get("Content-Length", "")
        # bodyは巨大化防止のため先頭だけ
        rec["body_head"] = r.text[:200]
    except Exception as e:
        elapsed_ms = int((time.time() - t0) * 1000)
        rec["elapsed_ms"] = elapsed_ms
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
            # 典型: ('Close','JPY=X')
            if ("Close", ticker) in df.columns:
                s = df[("Close", ticker)]
            elif (ticker, "Close") in df.columns:
                s = df[(ticker, "Close")]
            else:
                # 明示一致が無ければ誤取得回避のため失敗扱い
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

def _yf_download_multi(tickers: List[str]) -> Tuple[pd.DataFrame | None, str, str]:
    """
    1回のdownloadでまとめて取得（リクエスト数を減らす）。
    戻り: (df or None, err_type, err_msg)
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
        return df, "", ""
    except Exception as e:
        return None, type(e).__name__, str(e)

@dataclass
class AssetResult:
    price: float
    missing: int
    source: str
    date: str
    fail: str

def collect() -> int:
    print("=== yfinance market fetch ===")
    print(f"run_id       : {RUN_ID}")
    print(f"timestamp_jst: {now_jst_str()}")

    # CSVの安全装置（列ズレ/破損なら隔離）
    _ensure_csv_header_or_quarantine(OUT_CSV)

    tickers = list(ASSETS.values())

    # まず attempt_no=0 として「このrunの最初のHTTP応答」を1回残す
    _append_yahoo_http_probe(RUN_ID, 0, tickers)

    results: Dict[str, AssetResult] = {}
    last_err_type = ""
    last_err_msg = ""

    for attempt in range(1, MAX_RETRIES + 1):
        # そのattemptの直前にもHTTP証拠を残す（失敗回の追跡が一発になる）
        _append_yahoo_http_probe(RUN_ID, attempt, tickers)

        df, err_type, err_msg = _yf_download_multi(tickers)
        last_err_type, last_err_msg = err_type, err_msg

        ok = 0
        fail = 0

        for name, ticker in ASSETS.items():
            v, why = _extract_last_close(df, ticker) if df is not None else (None, err_type or "DownloadFailed")
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
        planned_sleep = 0.0 if ok == len(ASSETS) else (BASE_SLEEP * (2 ** (attempt - 1)))
        actual_sleep = 0.0

        # 試行ログ（失敗理由/待ち時間/何回目/ok-failを固定で残す）
        _append_retry_trial(
            run_id=RUN_ID,
            attempt_no=attempt,
            ok=ok,
            fail=fail,
            planned_sleep_sec=planned_sleep,
            actual_sleep_sec=0.0,
            err_type=err_type,
            err_msg=err_msg,
            symbols=tickers,
        )

        if ok == len(ASSETS):
            break

        if attempt == MAX_RETRIES:
            break

        actual_sleep = _sleep_with_jitter(planned_sleep)

        # actual_sleep を同attempt行に“上書き追記”はCSVでは面倒なので、
        # 次のattemptに進む前に「sleepだけの追記行」を追加しても良いが、
        # 追跡は planned_sleep_sec で可能なのでここでは省略。

    # 32列固定で追記（列順強制）
    row: Dict[str, object] = {"run_id": RUN_ID, "timestamp_jst": now_jst_str()}
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

    # 人間が見る用の表示
    for name, ticker in ASSETS.items():
        r = results[name]
        mark = "✅" if r.missing == 0 else "❌"
        date_disp = r.date if r.date else ""
        print(f"[yfinance] {name}({ticker}): {r.price} ({date_disp}) {mark} fail={r.fail}")

    print(f"=== saved -> {OUT_CSV} ===")

    # collector自体は落とさない（監視が落とす設計を維持）
    return 0

if __name__ == "__main__":
    raise SystemExit(collect())
