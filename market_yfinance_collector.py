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
CSV_LINETERMINATOR = "\n"  # ← pandas は lineterminator です（line_terminator ではない）

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
BASE_SLEEP = 15.0  # seconds

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
    - CSVが無ければ作る（ヘッダは後でto_csvのheader=Trueで書く）
    - 既存CSVのヘッダが想定(31列)と違う / パース不能なら隔離して新規にする
    """
    if not os.path.exists(path):
        return

    if os.path.getsize(path) == 0:
        return

    # まずヘッダ行だけ確認（列ズレの多くはここで確定できる）
    first = _read_first_line(path)
    expected = ",".join([f"\"{c}\"" for c in EXPECTED_COLS])  # QUOTE_ALL 前提のヘッダ形
    if first != expected:
        # QUOTE_ALLでない可能性もあるので、CSVとして解釈も試す
        try:
            dfh = pd.read_csv(path, encoding=CSV_ENCODING, nrows=1)
            if list(dfh.columns) != EXPECTED_COLS:
                _quarantine(path, "header_mismatch")
        except Exception as e:
            _quarantine(path, f"read_fail:{type(e).__name__}")
        return

    # 本文が壊れている可能性（ParserError等）を最小コストで確認
    try:
        pd.read_csv(path, encoding=CSV_ENCODING)
    except Exception as e:
        _quarantine(path, f"parse_fail:{type(e).__name__}")

def _sleep_with_jitter(sec: float) -> None:
    time.sleep(sec + random.uniform(0.0, 2.0))

def _write_retry_trial(attempt: int, ok: int, fail: int, err: str, sleep_sec: float, symbols: List[str]) -> None:
    row = {
        "timestamp_jst": now_jst_str(),
        "attempt": attempt,
        "symbols": " ".join(symbols),
        "ok_count": ok,
        "fail_count": fail,
        "error": err,
        "sleep_sec": round(sleep_sec, 3),
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

def _probe_yahoo_http(symbols: List[str]) -> None:
    """
    「Yahoo側が返しているHTTPステータスとレスポンスヘッダ」を証拠として残す。
    ※ yfinance内部の全通信を捕捉できるわけではないが、Yahooのquote APIに対する
       “この実行環境からの返り” を事実として残せる。
    """
    url = "https://query1.finance.yahoo.com/v7/finance/quote"
    params = {"symbols": ",".join(symbols)}
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json,text/plain,*/*",
    }
    rec = {
        "ts_utc": datetime.now(timezone.utc).isoformat(),
        "url": url,
        "symbols": symbols,
    }
    try:
        r = requests.get(url, params=params, headers=headers, timeout=20)
        rec["status_code"] = r.status_code
        rec["content_type"] = r.headers.get("Content-Type", "")
        rec["content_length"] = r.headers.get("Content-Length", "")
        # bodyを全部は保存しない（巨大化防止）
        rec["body_head"] = r.text[:200]
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

        # 複数ティッカーの場合は columns が MultiIndex になりやすい
        # 例: ('Close','JPY=X') など
        if isinstance(df.columns, pd.MultiIndex):
            if ("Close", ticker) in df.columns:
                s = df[("Close", ticker)]
            elif (ticker, "Close") in df.columns:
                s = df[(ticker, "Close")]
            else:
                # どれかの形でCloseがあるか探索
                close_cols = [c for c in df.columns if len(c) == 2 and ("Close" in c)]
                if close_cols:
                    # 間違って他のtickerを拾うのを避けたいので明示一致が無ければ失敗扱い
                    return None, "CloseNotFoundForTicker"
                return None, "CloseNotFound"
        else:
            # 単一tickerの可能性
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
    1回のdownloadでまとめて取得（リクエスト数を減らしてレート制限リスクを下げる）。
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

@dataclass
class AssetResult:
    price: float
    missing: int
    source: str
    date: str
    fail: str

def collect() -> int:
    print("=== yfinance market fetch ===")
    print(f"timestamp_jst: {now_jst_str()}")

    # CSVの安全装置（列ズレ/破損なら隔離）
    _ensure_csv_header_or_quarantine(OUT_CSV)

    tickers = list(ASSETS.values())

    # 証拠取り：Yahooのquote APIへのHTTP応答を1回ログ
    _probe_yahoo_http(tickers)

    results: Dict[str, AssetResult] = {}

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
                # 日付は df.index 最後（yfinanceの足の最終日）
                date_str = ""
                try:
                    date_str = str(df.index[-1])[:10] if (df is not None and not df.empty) else ""
                except Exception:
                    date_str = ""
                results[name] = AssetResult(float(v), 0, "yfinance", date_str, "")
                ok += 1

        # 試行ログ（retry_trials.csv）
        sleep_sec = 0.0 if ok == len(ASSETS) else (BASE_SLEEP * (2 ** (attempt - 1)))
        _write_retry_trial(attempt, ok, fail, last_err or "", sleep_sec, tickers)

        # 成功なら終了
        if ok == len(ASSETS):
            break

        # 最終試行なら抜ける
        if attempt == MAX_RETRIES:
            break

        # 失敗→待って再試行
        _sleep_with_jitter(sleep_sec)

    # 31列固定で書き出し（列順強制）
    row: Dict[str, object] = {"timestamp_jst": now_jst_str()}
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

    # 表示（人間が見る用）
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
