# save as: market_yfinance_collector.py
from __future__ import annotations

import os
import time
import json
import csv
import random
from datetime import datetime, timezone, timedelta
from typing import Dict, Tuple, Optional

import pandas as pd
import yfinance as yf
import requests

JST = timezone(timedelta(hours=9))

ASSETS = {
    "USDJPY": "JPY=X",
    "BTC": "BTC-USD",
    "Gold": "GC=F",
    "US10Y": "^TNX",
    "Oil": "CL=F",
    "VIX": "^VIX",
}

OUT_CSV = "market_yfinance_log.csv"
PROBE_JSONL = "yahoo_http_probe.jsonl"
TRY_CSV = "retry_trials.csv"

CSV_ENCODING = "utf-8-sig"
CSV_QUOTING = csv.QUOTE_MINIMAL
CSV_LINETERMINATOR = "\n"

# ===== 検証用: 待機時間（秒）を候補として並べる =====
# 例: 5秒→15秒→45秒→120秒 ... のように強めていく
RETRY_SCHEDULE_SECONDS = [5, 15, 45, 120]

YF_PERIOD = "7d"
YF_INTERVAL = "1d"

HTTP_TIMEOUT = 20

# Yahoo側の「実体」(yfinanceが内部で叩くのと同系統)
# 価格取得の正否というより「429が返ってきてるか」を観測する用途
YAHOO_PROBE_URLS = [
    "https://query1.finance.yahoo.com/v7/finance/quote?symbols=JPY=X",
    "https://query1.finance.yahoo.com/v7/finance/quote?symbols=GC=F",
    "https://query1.finance.yahoo.com/v7/finance/quote?symbols=^VIX",
]


def now_jst_str() -> str:
    return datetime.now(JST).strftime("%Y-%m-%d %H:%M:%S")


def append_jsonl(path: str, obj: dict) -> None:
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(obj, ensure_ascii=False) + "\n")


def safe_float(x) -> float:
    try:
        v = float(x)
        if pd.isna(v):
            return 0.0
        return v
    except Exception:
        return 0.0


def probe_yahoo_http(tag: str) -> None:
    """
    Yahoo側に実際にHTTPで当てて、status(200/429等)とヘッダを記録する。
    """
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json,text/html,*/*",
    }
    for url in YAHOO_PROBE_URLS:
        t0 = time.time()
        try:
            r = requests.get(url, timeout=HTTP_TIMEOUT, headers=headers)
            dt = int((time.time() - t0) * 1000)
            body = (r.text or "")[:200]
            rec = {
                "ts_jst": now_jst_str(),
                "tag": tag,
                "url": url,
                "status": r.status_code,
                "elapsed_ms": dt,
                "content_type": r.headers.get("Content-Type", ""),
                "content_length": r.headers.get("Content-Length", ""),
                "server": r.headers.get("Server", ""),
                "via": r.headers.get("Via", ""),
                "body_head": body,
            }
            append_jsonl(PROBE_JSONL, rec)
        except Exception as e:
            rec = {
                "ts_jst": now_jst_str(),
                "tag": tag,
                "url": url,
                "status": None,
                "error": f"{type(e).__name__}: {e}",
            }
            append_jsonl(PROBE_JSONL, rec)


def yfinance_fetch_one(ticker: str) -> Tuple[float, str, str]:
    """
    価格, fail_reason, date_str
    取れなければ (0.0, "EmptyDF" or exception, "")
    """
    try:
        df = yf.download(
            ticker,
            period=YF_PERIOD,
            interval=YF_INTERVAL,
            progress=False,
        )
        if df is None or df.empty:
            return 0.0, "EmptyDF", ""
        # Close優先
        if "Close" in df.columns:
            s = df["Close"]
            if isinstance(s, pd.DataFrame):
                s = s.iloc[:, 0]
        else:
            s = df.iloc[:, 0]
        s = pd.to_numeric(s, errors="coerce").dropna()
        if s.empty:
            return 0.0, "NoDataAfterDrop", ""
        price = float(s.iloc[-1])
        if not (price > 0):
            return 0.0, "NonPositive", ""
        date_str = str(s.index[-1])[:10]
        return price, "", date_str
    except Exception as e:
        return 0.0, type(e).__name__, ""


def collect_once(tag: str) -> Dict[str, object]:
    """
    1回分の収集結果（成功/失敗含む）を返す。
    """
    row: Dict[str, object] = {"timestamp_jst": now_jst_str(), "tag": tag}
    # 先にHTTP観測（証拠取り）
    probe_yahoo_http(tag=f"collector:{tag}")

    for name, ticker in ASSETS.items():
        price, fail, d = yfinance_fetch_one(ticker)
        row[name] = price
        row[f"{name}_fail"] = fail or ""
        row[f"{name}_date"] = d or ""
        row[f"{name}_missing"] = 1 if (price <= 0 or fail) else 0
    return row


def append_csv_row(path: str, row: Dict[str, object]) -> None:
    df = pd.DataFrame([row])
    header = not (os.path.exists(path) and os.path.getsize(path) > 0)
    df.to_csv(
        path,
        mode="a",
        index=False,
        header=header,
        encoding=CSV_ENCODING,
        quoting=CSV_QUOTING,
        lineterminator=CSV_LINETERMINATOR,  # ← pandasはこれが正しい
    )


def main() -> int:
    print("=== yfinance retry interval experiment ===")
    print(f"timestamp_jst: {now_jst_str()}")
    print(f"retry_schedule_seconds: {RETRY_SCHEDULE_SECONDS}")

    # 各トライの要約を残す（成功率をあとで見れる）
    for i, wait_s in enumerate(RETRY_SCHEDULE_SECONDS, start=1):
        tag = f"try{i}_wait{wait_s}s"

        # 1回実行
        row = collect_once(tag=tag)

        # ログ（試行サマリ）に追記
        miss = 0
        for a in ASSETS.keys():
            miss += int(row.get(f"{a}_missing", 1))
        row["missing_assets_count"] = miss
        append_csv_row(TRY_CSV, row)

        # 本番ログにも追記（※仕様を壊さない：同じ列名で追記）
        append_csv_row(OUT_CSV, row)

        # コンソール表示（GitHub Actionsで見やすい）
        print(f"\n--- {tag} ---")
        for a, t in ASSETS.items():
            v = row.get(a, 0.0)
            fail = row.get(f"{a}_fail", "")
            d = row.get(f"{a}_date", "")
            ok = "✅" if (safe_float(v) > 0 and not fail) else "❌"
            print(f"[yfinance] {a}({t}): {v} ({d}) {ok} fail={fail}")
        print(f"missing_assets_count: {miss}")

        # すべて揃ったら実験を早期終了（無駄打ち防止）
        if miss == 0:
            print("All assets succeeded. Stop early.")
            return 0

        # 次の試行まで待機（ジッター少し足す）
        jitter = random.uniform(0.0, 2.0)
        sleep_s = wait_s + jitter
        print(f"sleeping {sleep_s:.1f}s before next try...")
        time.sleep(sleep_s)

    # 最後まで揃わなかった
    print("Experiment finished, but some assets still missing.")
    return 0  # ← 実験自体は成功（監視で落とすのはmonitor側）


if __name__ == "__main__":
    raise SystemExit(main())
