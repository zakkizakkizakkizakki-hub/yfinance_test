# save as: yahoo_http_probe.py
from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone
from typing import Dict, Any, List, Tuple

import requests

# =========================
# Config (edit if needed)
# =========================
# まずは 1銘柄だけで検証推奨（負荷を増やさない）
SYMBOLS = os.environ.get("SYMBOLS", "JPY=X").split(",")

# 検証のリトライ間隔（秒）
# 例: 10,30,60,120,300 などにして「待てば復活するか」を確定する
BACKOFF_SECONDS = [int(x) for x in os.environ.get("BACKOFF", "10,30,60,120,300").split(",")]

# 1回の実行で各銘柄に何回打つか（= len(BACKOFF_SECONDS) 推奨）
ATTEMPTS = int(os.environ.get("ATTEMPTS", str(len(BACKOFF_SECONDS))))

# タイムアウト
TIMEOUT = int(os.environ.get("TIMEOUT", "20"))

# 出力（証拠ログ）
OUT_JSONL = os.environ.get("OUT_JSONL", "yahoo_http_probe.jsonl")

# User-Agent（“ブラウザっぽく”はするが、偽装や回避を狙うものではなく、差分確認用）
UA = os.environ.get(
    "UA",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

# =========================
# Targets (Yahoo endpoints)
# =========================
def build_targets(symbol: str) -> List[Tuple[str, str, Dict[str, str]]]:
    """
    代表的な2系統を叩く：
    - quote: 現在値系
    - chart: 時系列（yfinanceがよく使う）
    """
    symbol = symbol.strip()
    headers = {
        "User-Agent": UA,
        "Accept": "application/json,text/plain,*/*",
        "Accept-Language": "en-US,en;q=0.9,ja;q=0.8",
        "Connection": "close",
    }

    # 1) quote
    quote_url = "https://query1.finance.yahoo.com/v7/finance/quote"
    quote_params = {"symbols": symbol}

    # 2) chart
    chart_url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
    chart_params = {"range": "5d", "interval": "1d"}

    return [
        ("quote", quote_url, quote_params | headers),  # headersは後で分離する
        ("chart", chart_url, chart_params | headers),
    ]


def split_headers(params_or_headers: Dict[str, str]) -> Tuple[Dict[str, str], Dict[str, str]]:
    """paramsとheadersをざっくり分離（headersキーを知ってる前提）"""
    header_keys = {"User-Agent", "Accept", "Accept-Language", "Connection"}
    headers = {k: v for k, v in params_or_headers.items() if k in header_keys}
    params = {k: v for k, v in params_or_headers.items() if k not in header_keys}
    return params, headers


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def pick_headers(h: Dict[str, str]) -> Dict[str, Any]:
    """ログを汚さない程度に重要ヘッダだけ拾う"""
    want = [
        "date",
        "content-type",
        "content-length",
        "server",
        "cache-control",
        "expires",
        "pragma",
        "x-yahoo-request-id",
        "x-request-id",
        "set-cookie",
        "location",
        "retry-after",
        "strict-transport-security",
        "cf-ray",
        "via",
    ]
    out = {}
    for k in want:
        if k in h:
            out[k] = h.get(k)
    return out


def one_request(session: requests.Session, kind: str, url: str, params: Dict[str, str], headers: Dict[str, str]) -> Dict[str, Any]:
    t0 = time.time()
    try:
        r = session.get(url, params=params, headers=headers, timeout=TIMEOUT, allow_redirects=True)
        dt = time.time() - t0

        text = r.text if isinstance(r.text, str) else ""
        head = text[:500] if text else ""
        return {
            "ok": True,
            "kind": kind,
            "url": url,
            "final_url": str(r.url),
            "status": int(r.status_code),
            "elapsed_sec": round(dt, 3),
            "resp_headers": pick_headers({k.lower(): v for k, v in r.headers.items()}),
            "body_len": len(r.content or b""),
            "head_500": head,
        }
    except Exception as e:
        dt = time.time() - t0
        return {
            "ok": False,
            "kind": kind,
            "url": url,
            "status": None,
            "elapsed_sec": round(dt, 3),
            "error_type": type(e).__name__,
            "error": str(e),
        }


def append_jsonl(path: str, obj: Dict[str, Any]) -> None:
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(obj, ensure_ascii=False) + "\n")


def main() -> int:
    print("=== Yahoo HTTP Probe (evidence logger) ===")
    print(f"symbols: {SYMBOLS}")
    print(f"attempts: {ATTEMPTS}")
    print(f"backoff_seconds: {BACKOFF_SECONDS}")
    print(f"out: {OUT_JSONL}")
    print("NOTE: 429/403/200 を“事実として”記録するための最小スクリプトです。\n")

    session = requests.Session()

    for sym in [s.strip() for s in SYMBOLS if s.strip()]:
        print(f"\n--- SYMBOL: {sym} ---")

        targets = build_targets(sym)

        for i in range(ATTEMPTS):
            wait = BACKOFF_SECONDS[i] if i < len(BACKOFF_SECONDS) else BACKOFF_SECONDS[-1]

            for kind, url, mixed in targets:
                params, headers = split_headers(mixed)
                rec = {
                    "ts_utc": utc_now_iso(),
                    "symbol": sym,
                    "attempt": i + 1,
                    "planned_wait_sec": wait,
                    "request": {
                        "kind": kind,
                        "url": url,
                        "params": params,
                        "headers": {
                            # ログに残すのは差分追跡用の最小限
                            "User-Agent": headers.get("User-Agent", ""),
                            "Accept": headers.get("Accept", ""),
                            "Accept-Language": headers.get("Accept-Language", ""),
                        },
                    },
                }

                res = one_request(session, kind, url, params=params, headers=headers)
                rec["response"] = res

                append_jsonl(OUT_JSONL, rec)

                status = res.get("status")
                body_len = res.get("body_len")
                print(f"[{kind}] status={status} body={body_len} bytes final_url={res.get('final_url','')}")

            if i < ATTEMPTS - 1:
                print(f"sleep {wait}s ...")
                time.sleep(wait)

    print("\n=== DONE. Check jsonl ===")
    print(f"  {OUT_JSONL}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
