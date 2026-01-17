# probe_yahoo_http.py
# Purpose:
#   Evidence-only probe for Yahoo Finance endpoints used by many clients.
#   - Record HTTP status code (200/403/429/etc)
#   - Record response headers (Content-Type, Cache-Control, etc)
#   - Record a short preview of body
# Output:
#   - run_logs/yahoo_http_probe.jsonl (append)

from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple

import requests


SYMBOLS = ["JPY=X", "BTC-USD", "GC=F", "^TNX", "CL=F", "^VIX"]

# Common Yahoo Finance endpoints
# (We are NOT claiming these are "the" endpoints yfinance always uses;
#  we just probe these well-known public endpoints to capture evidence.)
URLS: List[Tuple[str, str]] = [
    (
        "quote(v7)",
        "https://query1.finance.yahoo.com/v7/finance/quote?symbols="
        + requests.utils.quote(",".join(SYMBOLS)),
    ),
    (
        "chart(JPY=X)",
        "https://query1.finance.yahoo.com/v8/finance/chart/JPY=X?range=1d&interval=1d",
    ),
    (
        "chart(GC=F)",
        "https://query1.finance.yahoo.com/v8/finance/chart/GC=F?range=1d&interval=1d",
    ),
    (
        "chart(^VIX)",
        "https://query1.finance.yahoo.com/v8/finance/chart/%5EVIX?range=1d&interval=1d",
    ),
]

OUT_DIR = "run_logs"
OUT_PATH = os.path.join(OUT_DIR, "yahoo_http_probe.jsonl")


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def safe_text_preview(resp: requests.Response, limit: int = 300) -> str:
    # bytes preview -> decode safely
    b = resp.content[:2000]
    try:
        txt = b.decode(resp.encoding or "utf-8", errors="replace")
    except Exception:
        txt = b.decode("utf-8", errors="replace")
    txt = txt.replace("\r", "\\r").replace("\n", "\\n")
    return txt[:limit]


def pick_headers(h: Dict[str, str]) -> Dict[str, str]:
    # keep key headers useful for diagnosis
    keep = [
        "Content-Type",
        "Content-Length",
        "Cache-Control",
        "Pragma",
        "Expires",
        "Date",
        "Server",
        "Via",
        "X-Cache",
        "X-Cache-Hits",
        "Set-Cookie",
        "Location",
        "Retry-After",
        "Strict-Transport-Security",
    ]
    out: Dict[str, str] = {}
    for k in keep:
        if k in h:
            out[k] = h.get(k, "")
    return out


def one_fetch(session: requests.Session, name: str, url: str) -> Dict[str, Any]:
    headers = {
        # "Browser-like" but minimal; not claiming this bypasses anything.
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36",
        "Accept": "application/json,text/plain,*/*",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "close",
    }

    t0 = time.time()
    try:
        resp = session.get(url, headers=headers, timeout=20, allow_redirects=True)
        dt_ms = int((time.time() - t0) * 1000)

        rec = {
            "ts_utc": utc_now_iso(),
            "name": name,
            "url": url,
            "final_url": resp.url,
            "status": resp.status_code,
            "elapsed_ms": dt_ms,
            "headers": pick_headers(dict(resp.headers)),
            "body_len_bytes": len(resp.content),
            "preview": safe_text_preview(resp),
        }
        return rec
    except Exception as e:
        dt_ms = int((time.time() - t0) * 1000)
        return {
            "ts_utc": utc_now_iso(),
            "name": name,
            "url": url,
            "status": None,
            "elapsed_ms": dt_ms,
            "error": f"{type(e).__name__}: {e}",
        }


def main() -> int:
    os.makedirs(OUT_DIR, exist_ok=True)

    print("=== Yahoo HTTP Probe (evidence only) ===")
    print("utc:", utc_now_iso())
    print("out:", OUT_PATH)
    print("targets:", ", ".join(SYMBOLS))
    print("---------------------------------------")

    session = requests.Session()

    records: List[Dict[str, Any]] = []
    for name, url in URLS:
        rec = one_fetch(session, name, url)
        records.append(rec)

        # console output (short)
        print(f"[{name}] status={rec.get('status')} bytes={rec.get('body_len_bytes')} url={rec.get('final_url', rec.get('url'))}")
        if rec.get("status") in (301, 302, 303, 307, 308):
            print("  redirect ->", rec.get("final_url"))
        if rec.get("headers"):
            h = rec["headers"]
            ct = h.get("Content-Type", "")
            ra = h.get("Retry-After", "")
            loc = h.get("Location", "")
            if ct:
                print("  Content-Type:", ct)
            if ra:
                print("  Retry-After:", ra)
            if loc:
                print("  Location:", loc)
        if rec.get("error"):
            print("  ERROR:", rec["error"])
        else:
            # show preview only when not huge
            print("  preview:", rec.get("preview", ""))

        print("---------------------------------------")

        # gentle spacing between requests
        time.sleep(1.0)

    # append jsonl
    with open(OUT_PATH, "a", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

    print("saved ->", OUT_PATH)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
