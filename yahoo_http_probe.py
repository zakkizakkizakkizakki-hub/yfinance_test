# save as: yahoo_http_probe.py
from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from typing import Dict, Any, List

import requests


OUT_JSONL = "yahoo_http_probe.jsonl"
TIMEOUT = 20

SYMBOLS = ["JPY=X", "BTC-USD", "GC=F", "^TNX", "CL=F", "^VIX"]


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def safe_preview(text: str, n: int = 200) -> str:
    if text is None:
        return ""
    t = text.replace("\r", "\\r").replace("\n", "\\n")
    return t[:n]


def probe_one(name: str, url: str, headers: Dict[str, str]) -> Dict[str, Any]:
    t0 = time.time()
    try:
        r = requests.get(url, headers=headers, timeout=TIMEOUT, allow_redirects=True)
        elapsed_ms = int((time.time() - t0) * 1000)

        body_bytes = r.content or b""
        body_len = len(body_bytes)

        try:
            text_preview = safe_preview(r.text, 200)
        except Exception:
            text_preview = ""

        out = {
            "ts_utc": utc_now_iso(),
            "name": name,
            "url": url,
            "final_url": r.url,
            "status": int(r.status_code),
            "elapsed_ms": elapsed_ms,
            "headers": {
                k: v for k, v in r.headers.items()
                if k.lower() in {
                    "content-type",
                    "content-length",
                    "cache-control",
                    "date",
                    "server",
                    "strict-transport-security",
                    "x-yahoo-request-id",
                    "set-cookie",
                }
            },
            "body_len_bytes": body_len,
            "preview": text_preview,
        }
        return out

    except Exception as e:
        elapsed_ms = int((time.time() - t0) * 1000)
        return {
            "ts_utc": utc_now_iso(),
            "name": name,
            "url": url,
            "final_url": "",
            "status": -1,
            "elapsed_ms": elapsed_ms,
            "headers": {},
            "body_len_bytes": 0,
            "preview": f"EXCEPTION: {type(e).__name__}: {e}",
        }


def append_jsonl(rows: List[Dict[str, Any]], path: str) -> None:
    with open(path, "a", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")


def main() -> int:
    # GitHub Actions でも実行できる最低限のUA
    req_headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        "Accept": "*/*",
        "Connection": "close",
    }

    urls = [
        ("quote(v7)", "https://query1.finance.yahoo.com/v7/finance/quote?symbols=" + ",".join(SYMBOLS)),
    ]
    for s in SYMBOLS:
        urls.append(
            (f"chart({s})", f"https://query1.finance.yahoo.com/v8/finance/chart/{s}?range=1d&interval=1d")
        )

    rows = []
    for name, url in urls:
        row = probe_one(name=name, url=url, headers=req_headers)
        rows.append(row)
        print(f"[{row['status']}] {name}  {row['final_url']}  ({row['elapsed_ms']}ms)")

    append_jsonl(rows, OUT_JSONL)
    print(f"=== saved -> {OUT_JSONL} (added {len(rows)} lines) ===")

    # 429が1つでもあったら「証拠」として exit 1 にする（監視用）
    if any(r.get("status") == 429 for r in rows):
        print("ERROR: Detected HTTP 429 Too Many Requests")
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
