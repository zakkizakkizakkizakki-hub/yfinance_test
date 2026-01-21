# save as: monitor.py
from __future__ import annotations

import os
import sys
from typing import List, Dict

import pandas as pd

CSV_PATH = os.getenv("MARKET_CSV", "market_yfinance_log.csv")
ENCODING = "utf-8-sig"

ASSETS: List[str] = ["USDJPY", "BTC", "Gold", "US10Y", "Oil", "VIX"]

def _as_int(v) -> int:
    try:
        if pd.isna(v):
            return 1
        return int(float(v))
    except Exception:
        return 1

def main() -> int:
    print("\n" + "=" * 60)
    print("📡 Market Monitor")
    print("=" * 60)

    if not os.path.exists(CSV_PATH) or os.path.getsize(CSV_PATH) == 0:
        print(f"[ERROR] CSV not found or empty: {CSV_PATH}")
        return 1

    # CSVが多少壊れていても「最後の行」だけは読める可能性があるため、engine=pythonで読む
    try:
        df = pd.read_csv(CSV_PATH, encoding=ENCODING, engine="python")
    except Exception as e:
        print(f"[ERROR] Failed to read CSV: {type(e).__name__}: {e}")
        return 1

    if df.empty:
        print("[ERROR] CSV has no rows.")
        return 1

    last = df.iloc[-1].to_dict()
    run_id = str(last.get("run_id", "")) if "run_id" in last else "(no_run_id)"
    ts = str(last.get("timestamp_jst", ""))

    print(f"[ Latest ] {ts}  run_id={run_id}")

    missing_assets: List[str] = []

    for a in ASSETS:
        v = last.get(a, None)
        miss_flag = last.get(f"{a}_missing", None)
        fail = last.get(f"{a}_fail", "")

        missing = _as_int(miss_flag) if miss_flag is not None else 1
        if missing != 0:
            missing_assets.append(a)

        # 表示用（専門用語注釈：missing=取得できなかったフラグ、fail=失敗理由の短文）
        try:
            vv = float(v) if v is not None and str(v) != "" else 0.0
        except Exception:
            vv = 0.0

        mark = "✅正常" if missing == 0 else "⚠️欠損"
        print(f" - {a:5s}: {vv:12.6f} ({mark})")
        if missing != 0:
            print(f"   Warning: {a}_fail: {fail}")

    if missing_assets:
        print("\n" + "!" * 56)
        print(f"❌ 欠損を検知: {', '.join(missing_assets)}")
        print("   → 監視仕様により exit code 1 で終了します。")
        print("!" * 56)
        return 1

    print("\n✅ すべて取得できています（欠損なし）")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
