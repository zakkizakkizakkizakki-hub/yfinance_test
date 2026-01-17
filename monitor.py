# save as: monitor.py
from __future__ import annotations

import os
import sys
import pandas as pd

CSV_PATH = "market_yfinance_log.csv"

ASSETS = ["USDJPY", "BTC", "Gold", "US10Y", "Oil", "VIX"]


def _to_float(x) -> float:
    try:
        if pd.isna(x):
            return 0.0
        return float(x)
    except Exception:
        return 0.0


def _to_int(x) -> int:
    try:
        if pd.isna(x):
            return 1
        return int(float(x))
    except Exception:
        return 1


def main() -> int:
    print("\n" + "=" * 60)
    print("📡 Market Monitor")
    print("=" * 60)

    if not (os.path.exists(CSV_PATH) and os.path.getsize(CSV_PATH) > 0):
        print(f"Error: {CSV_PATH} not found or empty")
        return 1

    df = pd.read_csv(CSV_PATH, encoding="utf-8-sig")
    if df.empty:
        print(f"Error: {CSV_PATH} has no rows")
        return 1

    last = df.iloc[-1].to_dict()
    latest_ts = str(last.get("timestamp_jst", "Unknown"))
    print(f"[ Latest ] {latest_ts}")

    missing_assets = []

    for a in ASSETS:
        v = _to_float(last.get(a))
        miss = _to_int(last.get(f"{a}_missing"))
        date = last.get(f"{a}_date")
        fail = last.get(f"{a}_fail")

        # “欠損”判定（仕様維持）
        is_missing = (miss != 0) or (v <= 0.0)

        badge = "⚠️欠損" if is_missing else "✅正常"
        date_str = "nan" if pd.isna(date) else str(date)

        print(f" - {a:5s}: {v:12.6f} ({badge}) date={date_str}")
        if pd.notna(fail) and str(fail).strip() and str(fail) != "nan":
            print(f"   Warning: {a}_fail: {fail}")

        if is_missing:
            missing_assets.append(a)

    if missing_assets:
        print("\n" + "!" * 60)
        print(f"❌ 欠損を検知: {', '.join(missing_assets)}")
        print("   → 監視仕様により exit code 1 で終了します。")
        print("!" * 60)
        return 1

    print("\n✅ OK: 欠損なし")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
