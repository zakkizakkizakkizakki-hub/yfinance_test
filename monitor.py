# save as: monitor.py
from __future__ import annotations

import os
import sys
import pandas as pd

CSV_PATH = "market_yfinance_log.csv"

ASSETS = ["USDJPY", "BTC", "Gold", "US10Y", "Oil", "VIX"]


def to_float(x) -> float:
    try:
        v = float(x)
        if pd.isna(v):
            return 0.0
        return v
    except Exception:
        return 0.0


def main() -> int:
    print("\n============================================================")
    print("📡 Market Monitor")
    print("============================================================")

    if not os.path.exists(CSV_PATH) or os.path.getsize(CSV_PATH) == 0:
        print(f"❌ {CSV_PATH} がありません（または空です）")
        return 1

    df = pd.read_csv(CSV_PATH, encoding="utf-8-sig")
    if df.empty:
        print(f"❌ {CSV_PATH} が空です")
        return 1

    last = df.iloc[-1].to_dict()
    ts = str(last.get("timestamp_jst", "Unknown"))
    print(f"[ Latest ] {ts}")

    missing = []
    for a in ASSETS:
        v = to_float(last.get(a, 0.0))
        fail = str(last.get(f"{a}_fail", "") or "")
        d = str(last.get(f"{a}_date", "") or "")
        is_missing = (v <= 0.0) or (fail.strip() != "")

        mark = "⚠️欠損" if is_missing else "✅正常"
        print(f" - {a:<5}: {v:12.6f} ({mark}) date={d if d else 'nan'}")
        if fail:
            print(f"   Warning: {a}_fail: {fail}")

        if is_missing:
            missing.append(a)

    if missing:
        print("\n!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        print(f"❌ 欠損を検知: {', '.join(missing)}")
        print("   → 監視仕様により exit code 1 で終了します。")
        print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        return 1

    print("\n✅ 全資産OK（欠損なし）")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
