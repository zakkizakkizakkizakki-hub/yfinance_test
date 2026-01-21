# save as: monitor.py
from __future__ import annotations

import sys
from typing import List

import pandas as pd

CSV_PATH = "market_yfinance_log.csv"
ENCODING = "utf-8-sig"

ASSETS = ["USDJPY", "BTC", "Gold", "US10Y", "Oil", "VIX"]

def _to_float(x) -> float | None:
    try:
        if x is None:
            return None
        if isinstance(x, str) and x.strip() == "":
            return None
        v = float(x)
        if pd.isna(v):
            return None
        return v
    except Exception:
        return None

def main() -> int:
    print("\n" + "=" * 60)
    print("📡 Market Monitor")
    print("=" * 60)

    df = pd.read_csv(CSV_PATH, encoding=ENCODING)

    if df.empty:
        print("❌ CSVが空です（記録がありません）")
        return 1

    last = df.iloc[-1].to_dict()

    run_id = str(last.get("run_id", ""))
    ts = str(last.get("timestamp_jst", ""))
    print(f"[ Latest ] {ts}")
    if run_id:
        print(f"[ run_id ] {run_id}")

    missing_assets: List[str] = []

    for a in ASSETS:
        price = _to_float(last.get(a))
        miss = _to_float(last.get(f"{a}_missing"))
        fail = str(last.get(f"{a}_fail", ""))

        is_missing = False
        if miss is None or int(miss) != 0:
            is_missing = True
        if price is None or not (price > 0):
            is_missing = True
        if fail.strip() != "":
            # failに理由が入っている＝成功扱いにしない
            is_missing = True

        mark = "✅正常" if not is_missing else "⚠️欠損"
        date = str(last.get(f"{a}_date", ""))

        print(f" - {a:5s}: {0.0 if price is None else price:12.6f} ({mark}) date={date if date else 'nan'}")
        if fail.strip():
            print(f"   Warning: {a}_fail: {fail}")

        if is_missing:
            missing_assets.append(a)

    if missing_assets:
        print("\n" + "!" * 60)
        print(f"❌ 欠損を検知: {', '.join(missing_assets)}")
        print("   → 監視仕様により exit code 1 で終了します。")
        print("!" * 60)
        return 1

    print("\n✅ 欠損なし（監視OK）")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
