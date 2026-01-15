# save as: monitor.py
from __future__ import annotations

import sys
import pandas as pd

CSV_PATH = "market_yfinance_log.csv"

# 監視対象（collectorの列名と一致させる）
ASSETS = ["USDJPY", "BTC", "Gold", "US10Y", "Oil", "VIX"]

def main() -> int:
    if not pd.io.common.file_exists(CSV_PATH):
        print(f"Error: {CSV_PATH} not found")
        return 1

    df = pd.read_csv(CSV_PATH, encoding="utf-8-sig")
    if df.empty:
        print("Error: CSV is empty")
        return 1

    last = df.iloc[-1].to_dict()
    ts = last.get("timestamp_jst", "Unknown")

    print("\n" + "=" * 60)
    print("📡 Market Monitor")
    print("=" * 60)
    print(f"[ Latest ] {ts}")

    missing = []
    warnings = []

    for a in ASSETS:
        v = float(last.get(a, 0.0) or 0.0)
        miss = int(last.get(f"{a}_missing", 1) or 1)
        d = str(last.get(f"{a}_date", ""))
        fail = str(last.get(f"{a}_fail", ""))

        status = "✅正常" if miss == 0 else "⚠️欠損"
        print(f" - {a:5s}: {v:12.6f} ({status}) date={d if d else 'nan'}")

        if miss != 0:
            missing.append(a)
            if fail:
                print(f"   Warning: {a}_fail: {fail}")

        # ---- 異常値チェック（最低限）----
        if miss == 0:
            if v <= 0:
                warnings.append(f"{a} value<=0")
            # ざっくり上限チェック（壊れた値を弾く安全柵）
            if a == "USDJPY" and not (50 <= v <= 300):
                warnings.append(f"{a} out_of_range({v})")
            if a == "US10Y" and not (0 <= v <= 20):
                warnings.append(f"{a} out_of_range({v})")
            if a == "VIX" and not (5 <= v <= 200):
                warnings.append(f"{a} out_of_range({v})")

    if warnings:
        print("\n[WARN] value anomaly:")
        for w in warnings:
            print(" - " + w)
        # 異常値は「落とす」運用にするならここで return 1 にしてOK
        # 今回は欠損が最優先なので、異常値は警告のみ

    if missing:
        print("\n" + "!" * 60)
        print("❌ 欠損を検知:", ", ".join(missing))
        print("   → 監視仕様により exit code 1 で終了します。")
        print("!" * 60)
        return 1

    print("\n✅ OK: 欠損なし")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
