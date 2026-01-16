# monitor.py
from __future__ import annotations

import os
import pandas as pd
from datetime import datetime


CSV_PATH = os.getenv("MARKET_CSV", "market_yfinance_log.csv")

ASSETS = ["USDJPY", "BTC", "Gold", "US10Y", "Oil", "VIX"]

# 簡易な異常値検知（“明らかにおかしい” を落とす）
# ※厳密な金融工学的レンジではなく「ゼロ/負/NaN」や極端値を検知する最小限
ABNORMAL_RULES = {
    "USDJPY": (50, 300),
    "BTC": (1000, 1_000_000),
    "Gold": (100, 50_000),
    "US10Y": (0.0, 20.0),
    "Oil": (1, 500),
    "VIX": (1, 200),
}


def main() -> int:
    if not os.path.exists(CSV_PATH):
        print(f"[ERROR] CSV not found: {CSV_PATH}")
        return 1

    df = pd.read_csv(CSV_PATH)
    if df.empty:
        print("[ERROR] CSV is empty")
        return 1

    last = df.iloc[-1].to_dict()
    latest_ts = str(last.get("timestamp_jst", "Unknown"))

    print("\n" + "=" * 60)
    print("📡 Market Monitor")
    print("=" * 60)
    print(f"[ Latest ] {latest_ts}")

    missing = []
    abnormal = []

    for a in ASSETS:
        price_key = f"{a}_price"
        src_key = f"{a}_src"
        fail_key = f"{a}_fail"
        date_key = f"{a}_date"

        src = str(last.get(src_key, "missing"))
        fail = str(last.get(fail_key, ""))
        date = str(last.get(date_key, ""))

        # 数値化できないケースは欠損扱いに倒す（monitorが落ちるべき）
        try:
            v = float(last.get(price_key))
        except Exception:
            v = float("nan")

        is_missing = (src == "missing") or (pd.isna(v)) or (v == 0.0)
        if is_missing:
            missing.append(a)

        # 異常値（ただし欠損は別枠で扱う）
        if not is_missing:
            lo, hi = ABNORMAL_RULES[a]
            if not (lo <= v <= hi):
                abnormal.append(a)

        status = "⚠️欠損" if is_missing else "✅正常"
        print(f" - {a:5s}: {v:12.6f} ({status}) date={date or 'nan'}")
        if fail and fail != "nan":
            print(f"   Warning: {a}_fail: {fail}")

    if abnormal:
        print("\n" + "!" * 60)
        print(f"❌ 異常値を検知: {', '.join(abnormal)}")
        print("   → 監視仕様により exit code 1 で終了します。")
        print("!" * 60)
        return 1

    if missing:
        print("\n" + "!" * 60)
        print(f"❌ 欠損を検知: {', '.join(missing)}")
        print("   → 監視仕様により exit code 1 で終了します。")
        print("!" * 60)
        return 1

    print("\n✅ All OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
