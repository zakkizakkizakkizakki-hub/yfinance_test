# save as: monitor.py
from __future__ import annotations

import sys
from typing import List

import pandas as pd

CSV_PATH = "market_yfinance_log.csv"

ASSETS: List[str] = ["USDJPY", "BTC", "Gold", "US10Y", "Oil", "VIX"]

def _to_float(x) -> float:
    # "EmptyDF" など文字が紛れても落ちないように安全変換
    try:
        return float(x)
    except Exception:
        return float("nan")

def main() -> int:
    print("\n" + "=" * 60)
    print("📡 Market Monitor")
    print("=" * 60)

    # CSVが読めない時点で監視としては異常なので exit 1
    try:
        df = pd.read_csv(CSV_PATH, encoding="utf-8-sig")
    except Exception as e:
        print(f"❌ CSVが読めません: {type(e).__name__}: {e}")
        return 1

    if df.empty:
        print("❌ CSVが空です（データ行がありません）")
        return 1

    last = df.iloc[-1].to_dict()
    ts = str(last.get("timestamp_jst", "Unknown"))
    print(f"[ Latest ] {ts}")

    missing_assets: List[str] = []

    for a in ASSETS:
        v = _to_float(last.get(a, 0.0))
        miss = int(_to_float(last.get(f"{a}_missing", 1)) or 1)
        date = str(last.get(f"{a}_date", ""))
        fail = str(last.get(f"{a}_fail", ""))

        # 異常判定：missing=1 または 数値がNaN/<=0
        bad = (miss == 1) or (pd.isna(v)) or not (v > 0)

        if bad:
            missing_assets.append(a)
            print(f" - {a:5s}: {v:12.6f} (⚠️欠損) date={date}")
            if fail and fail != "nan":
                print(f"   Warning: {a}_fail: {fail}")
        else:
            print(f" - {a:5s}: {v:12.6f} (✅正常) date={date}")

    if missing_assets:
        print("\n" + "!" * 60)
        print(f"❌ 欠損を検知: {', '.join(missing_assets)}")
        print("   → 監視仕様により exit code 1 で終了します。")
        print("!" * 60)
        return 1

    print("\n✅ 監視OK（欠損なし）")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
