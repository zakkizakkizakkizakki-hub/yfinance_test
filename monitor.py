# save as: monitor.py
from __future__ import annotations

import sys
import pandas as pd

CSV_PATH = "market_yfinance_log.csv"
ENCODING = "utf-8-sig"

ASSETS = ["USDJPY", "BTC", "Gold", "US10Y", "Oil", "VIX"]

def _safe_float(x) -> float:
    # 数字に変換できない場合は NaN 扱いにする（monitorが落ちないように）
    try:
        return float(x)
    except Exception:
        return float("nan")

def main() -> int:
    print("\n" + "=" * 60)
    print("📡 Market Monitor")
    print("=" * 60)

    df = pd.read_csv(CSV_PATH, encoding=ENCODING, engine="python")
    if df.empty:
        print("❌ CSVが空です。")
        return 1

    last = df.iloc[-1].to_dict()
    run_id = str(last.get("run_id", "Unknown"))
    ts = str(last.get("timestamp_jst", "Unknown"))

    print(f"[ Latest ] {ts}")
    print(f"[ run_id ] {run_id}")

    missing_assets = []

    for a in ASSETS:
        v = _safe_float(last.get(a))
        miss = int(_safe_float(last.get(f"{a}_missing")))
        date = str(last.get(f"{a}_date", ""))
        fail = str(last.get(f"{a}_fail", ""))

        status = "✅正常" if miss == 0 and (v == v) and v > 0 else "⚠️欠損"
        print(f" - {a:5s}: {v:12.6f} ({status}) date={date if date else 'n/a'}")
        if fail and fail != "nan":
            print(f"   Warning: {a}_fail: {fail}")

        if status == "⚠️欠損":
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
    sys.exit(main())
