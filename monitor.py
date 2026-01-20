# save as: monitor.py
from __future__ import annotations

import sys
import pandas as pd

CSV_PATH = "market_yfinance_log.csv"
ENCODING = "utf-8-sig"

ASSETS = ["USDJPY", "BTC", "Gold", "US10Y", "Oil", "VIX"]

def _to_float(x):
    try:
        return float(x)
    except Exception:
        return float("nan")

def _to_int(x, default=0) -> int:
    try:
        v = int(float(x))
        return v
    except Exception:
        return default

def main() -> int:
    print("\n" + "=" * 60)
    print("📡 Market Monitor")
    print("=" * 60)

    try:
        df = pd.read_csv(CSV_PATH, encoding=ENCODING, engine="python")
    except Exception as e:
        print(f"❌ CSVを読み取れません: {type(e).__name__}: {e}")
        print("   → 監視仕様として異常扱い（exit 1）にします。")
        return 1

    if df.empty:
        print("❌ CSVが空です（監視不能） → exit 1")
        return 1

    last = df.iloc[-1].to_dict()
    run_id = str(last.get("run_id", "Unknown"))
    ts = str(last.get("timestamp_jst", "Unknown"))

    print(f"[ Latest ] {ts}")
    print(f"[ run_id ] {run_id}")

    missing_assets = []

    for a in ASSETS:
        v = _to_float(last.get(a))
        miss = _to_int(last.get(f"{a}_missing", 1), default=1)
        date = str(last.get(f"{a}_date", ""))
        fail = str(last.get(f"{a}_fail", ""))

        # 欠損判定（flag優先 + 値の安全チェック）
        is_missing = (miss == 1) or not (v == v) or not (v > 0)
        status = "✅正常" if not is_missing else "⚠️欠損"

        print(f" - {a:5s}: {v:12.6f} ({status}) date={date if date and date != 'nan' else 'n/a'}")
        if fail and fail != "nan":
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
    sys.exit(main())
