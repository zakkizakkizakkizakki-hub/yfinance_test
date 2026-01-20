# save as: monitor.py
from __future__ import annotations

import os
import sys
import pandas as pd

CSV_PATH = os.getenv("MARKET_CSV", "market_yfinance_log.csv")
ENC = "utf-8-sig"

ASSETS = ["USDJPY", "BTC", "Gold", "US10Y", "Oil", "VIX"]

def _safe_read_csv(path: str) -> pd.DataFrame:
    # まず通常で試す → だめなら python engine + bad line skip
    try:
        return pd.read_csv(path, encoding=ENC)
    except Exception:
        return pd.read_csv(path, encoding=ENC, engine="python", on_bad_lines="skip")

def main() -> int:
    print("\n" + "=" * 60)
    print("📡 Market Monitor")
    print("=" * 60)

    if not os.path.exists(CSV_PATH) or os.path.getsize(CSV_PATH) == 0:
        print(f"❌ CSVが存在しない or 空です: {CSV_PATH}")
        return 1

    try:
        df = _safe_read_csv(CSV_PATH)
    except Exception as e:
        print(f"❌ CSVの読み取りに失敗しました: {type(e).__name__}: {e}")
        return 1

    if df.empty:
        print("❌ CSVは読み取れましたが、データ行がありません（空）")
        return 1

    last = df.iloc[-1].to_dict()
    ts = str(last.get("timestamp_jst", "Unknown"))
    print(f"[ Latest ] {ts}")

    missing_assets = []
    for a in ASSETS:
        miss_key = f"{a}_missing"
        fail_key = f"{a}_fail"
        date_key = f"{a}_date"

        # 値は表示用（変な文字が来ても監視は missing フラグで判断）
        val_raw = last.get(a, "")
        try:
            val = float(val_raw) if val_raw not in ("", None) else 0.0
        except Exception:
            val = 0.0

        miss_raw = last.get(miss_key, 1)
        try:
            miss = int(miss_raw)
        except Exception:
            miss = 1

        date = str(last.get(date_key, ""))
        fail = str(last.get(fail_key, ""))

        mark = "✅正常" if miss == 0 else "⚠️欠損"
        print(f" - {a:<5}: {val:>12.6f} ({mark}) date={date if date else 'nan'}")
        if fail and fail != "nan":
            print(f"   Warning: {a}_fail: {fail}")

        if miss != 0:
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
