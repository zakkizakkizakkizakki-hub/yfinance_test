# save as: monitor.py
from __future__ import annotations

import math
import os
from typing import List, Tuple

import pandas as pd

CSV_PATH = "market_yfinance_log.csv"
ENCODING = "utf-8-sig"

ASSET_NAMES = ["USDJPY", "BTC", "Gold", "US10Y", "Oil", "VIX"]

def _to_float(x) -> float:
    """
    数字に変換できない（例: 'EmptyDF'）場合は NaN にする
    """
    try:
        if x is None:
            return float("nan")
        if isinstance(x, str) and x.strip() == "":
            return float("nan")
        return float(x)
    except Exception:
        return float("nan")

def _is_missing(value: float, missing_flag) -> bool:
    if missing_flag is None:
        return True
    try:
        m = int(missing_flag)
    except Exception:
        m = 1
    if m == 1:
        return True
    if not (value > 0) or math.isnan(value):
        return True
    return False

def main() -> int:
    print("\n============================================================")
    print("📡 Market Monitor")
    print("============================================================")

    if not os.path.exists(CSV_PATH) or os.path.getsize(CSV_PATH) == 0:
        print(f"❌ {CSV_PATH} が存在しない/空です。")
        return 1

    df = pd.read_csv(CSV_PATH, encoding=ENCODING)
    if df.empty:
        print(f"❌ {CSV_PATH} が空です。")
        return 1

    last = df.iloc[-1].to_dict()

    run_id = str(last.get("run_id", ""))
    ts = str(last.get("timestamp_jst", ""))

    print(f"[ Latest ] {ts}")
    if run_id:
        print(f"[ run_id ] {run_id}")

    missing_assets: List[str] = []

    for a in ASSET_NAMES:
        v = _to_float(last.get(a))
        miss_flag = last.get(f"{a}_missing")
        src = str(last.get(f"{a}_source", ""))
        date = str(last.get(f"{a}_date", ""))
        fail = str(last.get(f"{a}_fail", ""))

        miss = _is_missing(v, miss_flag)

        mark = "✅正常" if not miss else "⚠️欠損"
        v_disp = "nan" if math.isnan(v) else f"{v:.6f}"
        print(f" - {a:5s}: {v_disp:>12s} ({mark}) src={src} date={date}")
        if fail:
            print(f"   Warning: {a}_fail: {fail}")

        if miss:
            missing_assets.append(a)

    if missing_assets:
        print("\n!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        print(f"❌ 欠損を検知: {', '.join(missing_assets)}")
        print("   → 監視仕様により exit code 1 で終了します。")
        print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        return 1

    print("\n✅ 全資産取得OK（欠損なし）")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
