# save as: monitor.py
from __future__ import annotations

import os
import sys
from datetime import datetime

import pandas as pd

CSV_PATH = "market_yfinance_log.csv"
ENC = "utf-8-sig"

ASSETS = ["USDJPY", "BTC", "Gold", "US10Y", "Oil", "VIX"]

def _to_float(x) -> float | None:
    try:
        if pd.isna(x):
            return None
        # 文字列 "EmptyDF" 等は float 変換で落ちるのでここで弾く
        return float(x)
    except Exception:
        return None

def main() -> int:
    print("\n" + "=" * 60)
    print("📡 Market Monitor")
    print("=" * 60)

    if not os.path.exists(CSV_PATH) or os.path.getsize(CSV_PATH) == 0:
        print(f"❌ {CSV_PATH} が存在しない/空です（collectorが生成できていません）")
        return 1

    # 以前の列ズレ事故を想定して on_bad_lines="skip"
    df = pd.read_csv(CSV_PATH, encoding=ENC, on_bad_lines="skip")
    if df.empty:
        print(f"❌ {CSV_PATH} が読み込めましたが中身が空です（破損または全行スキップ）")
        return 1

    last = df.iloc[-1].to_dict()

    run_id = str(last.get("run_id", ""))
    ts = str(last.get("timestamp_jst", ""))

    print(f"[ Latest ] {ts}")
    if run_id:
        print(f"[ run_id ] {run_id}")

    missing_assets = []

    for a in ASSETS:
        v_raw = last.get(a, None)
        m_raw = last.get(f"{a}_missing", None)
        date = str(last.get(f"{a}_date", ""))
        fail = str(last.get(f"{a}_fail", ""))

        v = _to_float(v_raw)
        m = _to_float(m_raw)

        is_missing = False
        if m is None:
            is_missing = True
        else:
            is_missing = (int(m) == 1)

        # 値が数値でない/0以下も欠損扱い（監視として安全側）
        if v is None or not (v > 0):
            is_missing = True

        mark = "✅正常" if not is_missing else "⚠️欠損"
        v_disp = "nan" if v is None else f"{v:.6f}"
        print(f" - {a:<5}: {v_disp:>12} ({mark}) date={date or 'nan'}")
        if is_missing:
            print(f"   Warning: {a}_fail: {fail or 'Unknown'}")

        if is_missing:
            missing_assets.append(a)

    if missing_assets:
        print("\n" + "!" * 60)
        print(f"❌ 欠損を検知: {', '.join(missing_assets)}")
        print("   → 監視仕様により exit code 1 で終了します。")
        print("!" * 60)
        return 1

    print("\n✅ 欠損なし。monitorは正常終了します。")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
