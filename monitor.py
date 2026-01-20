# save as: monitor.py
from __future__ import annotations

import csv
import os
from datetime import datetime
from typing import List, Dict

import pandas as pd

CSV_PATH = "market_yfinance_log.csv"
CSV_ENCODING = "utf-8-sig"

ASSETS = ["USDJPY", "BTC", "Gold", "US10Y", "Oil", "VIX"]

def _expected_cols() -> List[str]:
    cols = ["run_id", "timestamp_jst"]
    for a in ASSETS:
        cols += [a, f"{a}_missing", f"{a}_source", f"{a}_date", f"{a}_fail"]
    return cols

EXPECTED_COLS = _expected_cols()

def main() -> int:
    print("\n" + "=" * 60)
    print("📡 Market Monitor")
    print("=" * 60)

    if not os.path.exists(CSV_PATH) or os.path.getsize(CSV_PATH) == 0:
        print(f"❌ {CSV_PATH} が存在しない/空です（collectorが未生成の可能性）")
        return 1

    # まずCSVを読む（列ズレなら即わかる）
    try:
        df = pd.read_csv(CSV_PATH, encoding=CSV_ENCODING)
    except Exception as e:
        print(f"❌ CSVの読み取りに失敗: {type(e).__name__}: {e}")
        return 1

    if df.empty:
        print("❌ CSVは読めたが、中身が空です")
        return 1

    # 列仕様チェック（事故防止）
    if list(df.columns) != EXPECTED_COLS:
        print("❌ CSV列仕様が想定と違います（列ズレ事故）")
        print(f" - expected: {len(EXPECTED_COLS)} cols")
        print(f" - actual  : {len(df.columns)} cols")
        print(" - actual columns:")
        for c in df.columns.tolist():
            print(f"   - {c}")
        return 1

    # 最新行（末尾）を監視対象にする
    last = df.iloc[-1].to_dict()
    run_id = str(last.get("run_id", ""))
    ts = str(last.get("timestamp_jst", ""))

    print(f"[ Latest ] {ts}")
    print(f"[ run_id ] {run_id}")

    missing_assets: List[str] = []

    for a in ASSETS:
        # 値
        raw_v = last.get(a, None)
        # 欠損フラグ
        raw_miss = last.get(f"{a}_missing", 1)
        # fail理由
        fail_reason = str(last.get(f"{a}_fail", ""))

        # 文字列等が混ざっても落ちないように安全化
        v = pd.to_numeric(pd.Series([raw_v]), errors="coerce").iloc[0]
        miss = pd.to_numeric(pd.Series([raw_miss]), errors="coerce").iloc[0]
        miss = int(miss) if pd.notna(miss) else 1

        is_missing = (miss == 1) or (pd.isna(v)) or (float(v) <= 0.0)

        mark = "✅正常" if not is_missing else "⚠️欠損"
        date = str(last.get(f"{a}_date", ""))
        src = str(last.get(f"{a}_source", ""))

        vv = float(v) if pd.notna(v) else float("nan")
        print(f" - {a:5s}: {vv:12.6f} ({mark})  src={src}  date={date}")
        if is_missing:
            print(f"   Warning: {a}_fail: {fail_reason}")
            missing_assets.append(a)

    if missing_assets:
        print("\n" + "!" * 60)
        print(f"❌ 欠損を検知: {', '.join(missing_assets)}")
        print("   → 監視仕様により exit code 1 で終了します。")
        print("!" * 60)
        return 1

    print("\n✅ すべて正常です（exit 0）")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
