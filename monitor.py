# save as: monitor.py
from __future__ import annotations

import os
import sys
import csv
from typing import List

import pandas as pd

CSV_PATH = "market_yfinance_log.csv"
ENCODING = "utf-8-sig"
ASSETS = ["USDJPY", "BTC", "Gold", "US10Y", "Oil", "VIX"]


def _read_csv_safely(path: str) -> pd.DataFrame:
    """
    CSVが途中で壊れていても、監視が「何が起きたか」を表示して落ちるための読み方。
    - on_bad_lines='skip' で読み飛ばし、最後に残った行で判定する
    """
    if not os.path.exists(path) or os.path.getsize(path) == 0:
        return pd.DataFrame()
    try:
        return pd.read_csv(path, encoding=ENCODING)
    except Exception:
        # 壊れた行が混ざってる可能性 → skip で読めるだけ読む
        return pd.read_csv(path, encoding=ENCODING, engine="python", on_bad_lines="skip")


def main() -> int:
    print("\n" + "=" * 60)
    print("📡 Market Monitor")
    print("=" * 60)

    df = _read_csv_safely(CSV_PATH)
    if df.empty:
        print(f"[ERROR] {CSV_PATH} が空、または読み取れません。")
        return 1

    last = df.iloc[-1].to_dict()
    ts = str(last.get("timestamp_jst", "Unknown"))
    print(f"[ Latest ] {ts}")

    missing_assets: List[str] = []

    for a in ASSETS:
        miss_col = f"{a}_missing"
        fail_col = f"{a}_fail"
        date_col = f"{a}_date"

        # 値は壊れてる可能性があるので慎重に数値化
        raw_v = last.get(a, 0.0)
        v = pd.to_numeric(pd.Series([raw_v]), errors="coerce").iloc[0]
        v = float(v) if pd.notna(v) else 0.0

        miss_raw = last.get(miss_col, 1)
        miss = pd.to_numeric(pd.Series([miss_raw]), errors="coerce").iloc[0]
        miss = int(miss) if pd.notna(miss) else 1

        fail = str(last.get(fail_col, "") or "").strip()
        date = str(last.get(date_col, "") or "").strip()

        is_missing = (miss == 1) or (v <= 0.0)

        status = "✅正常" if not is_missing else "⚠️欠損"
        print(f" - {a:5s}: {v:12.6f} ({status}) date={date or 'nan'}")
        if fail:
            print(f"   Warning: {a}_fail: {fail}")

        if is_missing:
            missing_assets.append(a)

    if missing_assets:
        print("\n" + "!" * 60)
        print(f"❌ 欠損を検知: {', '.join(missing_assets)}")
        print("   → 監視仕様により exit code 1 で終了します。")
        print("!" * 60)
        return 1

    print("\n[OK] 欠損なし。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
