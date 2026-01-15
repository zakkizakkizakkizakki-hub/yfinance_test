# save as: monitor.py
from __future__ import annotations

import os
import math
import pandas as pd
from typing import Dict, List, Tuple

CSV_PATH = "market_yfinance_log.csv"

ASSETS = ["USDJPY", "BTC", "Gold", "US10Y", "Oil", "VIX"]

# 異常値検知（ざっくり安全側：必要なら後で調整）
MAX_ABS_PCT_CHANGE: Dict[str, float] = {
    "USDJPY": 0.05,  # 5%
    "US10Y":  0.20,  # 20%
    "Gold":  0.15,
    "Oil":   0.25,
    "VIX":   1.00,   # VIXは跳ねる
    "BTC":   0.40,   # BTCは跳ねる
}

def _to_float(x) -> float:
    try:
        v = float(x)
        if not math.isfinite(v):
            return float("nan")
        return v
    except Exception:
        return float("nan")

def _latest_non_missing(df: pd.DataFrame, asset: str) -> Tuple[float, str]:
    """
    Returns (value, timestamp_jst) for the latest row where asset_missing == 0 and value > 0
    If not found, returns (nan, "")
    """
    miss_col = f"{asset}_missing"
    if miss_col not in df.columns or asset not in df.columns:
        return float("nan"), ""
    d = df.copy()
    d[asset] = pd.to_numeric(d[asset], errors="coerce")
    d[miss_col] = pd.to_numeric(d[miss_col], errors="coerce").fillna(1).astype(int)
    d = d[(d[miss_col] == 0) & (d[asset].notna()) & (d[asset] > 0)]
    if d.empty:
        return float("nan"), ""
    r = d.iloc[-1]
    return float(r[asset]), str(r.get("timestamp_jst", ""))

def main() -> int:
    print("\n" + "=" * 60)
    print("📡 Market Monitor")
    print("=" * 60)

    if not os.path.exists(CSV_PATH):
        print(f"❌ CSVが見つかりません: {CSV_PATH}")
        return 1

    df = pd.read_csv(CSV_PATH, encoding="utf-8-sig")
    if df.empty:
        print("❌ CSVが空です")
        return 1

    last = df.iloc[-1]
    ts = str(last.get("timestamp_jst", "Unknown"))
    print(f"[ Latest ] {ts}")

    missing_assets: List[str] = []
    abnormal_assets: List[str] = []

    for a in ASSETS:
        v = _to_float(last.get(a))
        miss = int(_to_float(last.get(f"{a}_missing")) or 0)
        date = str(last.get(f"{a}_date", ""))
        fail = str(last.get(f"{a}_fail", ""))

        is_missing = (miss == 1) or (not math.isfinite(v)) or (v <= 0)
        status = "✅正常" if not is_missing else "⚠️欠損"

        print(f" - {a:5s}: {v:12.6f} ({status}) date={date if date else 'nan'}")
        if fail:
            print(f"   Warning: {a}_fail: {fail}")

        if is_missing:
            missing_assets.append(a)
            continue

        # 異常値検知：前回の「正常値」と比較
        prev_v, prev_ts = _latest_non_missing(df.iloc[:-1], a) if len(df) >= 2 else (float("nan"), "")
        if math.isfinite(prev_v) and prev_v > 0:
            pct = abs(v / prev_v - 1.0)
            if pct > MAX_ABS_PCT_CHANGE.get(a, 0.50):
                abnormal_assets.append(a)
                print(f"   ❗ Abnormal: prev={prev_v:.6f} at {prev_ts}  change={pct*100:.1f}%")

    if missing_assets:
        print("\n" + "!" * 60)
        print(f"❌ 欠損を検知: {', '.join(missing_assets)}")
        print("   → 監視仕様により exit code 1 で終了します。")
        print("!" * 60)
        return 1

    if abnormal_assets:
        print("\n" + "!" * 60)
        print(f"❌ 異常値を検知: {', '.join(abnormal_assets)}")
        print("   → 監視仕様により exit code 1 で終了します。")
        print("!" * 60)
        return 1

    print("\n✅ OK: 欠損なし / 異常値なし")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
