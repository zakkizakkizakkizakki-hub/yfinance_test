# save as: monitor.py
from __future__ import annotations

import math
from pathlib import Path
import pandas as pd

CSV_PATH = Path("market_yfinance_log.csv")
ENC = "utf-8-sig"

ASSETS = ["USDJPY", "BTC", "Gold", "US10Y", "Oil", "VIX"]


def _bad(v) -> bool:
    try:
        x = float(v)
    except Exception:
        return True
    return (not math.isfinite(x)) or (x <= 0.0)


def main() -> int:
    print("\n" + "=" * 60)
    print("📡 Market Monitor")
    print("=" * 60)

    if not CSV_PATH.exists() or CSV_PATH.stat().st_size == 0:
        print(f"❌ CSVが存在しない/空です: {CSV_PATH}")
        return 1

    try:
        df = pd.read_csv(CSV_PATH, encoding=ENC)
    except Exception as e:
        print("❌ CSVの読み取りに失敗しました（CSVが壊れている可能性）")
        print(f"原因: {type(e).__name__}: {e}")
        return 1

    if df.empty:
        print("❌ CSVは読めましたが中身が空です")
        return 1

    last = df.iloc[-1].to_dict()
    ts = str(last.get("timestamp_jst", "Unknown"))
    print(f"[ Latest ] {ts}")

    missing = []
    for a in ASSETS:
        v = last.get(a, 0.0)
        ok = int(last.get(f"{a}_ok", 0) or 0)
        date = last.get(f"{a}_date", "")
        fail = last.get(f"{a}_fail", "")

        status = "✅正常"
        if ok != 1 or _bad(v):
            status = "⚠️欠損"
            missing.append(a)

        # 表示（初心者向けに “fail理由”も出す）
        try:
            fv = float(v)
        except Exception:
            fv = v

        print(f" - {a:<5}: {fv:>12} ({status}) date={date if date else 'nan'}")
        if fail:
            print(f"   Warning: {a}_fail: {fail}")

    if missing:
        print("\n" + "!" * 60)
        print(f"❌ 欠損を検知: {', '.join(missing)}")
        print("   → 監視仕様により exit code 1 で終了します。")
        print("!" * 60)
        return 1

    print("\n✅ すべて正常です（欠損なし）")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
