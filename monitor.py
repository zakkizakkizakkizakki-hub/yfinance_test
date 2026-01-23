# save as: monitor.py
from __future__ import annotations

import os
import sys
from datetime import datetime

import pandas as pd

CSV_PATH = "market_yfinance_log.csv"
REPORT_PATH = "monitor_report.txt"

ASSETS = ["USDJPY", "BTC", "Gold", "US10Y", "Oil", "VIX"]

def main() -> int:
    lines = []
    lines.append("")
    lines.append("============================================================")
    lines.append("📡 Market Monitor")
    lines.append("============================================================")

    if not os.path.exists(CSV_PATH) or os.path.getsize(CSV_PATH) == 0:
        lines.append(f"[ERROR] {CSV_PATH} not found or empty.")
        print("\n".join(lines))
        with open(REPORT_PATH, "w", encoding="utf-8") as f:
            f.write("\n".join(lines))
        return 1

    try:
        df = pd.read_csv(CSV_PATH, encoding="utf-8-sig")
    except Exception as e:
        lines.append(f"[ERROR] CSV parse failed: {type(e).__name__}: {e}")
        print("\n".join(lines))
        with open(REPORT_PATH, "w", encoding="utf-8") as f:
            f.write("\n".join(lines))
        return 1

    if df.empty:
        lines.append("[ERROR] CSV has no rows.")
        print("\n".join(lines))
        with open(REPORT_PATH, "w", encoding="utf-8") as f:
            f.write("\n".join(lines))
        return 1

    last = df.iloc[-1].to_dict()
    run_id = str(last.get("run_id", ""))
    ts = str(last.get("timestamp_jst", ""))

    lines.append(f"[ Latest ] {ts}")
    lines.append(f"[ run_id ] {run_id}")

    missing_assets = []

    for a in ASSETS:
        miss = last.get(f"{a}_missing", 1)
        fail = str(last.get(f"{a}_fail", ensuring_str("")))
        date = str(last.get(f"{a}_date", ""))
        src = str(last.get(f"{a}_source", ""))

        # 数値は壊れてても監視判断は missing フラグで行う（ValueError事故防止）
        val = last.get(a, "")
        try:
            v = float(val) if val not in ("", None) else 0.0
        except Exception:
            v = 0.0

        if int(miss) == 0:
            lines.append(f" - {a:5s}: {v:12.6f} (✅正常) date={date} src={src}")
        else:
            lines.append(f" - {a:5s}: {v:12.6f} (⚠️欠損) date={date} src={src}")
            lines.append(f"   Warning: {a}_fail: {fail}")
            missing_assets.append(a)

    if missing_assets:
        lines.append("")
        lines.append("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        lines.append(f"❌ 欠損を検知: {', '.join(missing_assets)}")
        lines.append("   → 監視仕様により exit code 1 で終了します。")
        lines.append("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")

        out = "\n".join(lines)
        print(out)
        with open(REPORT_PATH, "w", encoding="utf-8") as f:
            f.write(out)
        return 1

    out = "\n".join(lines)
    print(out)
    with open(REPORT_PATH, "w", encoding="utf-8") as f:
        f.write(out)
    return 0

def ensuring_str(x) -> str:
    return "" if x is None else str(x)

if __name__ == "__main__":
    raise SystemExit(main())
