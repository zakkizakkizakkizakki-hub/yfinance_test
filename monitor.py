# save as: monitor.py
from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd


# =========================
# Config
# =========================
CSV_PATH = os.environ.get("MARKET_CSV", "market_yfinance_log.csv")

ASSETS = ["USDJPY", "BTC", "Gold", "US10Y", "Oil", "VIX"]

# 値がこの範囲を大きく外れたら「警告」だけ出す（※標準では落とさない）
PLAUSIBLE_RANGES: Dict[str, Tuple[float, float]] = {
    "USDJPY": (50, 300),
    "BTC": (1000, 1_000_000),
    "Gold": (200, 20_000),   # どの系列を取るかで桁が変わり得るので広め
    "US10Y": (0.0, 20.0),    # %表記
    "Oil": (0.1, 500),
    "VIX": (0.1, 200),
}

# 前回比が大きすぎる場合の「警告」閾値（例：BTCは20%超の変動は一旦警告）
MAX_PCT_JUMP_WARN: Dict[str, float] = {
    "USDJPY": 0.05,
    "BTC": 0.20,
    "Gold": 0.10,
    "US10Y": 0.20,
    "Oil": 0.30,
    "VIX": 1.00,  # VIXは跳ねるので緩め
}

# これを 1 にすると「異常値警告」でも落とす（任意）
FAIL_ON_ANOMALY = os.environ.get("MONITOR_FAIL_ON_ANOMALY", "0") == "1"


# =========================
# Helpers
# =========================
def _as_float(x) -> float:
    try:
        v = float(x)
        if pd.isna(v):
            return 0.0
        return v
    except Exception:
        return 0.0


def _as_int(x) -> int:
    try:
        if pd.isna(x):
            return 0
        return int(x)
    except Exception:
        return 0


def _read_csv_safely(path: Path) -> pd.DataFrame:
    if not path.exists() or path.stat().st_size == 0:
        return pd.DataFrame()
    # まず標準で読む → だめなら python engine で救う
    try:
        return pd.read_csv(path, encoding="utf-8-sig")
    except Exception:
        try:
            return pd.read_csv(path, encoding="utf-8-sig", engine="python", on_bad_lines="skip")
        except Exception:
            return pd.DataFrame()


def _get_latest_rows(df: pd.DataFrame) -> Tuple[pd.Series, pd.Series | None]:
    # 最新1行と、ひとつ前（あれば）
    latest = df.iloc[-1]
    prev = df.iloc[-2] if len(df) >= 2 else None
    return latest, prev


# =========================
# Main
# =========================
def main() -> None:
    path = Path(CSV_PATH)
    df = _read_csv_safely(path)

    if df.empty:
        print("============================================================")
        print("❌ monitor: CSVが読めません（ファイルが無い/空/壊れている）")
        print(f"CSV_PATH: {path.resolve()}")
        print("============================================================")
        raise SystemExit(1)

    latest, prev = _get_latest_rows(df)

    ts = str(latest.get("timestamp_jst", "Unknown"))

    missing: List[str] = []
    warnings: List[str] = []

    print("\n============================================================")
    print("📡 Market Monitor Report")
    print("============================================================")
    print(f"[ latest timestamp_jst ] {ts}")
    print("\n[ Assets ]")

    for a in ASSETS:
        v = _as_float(latest.get(a, 0.0))
        ok = _as_int(latest.get(f"{a}_ok", 0))
        d = latest.get(f"{a}_date", "")
        fail = latest.get(f"{a}_fail", "")

        # 欠損判定（あなたの設計：欠損したら落とす）
        # - ok==1 かつ v>0 を最低条件にする
        is_missing = not (ok == 1 and v > 0)
        if is_missing:
            missing.append(a)

        status = "✅正常" if not is_missing else "⚠️欠損"
        print(f"  - {a:5s}: {v:12.6f} ({status})  date={d}  fail={fail}")

        # 警告1: 値レンジのざっくりチェック（落とさない）
        lo, hi = PLAUSIBLE_RANGES.get(a, (None, None))
        if not is_missing and lo is not None and hi is not None:
            if not (lo <= v <= hi):
                warnings.append(f"{a}: 値が想定レンジ外っぽい ({v} not in [{lo},{hi}])")

        # 警告2: 前回比の急変（落とさない）
        if prev is not None and not is_missing:
            pv = _as_float(prev.get(a, 0.0))
            if pv > 0:
                pct = abs(v - pv) / pv
                thr = MAX_PCT_JUMP_WARN.get(a)
                if thr is not None and pct > thr:
                    warnings.append(f"{a}: 前回比の変動が大きい ({pct*100:.1f}% > {thr*100:.1f}%)")

    if warnings:
        print("\n[ warnings ]")
        for w in warnings:
            print(f"  - {w}")

    if missing:
        print("\n!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        print(f"❌ 欠損を検知: {', '.join(missing)}")
        print("   → 監視のため exit code 1 で終了します。")
        print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n")
        raise SystemExit(1)

    if FAIL_ON_ANOMALY and warnings:
        print("\n!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        print("❌ 異常値警告を検知（MONITOR_FAIL_ON_ANOMALY=1 のため失敗扱い）")
        print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n")
        raise SystemExit(1)

    print("\n✅ monitor: 欠損なし（正常終了）\n")


if __name__ == "__main__":
    main()
