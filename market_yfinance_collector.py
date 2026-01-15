# save as: market_collector.py
from __future__ import annotations

import os
import time
import random
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Dict, Tuple, Optional, List

import pandas as pd
import yfinance as yf

# =========================
# Config
# =========================
JST = timezone(timedelta(hours=9))

OUT_CSV = "market_yfinance_log.csv"

# 既存仕様の「主列」は維持する（この順番・名前は壊さない）
ASSETS = {
    "USDJPY": "JPY=X",
    "BTC": "BTC-USD",
    "Gold": "GC=F",
    "US10Y": "^TNX",
    "Oil": "CL=F",
    "VIX": "^VIX",
}

# yfinance settings
YF_PERIOD = "7d"
YF_INTERVAL = "1d"

RETRIES = 4               # 再試行回数
BASE_DELAY_SEC = 3.0      # 初期待機
MAX_DELAY_SEC = 20.0      # 最大待機（指数バックオフの上限）

# 異常値検知（前回値に対する急変）
# - 0.30 = 30% 以上の変化を「異常の可能性」としてフラグ（ログに残すだけ、値は捨てない）
ANOMALY_PCT = {
    "USDJPY": 0.05,  # 為替は急変しにくいので5%でも警戒
    "BTC":    0.20,
    "Gold":   0.05,
    "US10Y":  0.20,
    "Oil":    0.20,
    "VIX":    0.40,
}

# =========================
# Utilities
# =========================
def now_jst() -> datetime:
    return datetime.now(JST)

def now_jst_str() -> str:
    return now_jst().strftime("%Y-%m-%d %H:%M:%S")

def _sleep_backoff(attempt: int) -> None:
    # attempt: 0,1,2...
    delay = min(MAX_DELAY_SEC, BASE_DELAY_SEC * (2 ** attempt))
    jitter = random.uniform(0.2, 1.2)
    time.sleep(delay + jitter)

def _file_has_data(path: str) -> bool:
    return os.path.exists(path) and os.path.getsize(path) > 0

def _safe_read_csv(path: str) -> pd.DataFrame:
    if not _file_has_data(path):
        return pd.DataFrame()
    try:
        return pd.read_csv(path, encoding="utf-8-sig")
    except Exception:
        # 多少壊れても落ちないように
        try:
            return pd.read_csv(path, encoding="utf-8-sig", engine="python", on_bad_lines="skip")
        except Exception:
            return pd.DataFrame()

def _last_good_value(df: pd.DataFrame, col: str) -> Optional[float]:
    """過去ログから、その列の最後の「0じゃない値」を拾う（異常値検知用）"""
    if df.empty or col not in df.columns:
        return None
    s = pd.to_numeric(df[col], errors="coerce")
    s = s[(s.notna()) & (s > 0)]
    if s.empty:
        return None
    return float(s.iloc[-1])

@dataclass
class FetchResult:
    value: float
    ok: int                # 1=OK, 0=missing
    source: str            # "yfinance" or "missing"
    date: str              # "YYYY-MM-DD" etc
    fail_reason: str       # "" if ok
    anomaly: int           # 1 if suspicious jump else 0
    anomaly_reason: str    # details

def _extract_last_close(df: pd.DataFrame) -> Tuple[Optional[float], str]:
    """yfinanceのdownload結果から最後の終値を取り出す"""
    if df is None or df.empty:
        return None, "EmptyDF"

    # Close列がMultiIndexになる場合があるので吸収
    if "Close" in df.columns:
        close = df["Close"]
        if isinstance(close, pd.DataFrame):
            close = close.iloc[:, 0]
        series = pd.to_numeric(close, errors="coerce").dropna()
    else:
        series = pd.to_numeric(df.iloc[:, 0], errors="coerce").dropna()

    if series.empty:
        return None, "NoCloseData"

    v = float(series.iloc[-1])
    if not (v > 0):
        return None, "NonPositive"

    # index から日付文字列
    d = str(series.index[-1])[:10]
    return v, d

def fetch_yfinance_once(ticker: str) -> Tuple[Optional[float], str, str]:
    """1回だけyfinanceを叩いて値と日付を返す"""
    try:
        df = yf.download(
            ticker,
            period=YF_PERIOD,
            interval=YF_INTERVAL,
            progress=False,
            threads=False,   # Actionsでの不安定回避（少し安定することがある）
        )
        v, d = _extract_last_close(df)
        if v is None:
            return None, "EmptyDF", ""
        return v, "", d
    except Exception as e:
        return None, type(e).__name__, ""

def fetch_with_retries(name: str, ticker: str, prev_value: Optional[float]) -> FetchResult:
    last_err = ""
    last_date = ""

    for attempt in range(RETRIES):
        v, err, d = fetch_yfinance_once(ticker)
        last_err = err
        last_date = d

        if v is None:
            # リトライ
            _sleep_backoff(attempt)
            continue

        # OK扱い
        value = float(v)

        # 異常値検知（前回比）
        anomaly = 0
        anomaly_reason = ""
        if prev_value is not None and prev_value > 0:
            pct = abs(value - prev_value) / prev_value
            th = ANOMALY_PCT.get(name, 0.30)
            if pct >= th:
                anomaly = 1
                anomaly_reason = f"jump_pct={pct:.3f}>=th={th:.3f} (prev={prev_value}, now={value})"

        return FetchResult(
            value=value,
            ok=1,
            source="yfinance",
            date=last_date,
            fail_reason="",
            anomaly=anomaly,
            anomaly_reason=anomaly_reason,
        )

    # 全滅
    return FetchResult(
        value=0.0,
        ok=0,
        source="missing",
        date=last_date or "",
        fail_reason=last_err or "Unknown",
        anomaly=0,
        anomaly_reason="",
    )

def collect() -> None:
    print("=== yfinance market fetch ===")
    print(f"timestamp_jst: {now_jst_str()}")

    df_hist = _safe_read_csv(OUT_CSV)

    # 既存主列（仕様維持）
    row: Dict[str, object] = {"timestamp_jst": now_jst_str()}

    # 追加の理由ログ/検知ログ列（主列は壊さずに「追加」）
    # - 例: USDJPY_ok / USDJPY_fail / USDJPY_date / USDJPY_anomaly / USDJPY_anomaly_reason
    extra: Dict[str, object] = {}

    for name, ticker in ASSETS.items():
        prev = _last_good_value(df_hist, name)
        r = fetch_with_retries(name, ticker, prev)

        # 主列（既存仕様）
        row[name] = float(r.value)

        # 追加列
        extra[f"{name}_ok"] = int(r.ok)
        extra[f"{name}_source"] = r.source
        extra[f"{name}_date"] = r.date
        extra[f"{name}_fail"] = r.fail_reason
        extra[f"{name}_anomaly"] = int(r.anomaly)
        extra[f"{name}_anomaly_reason"] = r.anomaly_reason

        mark = "✅" if r.ok else "❌"
        print(f"[{r.source}] {name}({ticker}): {r.value} ({r.date}) {mark} fail={r.fail_reason}")

        if r.anomaly:
            print(f"  [warn] anomaly: {name} {r.anomaly_reason}")

    out_row = {**row, **extra}
    out_df = pd.DataFrame([out_row])

    # 空ファイルならヘッダを出す
    header = not _file_has_data(OUT_CSV)

    out_df.to_csv(
        OUT_CSV,
        mode="a",
        index=False,
        header=header,
        encoding="utf-8-sig",
        line_terminator="\n",
    )

    print(f"=== saved -> {OUT_CSV} ===")

if __name__ == "__main__":
    collect()
