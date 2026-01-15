# market_yfinance_collector.py
from __future__ import annotations

import os
import time
import random
from datetime import datetime, timedelta, timezone
from typing import Dict, Tuple, Optional

import pandas as pd
import yfinance as yf

# =========================
# Config
# =========================
JST = timezone(timedelta(hours=9))

OUT_CSV = os.environ.get("OUT_CSV", "market_yfinance_log.csv")

ASSETS = {
    "USDJPY": "JPY=X",
    "BTC": "BTC-USD",
    "Gold": "GC=F",
    "US10Y": "^TNX",
    "Oil": "CL=F",
    "VIX": "^VIX",
}

YF_PERIOD = "7d"
YF_INTERVAL = "1d"

# リトライ設定（GitHub側で詰まりやすいので長め）
MAX_ROUNDS = 3
BASE_SLEEP = 6.0  # seconds


def now_jst() -> datetime:
    return datetime.now(JST)


def now_jst_str() -> str:
    return now_jst().strftime("%Y-%m-%d %H:%M:%S")


def _sleep_backoff(round_idx: int) -> None:
    # round_idx: 0,1,2...
    base = BASE_SLEEP * (2 ** round_idx)
    jitter = random.uniform(0.5, 2.0)
    time.sleep(base + jitter)


def _safe_float(x) -> Optional[float]:
    try:
        v = float(x)
        if v != v:  # NaN
            return None
        return v
    except Exception:
        return None


def _pick_last_close(df: pd.DataFrame, ticker: str) -> Tuple[Optional[float], str]:
    """
    df: yf.download(tickers=..., group_by='ticker') 結果 or 単一ティッカー結果
    return: (price, date_str)
    """
    if df is None or df.empty:
        return None, ""

    # 1) マルチティッカー形式（列が MultiIndex になることが多い）
    # 例: columns = MultiIndex([('Close','JPY=X'), ...]) or ('JPY=X','Close') など揺れる
    try:
        if isinstance(df.columns, pd.MultiIndex):
            # パターンA: (field, ticker)
            if ("Close", ticker) in df.columns:
                s = df[("Close", ticker)]
            # パターンB: (ticker, field)
            elif (ticker, "Close") in df.columns:
                s = df[(ticker, "Close")]
            else:
                # 代替：Closeっぽい列を探す
                s = None
                for c in df.columns:
                    if len(c) == 2 and c[0] == "Close" and c[1] == ticker:
                        s = df[c]
                        break
                if s is None:
                    return None, ""
            s = pd.to_numeric(s, errors="coerce").dropna()
            if s.empty:
                return None, ""
            price = _safe_float(s.iloc[-1])
            date_str = str(s.index[-1])[:10]
            return price, date_str
    except Exception:
        pass

    # 2) 単一ティッカー形式（columnsにCloseがある）
    try:
        if "Close" in df.columns:
            s = pd.to_numeric(df["Close"], errors="coerce").dropna()
            if s.empty:
                return None, ""
            price = _safe_float(s.iloc[-1])
            date_str = str(s.index[-1])[:10]
            return price, date_str
    except Exception:
        pass

    return None, ""


def _ensure_csv_header(path: str, columns: list[str]) -> None:
    if os.path.exists(path) and os.path.getsize(path) > 0:
        return
    pd.DataFrame([], columns=columns).to_csv(path, index=False, encoding="utf-8-sig")


def fetch_all_with_yfinance() -> Tuple[Dict[str, float], Dict[str, str], Dict[str, str]]:
    """
    return:
      prices[name] float
      dates[name]  str
      fails[name]  str  ("" if ok)
    """
    names = list(ASSETS.keys())
    tickers = [ASSETS[n] for n in names]
    tickers_str = " ".join(tickers)

    prices: Dict[str, float] = {n: 0.0 for n in names}
    dates: Dict[str, str] = {n: "" for n in names}
    fails: Dict[str, str] = {n: "" for n in names}

    last_err = ""

    # まず「まとめて」取得（呼び出し回数が最小なので安定しやすい）
    for r in range(MAX_ROUNDS):
        try:
            if r > 0:
                _sleep_backoff(r - 1)
            df = yf.download(
                tickers=tickers_str,
                period=YF_PERIOD,
                interval=YF_INTERVAL,
                group_by="column",
                progress=False,
                threads=False,  # 並列を切って挙動を安定化
            )
            # まとめ取得に成功しても、個別に欠損が出ることがあるので tickerごとに拾う
            any_ok = False
            for n in names:
                t = ASSETS[n]
                p, d = _pick_last_close(df, t)
                if p is not None and p > 0:
                    prices[n] = float(p)
                    dates[n] = d
                    fails[n] = ""
                    any_ok = True
                else:
                    # 一旦未確定。後で個別フォールバック
                    fails[n] = "EmptyDF_or_NoClose"
            if any_ok:
                break
        except Exception as e:
            last_err = f"{type(e).__name__}: {e}"
            # 次ラウンドへ

    # まとめ取得で取れなかったものだけ「個別」フォールバック（ここでレート制限だと厳しいが、ログは残る）
    for n in names:
        if prices[n] > 0:
            continue

        t = ASSETS[n]
        indiv_err = ""
        for r in range(MAX_ROUNDS):
            try:
                if r > 0:
                    _sleep_backoff(r - 1)
                df = yf.download(
                    tickers=t,
                    period=YF_PERIOD,
                    interval=YF_INTERVAL,
                    progress=False,
                    threads=False,
                )
                p, d = _pick_last_close(df, t)
                if p is not None and p > 0:
                    prices[n] = float(p)
                    dates[n] = d
                    fails[n] = ""
                    indiv_err = ""
                    break
                indiv_err = "EmptyDF_or_NoClose"
            except Exception as e:
                indiv_err = f"{type(e).__name__}: {e}"
                continue

        if prices[n] <= 0:
            # まとめ取得のエラーが取れていたら併記
            if last_err and indiv_err:
                fails[n] = f"batch_fail={last_err} | indiv_fail={indiv_err}"
            elif indiv_err:
                fails[n] = indiv_err
            elif last_err:
                fails[n] = last_err
            else:
                fails[n] = "UnknownFail"

    return prices, dates, fails


def main() -> None:
    print("=== yfinance market fetch ===")
    ts = now_jst_str()
    print("timestamp_jst:", ts)

    prices, dates, fails = fetch_all_with_yfinance()

    row: Dict[str, object] = {"timestamp_jst": ts}

    # 値 + 欠損フラグ + 日付 + 失敗理由
    for name in ASSETS.keys():
        v = float(prices.get(name, 0.0) or 0.0)
        ok = (v > 0)
        row[name] = v
        row[f"{name}_missing"] = 0 if ok else 1
        row[f"{name}_date"] = dates.get(name, "")
        row[f"{name}_fail"] = "" if ok else (fails.get(name, "") or "UnknownFail")

        status = "✅" if ok else "❌"
        print(f"[yfinance] {name}({ASSETS[name]}): {v} ({row[f'{name}_date']}) {status} fail={row[f'{name}_fail']}")

    # 書き込み（追記）
    cols = list(row.keys())
    _ensure_csv_header(OUT_CSV, cols)

    df = pd.DataFrame([row])
    # 既存CSVに列が足りない/順序違いでも壊さないため、union列で保存
    try:
        existing = pd.read_csv(OUT_CSV, encoding="utf-8-sig")
        all_cols = list(dict.fromkeys(list(existing.columns) + cols))  # preserve order
        existing2 = existing.reindex(columns=all_cols)
        df2 = df.reindex(columns=all_cols)
        out = pd.concat([existing2, df2], ignore_index=True)
        out.to_csv(OUT_CSV, index=False, encoding="utf-8-sig")
    except Exception:
        # 既存が壊れていても今回の行は保存する
        df.to_csv(OUT_CSV, mode="a", index=False, header=False, encoding="utf-8-sig")

    print(f"=== saved -> {OUT_CSV} ===")


if __name__ == "__main__":
    main()
