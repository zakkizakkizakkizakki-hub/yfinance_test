# market_yfinance_collector.py
from __future__ import annotations

import os
import time
import random
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import pandas as pd

try:
    import yfinance as yf
except Exception as e:
    raise SystemExit(f"yfinance import failed: {e}")

JST = ZoneInfo("Asia/Tokyo")

# 収集対象（あなたのログに合わせた）
ASSETS = [
    ("USDJPY", "JPY=X"),
    ("BTC", "BTC-USD"),
    ("Gold", "GC=F"),
    ("US10Y", "^TNX"),
    ("Oil", "CL=F"),
    ("VIX", "^VIX"),
]

OUT_CSV = os.getenv("MARKET_CSV", "market_yfinance_log.csv")

# リトライ設定（“間隔を確認したい”とのことなので明示＆ログ出し）
MAX_RETRIES = int(os.getenv("YF_MAX_RETRIES", "4"))
BASE_SLEEP_SEC = float(os.getenv("YF_BASE_SLEEP_SEC", "15"))  # 15秒を基準に指数バックオフ
TIMEOUT_SEC = int(os.getenv("YF_TIMEOUT_SEC", "20"))


def now_jst_str() -> str:
    return datetime.now(JST).strftime("%Y-%m-%d %H:%M:%S")


def _safe_float(x) -> float:
    try:
        if x is None:
            return 0.0
        if isinstance(x, str) and x.strip() == "":
            return 0.0
        v = float(x)
        if pd.isna(v):
            return 0.0
        return v
    except Exception:
        return 0.0


def fetch_once() -> tuple[dict[str, dict], str]:
    """
    6銘柄を1回の yf.download で取得（呼び出し回数を最小化）。
    戻り値:
      - result: {asset: {price, date, src, fail}}
      - warn: 失敗要因の簡易文字列（例: EmptyDF / NoClose / Exception:...）
    """
    tickers = " ".join([t for _, t in ASSETS])

    try:
        df = yf.download(
            tickers=tickers,
            period="5d",
            interval="1d",
            group_by="ticker",
            auto_adjust=True,   # ログにも出ている “default changed” の影響を固定
            threads=False,      # スレッドで同時に叩くのを避ける（レート制限悪化を防ぐ）
            progress=False,
            timeout=TIMEOUT_SEC,
        )
    except Exception as e:
        res = {}
        for asset, _ in ASSETS:
            res[asset] = {"price": 0.0, "date": "", "src": "missing", "fail": f"Exception:{type(e).__name__}"}
        return res, f"Exception:{type(e).__name__}"

    if df is None or len(df) == 0:
        res = {}
        for asset, _ in ASSETS:
            res[asset] = {"price": 0.0, "date": "", "src": "missing", "fail": "EmptyDF"}
        return res, "EmptyDF"

    # yfinanceの返り値は MultiIndex の場合がある
    # 例: columns = ('Close','JPY=X') もしくは ticker->OHLCV の入れ子
    # ここでは「各銘柄の最後の Close」を取りに行く
    result = {}
    for asset, ticker in ASSETS:
        price = 0.0
        date_str = ""
        fail = ""

        try:
            close_series = None

            # パターンA: columns が (field, ticker) の MultiIndex
            if isinstance(df.columns, pd.MultiIndex):
                if ("Close", ticker) in df.columns:
                    close_series = df[("Close", ticker)]
                elif ("Adj Close", ticker) in df.columns:
                    close_series = df[("Adj Close", ticker)]

            # パターンB: columns が ticker->field 形式（group_by="ticker" で起きがち）
            if close_series is None:
                if ticker in df.columns:
                    sub = df[ticker]
                    if isinstance(sub, pd.DataFrame) and "Close" in sub.columns:
                        close_series = sub["Close"]

            if close_series is None:
                fail = "NoClose"
            else:
                close_series = close_series.dropna()
                if len(close_series) == 0:
                    fail = "NoClose"
                else:
                    last_dt = close_series.index[-1]
                    # pandas Timestamp の場合あり
                    if hasattr(last_dt, "to_pydatetime"):
                        last_dt = last_dt.to_pydatetime()
                    date_str = str(last_dt.date())
                    price = float(close_series.iloc[-1])

        except Exception as e:
            fail = f"ParseErr:{type(e).__name__}"

        if fail:
            result[asset] = {"price": 0.0, "date": "", "src": "missing", "fail": fail}
        else:
            result[asset] = {"price": price, "date": date_str, "src": "yfinance", "fail": ""}

    # 6個すべて missing なら “実質失敗”
    if all(result[a]["src"] == "missing" for a, _ in ASSETS):
        return result, "AllMissing"

    return result, ""


def collect() -> int:
    print("=== yfinance market fetch ===")
    ts_jst = now_jst_str()
    print(f"timestamp_jst: {ts_jst}")

    last_warn = ""
    final = None

    for attempt in range(1, MAX_RETRIES + 1):
        data, warn = fetch_once()
        final = data
        last_warn = warn

        # 成功判定：少なくとも1つは取れている
        ok_any = any(v["src"] != "missing" for v in data.values())
        if ok_any:
            break

        # 失敗時の待ち（指数バックオフ＋ジッター）
        sleep_sec = BASE_SLEEP_SEC * (2 ** (attempt - 1))
        sleep_sec = sleep_sec * (0.8 + 0.4 * random.random())  # 0.8〜1.2倍
        sleep_sec = min(sleep_sec, 180)  # 上限3分
        print(f"[retry] attempt={attempt}/{MAX_RETRIES} warn={warn} sleep={sleep_sec:.1f}s")
        time.sleep(sleep_sec)

    # CSV行を作る（既存仕様を維持しつつ、理由ログ列を追加）
    row = {
        "timestamp_jst": ts_jst,
        "timestamp_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "run_warn": last_warn,
    }
    for asset, _ in ASSETS:
        row[f"{asset}_price"] = _safe_float(final[asset]["price"])
        row[f"{asset}_date"] = final[asset]["date"]
        row[f"{asset}_src"] = final[asset]["src"]
        row[f"{asset}_fail"] = final[asset]["fail"]

        # 表示ログ（あなたの形式に寄せる）
        status = "✅" if final[asset]["src"] != "missing" else "❌"
        print(f"[{final[asset]['src']}] {asset}: {row[f'{asset}_price']} ({row[f'{asset}_date']}) {status} fail={final[asset]['fail']}")

    out_df = pd.DataFrame([row])

    # 追記保存（ヘッダは初回だけ）
    exists = os.path.exists(OUT_CSV)
    out_df.to_csv(OUT_CSV, mode="a", index=False, header=not exists)

    print(f"=== saved -> {OUT_CSV} ===")

    # collector自体は「欠損で落とす」責務を持たせない（監視は monitor が担当）
    return 0


if __name__ == "__main__":
    raise SystemExit(collect())
