import ccxt
import pandas as pd
import numpy as np
import time
from datetime import datetime, timezone

# ==========================================
# 1. 參數設定 (Binance USDT 永續合約)
# ==========================================
CONFIG = {
    "exchange_id": "binance",
    "symbols": {
        "ETH": "ETH/USDT:USDT",  # Binance USDT-M perpetual (ccxt unified symbol)
        "SOL": "SOL/USDT:USDT",
    },
    "timeframe": "1d",
    "start_date": "2022-01-01 00:00:00",
    "output_file": "pair_merged_1d_real.parquet",

    # Funding 防爆參數：單筆 fundingRate 絕對值超過此值就視為異常
    # 一般 funding 多數時間遠小於 1%（0.01），>1%/8h 已經很可疑
    "funding_abs_cap": 0.01,

    # 若某天 funding 事件數不是 3（00/08/16 UTC），視為不完整
    "expected_funding_events_per_day": 3,
}

def init_exchange():
    return ccxt.binance({
        "enableRateLimit": True,
        "options": {"defaultType": "future"},
    })

# ==========================================
# 2. 抓取 K 線 (OHLCV) - 這次保留完整 OHLCV
# ==========================================
def fetch_perp_ohlcv(exchange, symbol, timeframe, start_str):
    print(f"📥 [OHLCV] 正在下載 {symbol} ({timeframe})...")
    since = exchange.parse8601(start_str)
    all_ohlcv = []

    while True:
        try:
            ohlcv = exchange.fetch_ohlcv(symbol, timeframe, since, limit=1000)
            if not ohlcv:
                break
            all_ohlcv.extend(ohlcv)
            since = ohlcv[-1][0] + 1
            last_date = datetime.fromtimestamp(ohlcv[-1][0] / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
            print(f"   -> 已抓取至 {last_date} | 累積 {len(all_ohlcv)} 筆")
            if len(ohlcv) < 1000:
                break
        except Exception as e:
            print(f"⚠️ [OHLCV] Error: {e}")
            time.sleep(2)

    df = pd.DataFrame(all_ohlcv, columns=["timestamp", "open", "high", "low", "close", "volume"])
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
    df.set_index("timestamp", inplace=True)
    df = df[~df.index.duplicated(keep="first")]
    df = df.sort_index()

    return df[["open", "high", "low", "close", "volume"]]

# ==========================================
# 3. Funding Rate：抓取 + 防爆濾網 + 轉日線(sum) + events count
# ==========================================
def fetch_funding_history(exchange, symbol, start_str):
    print(f"💰 [Funding] 正在下載 {symbol} 資金費率...")
    since = exchange.parse8601(start_str)
    all_funding = []

    while True:
        try:
            funding = exchange.fetch_funding_rate_history(symbol, since, limit=1000)
            if not funding:
                break
            all_funding.extend(funding)
            since = funding[-1]["timestamp"] + 1
            last_date = datetime.fromtimestamp(funding[-1]["timestamp"] / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
            print(f"   -> 已抓取至 {last_date} | 累積 {len(all_funding)} 筆")
            if len(funding) < 1000:
                break
        except Exception as e:
            print(f"⚠️ [Funding] Error: {e}")
            time.sleep(2)

    if not all_funding:
        print("❌ 警告：抓不到 Funding Rate，請確認網路或 symbol 是否正確。")
        return pd.DataFrame()

    df = pd.DataFrame(all_funding)
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
    df = df[["timestamp", "fundingRate"]].copy()
    df["fundingRate"] = pd.to_numeric(df["fundingRate"], errors="coerce")
    df.set_index("timestamp", inplace=True)
    df = df.sort_index()

    return df

def funding_daily_with_guardrails(df_funding_raw, abs_cap=0.01, expected_events=3):
    """
    1) 防爆濾網：abs(fundingRate) > abs_cap 視為異常，改成 NaN（不納入 sum）
    2) resample 日線：
       - daily_funding_sum：sum(skipna)
       - funding_events：count（有效筆數）
       - funding_incomplete：事件數 != expected_events
    """
    if df_funding_raw.empty:
        out = pd.DataFrame(columns=["daily_funding_sum", "funding_events", "funding_incomplete"])
        return out

    df = df_funding_raw.copy()

    # 防爆：把離譜值剔除（設 NaN）
    outlier_mask = df["fundingRate"].abs() > abs_cap
    outlier_count = int(outlier_mask.sum())
    if outlier_count > 0:
        print(f"🧯 [Funding] 偵測到 {outlier_count} 筆異常 fundingRate (|rate|>{abs_cap})，已排除不納入日加總。")
    df.loc[outlier_mask, "fundingRate"] = np.nan

    # 日彙總
    daily = df.resample("1D").agg(
        daily_funding_sum=("fundingRate", "sum"),
        funding_events=("fundingRate", "count"),
        daily_funding_mean=("fundingRate", "mean"),
    )
    daily["funding_incomplete"] = daily["funding_events"] != expected_events

    return daily

# ==========================================
# 4. 移除未收盤日線（回測版）
# ==========================================
def drop_unclosed_last_daily_bar(df_daily):
    """
    如果最後一根的 timestamp = 今天(UTC)的 00:00:00，代表這根還在形成中 → drop。
    """
    if df_daily.empty:
        return df_daily

    idx = df_daily.index
    last_ts = idx.max()

    now_utc = pd.Timestamp.now(tz="UTC")
    today_floor = now_utc.floor("D")

    if last_ts == today_floor:
        print(f"🧹 [OHLCV] 偵測到最後一根為未收盤日線 ({last_ts})，回測版已移除。")
        return df_daily.iloc[:-1].copy()

    return df_daily

# ==========================================
# 5. 主程式
# ==========================================
def main():
    exchange = init_exchange()
    exchange.load_markets()  # 穩定性更好

    datasets = {}

    print("🚀 開始執行 Binance 真實數據下載 (Local)...")

    for name, symbol in CONFIG["symbols"].items():
        # A) OHLCV（保留完整）
        df_ohlcv = fetch_perp_ohlcv(exchange, symbol, CONFIG["timeframe"], CONFIG["start_date"])
        df_ohlcv = drop_unclosed_last_daily_bar(df_ohlcv)

        # B) Funding raw -> daily + guardrails
        df_fund_raw = fetch_funding_history(exchange, symbol, CONFIG["start_date"])
        df_fund_daily = funding_daily_with_guardrails(
            df_fund_raw,
            abs_cap=CONFIG["funding_abs_cap"],
            expected_events=CONFIG["expected_funding_events_per_day"],
        )

        # C) 合併（Left join：以 OHLCV 為主）
        df_merged = df_ohlcv.join(df_fund_daily, how="left")

        # 這裡不要直接 fillna(0) 把「缺資料」變成「0」
        # 只對 daily_funding_sum 做保守處理：若缺就先當 0，但保留 funding_incomplete 讓你之後可排除
        df_merged["daily_funding_sum"] = df_merged["daily_funding_sum"].fillna(0.0)
        df_merged["funding_events"] = df_merged["funding_events"].fillna(0).astype(int)
        df_merged["funding_incomplete"] = df_merged["funding_incomplete"].fillna(True)

        datasets[name] = df_merged
        print(f"✅ {name} 處理完成！ rows={len(df_merged)}")

    # --- 合併兩幣種 ---
    print("\n🔗 正在合併 ETH 與 SOL...")
    eth = datasets["ETH"].add_prefix("ETH_")
    sol = datasets["SOL"].add_prefix("SOL_")

    pair_df = pd.concat([eth, sol], axis=1, join="inner").sort_index()

    # --- 衍生欄位：Log & Returns ---
    pair_df["ETH_log"] = np.log(pair_df["ETH_close"])
    pair_df["SOL_log"] = np.log(pair_df["SOL_close"])
    pair_df["ETH_ret"] = pair_df["ETH_log"].diff()
    pair_df["SOL_ret"] = pair_df["SOL_log"].diff()

    # 只 drop 因 diff 造成的第一列 NaN（不要把其他欄位 NaN 全清掉）
    pair_df = pair_df.iloc[1:].copy()

    # --- 存檔 ---
    pair_df.to_parquet(CONFIG["output_file"])
    print(f"\n🎉 成功！已輸出: {CONFIG['output_file']}")
   
    # --- 簡易驗收報告 ---
    print("\n📋 === 驗收報告 ===")
    print(f"期間: {pair_df.index.min()} → {pair_df.index.max()} | rows={len(pair_df)} | tz={pair_df.index.tz}")

    # Funding 完整性（每天應該 3 筆）
    eth_incomplete = pair_df["ETH_funding_incomplete"].mean() * 100
    sol_incomplete = pair_df["SOL_funding_incomplete"].mean() * 100
    print(f"ETH funding 不完整比例: {eth_incomplete:.2f}%")
    print(f"SOL funding 不完整比例: {sol_incomplete:.2f}%")

    # Funding 異常快速掃描（日加總太誇張也列出）
    for col in ["ETH_daily_funding_sum", "SOL_daily_funding_sum"]:
        mn, mx = pair_df[col].min(), pair_df[col].max()
        print(f"{col} min={mn:.6f}, max={mx:.6f}")

    print("\n[最後 3 筆預覽]")
    print(pair_df.tail(3))

if __name__ == "__main__":
    main()
