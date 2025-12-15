import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

DATA_FILE = "pair_merged_1d_real.parquet"

df = pd.read_parquet(DATA_FILE)

# 1) Summary table
summary = {
    "start": df.index.min(),
    "end": df.index.max(),
    "rows": len(df),
    "tz": str(df.index.tz),
    "dup_index": int(df.index.duplicated().sum()),
    "missing_ratio_max": float(df.isna().mean().max()),
    "ETH_funding_incomplete_days": int(df["ETH_funding_incomplete"].sum()),
    "SOL_funding_incomplete_days": int(df["SOL_funding_incomplete"].sum()),
}
summary_df = pd.DataFrame([summary])
summary_df.to_csv("phase1_summary.csv", index=False, encoding="utf-8-sig")

# 2) Missing ratio
missing = df.isna().mean().sort_values(ascending=False)
missing.to_csv("phase1_missing_ratio.csv", header=["missing_ratio"], encoding="utf-8-sig")

# 3) Funding incomplete days list
bad = df[df["ETH_funding_incomplete"] | df["SOL_funding_incomplete"]].copy()
bad_cols = [
    "ETH_funding_events","ETH_daily_funding_sum",
    "SOL_funding_events","SOL_daily_funding_sum"
]
bad[bad_cols].to_csv("phase1_funding_incomplete_days.csv", encoding="utf-8-sig")

# 4) Basic anomaly checks (price=0, extreme returns)
anoms = []
for sym in ["ETH", "SOL"]:
    close = df[f"{sym}_close"]
    ret = df[f"{sym}_ret"]
    anoms.append({
        "symbol": sym,
        "close_eq_0_days": int((close == 0).sum()),
        "ret_abs_gt_20pct_days": int((ret.abs() > np.log(1.2)).sum()),  # ~20% log-move
        "ret_min": float(ret.min()),
        "ret_max": float(ret.max()),
    })
pd.DataFrame(anoms).to_csv("phase1_anomaly_checks.csv", index=False, encoding="utf-8-sig")

# 5) Plot: log prices
plt.figure()
plt.plot(df.index, df["ETH_log"], label="ETH log price")
plt.plot(df.index, df["SOL_log"], label="SOL log price")
plt.legend()
plt.title("ETH vs SOL (Log Price, 1D, UTC)")
plt.xlabel("UTC Date")
plt.ylabel("log(price)")
plt.tight_layout()
plt.savefig("phase1_log_price.png", dpi=160)
plt.close()

print("✅ Phase 1 report exported:")
print(" - phase1_summary.csv")
print(" - phase1_missing_ratio.csv")
print(" - phase1_funding_incomplete_days.csv")
print(" - phase1_anomaly_checks.csv")
print(" - phase1_log_price.png")
