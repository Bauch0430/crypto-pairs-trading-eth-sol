# crypto-pairs-trading-eth-sol

Funding-aware ETH/SOL pairs trading research on Binance USDT perpetuals (OHLCV + funding rate), with a reproducible data pipeline.

## TL;DR
- **Goal:** Explore whether ETH & SOL exhibit a mean-reverting relationship suitable for **market-neutral statistical arbitrage**, while accounting for **funding-rate costs**.
- **Data:** Binance USDT-M perpetual futures daily OHLCV + funding rates.
- **Current status:** Phase 1 (data quality checks + reporting) ✅  
- **Next:** Phase 2 (cointegration / spread / z-score) → Phase 3 (backtest) → Phase 4 (report)

## Key Deliverables
- **Code (pipeline):**
  - `src/01_get_binance_data.py` — download & clean OHLCV + funding
  - `src/02_phase1_report.py` — generate Phase 1 QA reports
- **Phase 1 outputs:** `reports/phase1/`
  - `phase1_summary.csv`
  - `phase1_missing_ratio.csv`
  - `phase1_funding_incomplete_days.csv`
  - `phase1_anomaly_checks.csv`
  - `phase1_log_price.png`

### Sample chart (Phase 1)
![Log price](reports/phase1/phase1_log_price.png)

## Repository Structure
```text
crypto-pairs-trading-eth-sol/
  src/
    01_get_binance_data.py
    02_phase1_report.py
  reports/
    phase1/
      phase1_summary.csv
      phase1_missing_ratio.csv
      phase1_funding_incomplete_days.csv
      phase1_anomaly_checks.csv
      phase1_log_price.png
  README.md
  LICENSE
  .gitignore
