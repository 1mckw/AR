# AR/DR Hourly Signals

每小時 GitHub Action 掃描並只輸出 **AR/DR 訊號**。

## 線上報告

- 最新：https://github.com/1mckw/AR/blob/main/signals/latest.md
- JSON：https://github.com/1mckw/AR/blob/main/signals/latest.json
- Actions：https://github.com/1mckw/AR/actions/workflows/hourly-signals.yml

## 商品池

| 池 | 來源 | 說明 |
|----|------|------|
| **S&P 500** | datasets constituents CSV | 約 500 檔美股 |
| **期貨** | Yahoo `=F` | 金銀銅油氣、股指、債、外匯、BTC 期 |
| **Crypto Top 50** | Binance 24h USDT 成交額 | 前 50 名現貨 |

週期固定 **1H**。參數：急跌/漲 `3%`、連 K `3`、量 ×`1.2`、結構 LH/HL、近 `2` 根內新鮮訊號。

## 手動觸發

Repo → **Actions** → **Hourly AR/DR Signals** → **Run workflow**

排程：每小時 `:10` UTC。

## 本機跑一次

```bash
python scan_signals.py
# → signals/latest.md / signals/latest.json
```

## 可選：本機圖表 UI

```bash
python server.py
# http://127.0.0.1:8765/
```
