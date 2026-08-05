# AR/DR Hourly Signals

https://1mckw.github.io/AR/

每小時 GitHub Action 掃描並只輸出 **AR/DR 訊號**。

## 線上報告

- HTML：https://1mckw.github.io/AR/ （或 repo 內 [`signals/latest.html`](./signals/latest.html)）
- JSON：https://github.com/1mckw/AR/blob/main/signals/latest.json
- Actions：https://github.com/1mckw/AR/actions/workflows/hourly-signals.yml

首次請到 **Settings → Pages → Source: GitHub Actions**。

## 商品池

| 池 | 來源 | 說明 |
|----|------|------|
| **NASDAQ-100** | index-constituents CSV | 約 100 檔美股 |
| **DJI 30** | index-constituents CSV | 道瓊 30 成分股 |
| **期貨** | Yahoo `=F` | 金銀銅油氣、股指、債、外匯、BTC 期 |
| **Crypto Top 50** | Binance 24h USDT 成交額 | 前 50 名現貨 |

週期：**1H** 與 **1D**，最多 **2000** 根 K。

### AR/DR 規則

| | AR | DR |
|---|----|----|
| 觸發 | 急跌後反轉陽線 | 急漲後反轉陰線 |
| 射線 | 信號 K **上引線（高）** 與 **下引線（低）** 各向右延伸，碰到即停 |
| 晚觸碰報告 | AR→上引線、DR→下引線，超過 12 根後 |

**只報告：**
- AR/DR **超過 12 根 K 後**觸碰主引線（AR→上引線、DR→下引線，晚觸碰）
- **趨勢線**影線觸碰

**趨勢線：** 至少 **3** 個觸點。首末 Pivot 定斜率；觸點含 Pivot 共線，以及 **不明顯局部高低點**（左右 2 根內極值 + 影線距線 0.4% 內）。相鄰觸點至少間隔 3 根 K。

**趨勢線貫穿：** 急漲/急跌 K 實體可貫穿（阻力←急漲、支撐←急跌）；貫穿後實體仍在线外不得超過 **2** 根 K，否則線失效。

**不報告：** 近 12 根內新出現的 AR/DR 信號本身。

參數：急跌/漲 `3%`、連 K `3`、量 ×`1.2`、結構 LH/HL、近 `2` 根內新鮮觸碰。

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
