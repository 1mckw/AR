# AR/DR · Trend Lines

HTML 圖表：AR/DR 信號 + Simple Auto Trend Lines（TradingView 商品搜尋、圖例、12 根 K 延伸）。

## 線上（GitHub Actions → Pages）

自動部署：**https://1mckw.github.io/AR/**

1. Repo → **Settings → Pages → Build and deployment → Source: GitHub Actions**
2. 推送 `main` 後，於 **Actions** 查看 `Deploy GitHub Pages` / `CI`

> Pages 為靜態站：Binance crypto 可直接用；TradingView 搜尋與 Yahoo（股／期）需本機 `server.py`。

## 本機完整功能

```bash
python server.py
# → http://127.0.0.1:8765/
```

| 端點 | 用途 |
|------|------|
| `/` | 圖表 UI |
| `/api/tv-search?q=` | TradingView 商品搜尋 |
| `/api/yahoo?symbol=&bars=&interval=` | 股票／期貨 K 線 |

## 你的定義

| 信號 | 意思 |
|------|------|
| **AR** | 急**下跌**趨勢中，**第一根陽線** |
| **DR** | 急**上漲**趨勢中，**第一根陰線** |

## 圖表設定（TradingView Pine）

1. 開 **1H** 圖表
2. 加 [Simple Auto Trend Lines](https://www.tradingview.com/script/UpsMdRXG-Simple-Auto-Trend-Lines/)
3. 再加 `pine/ar_signal_volume_filter.pine`

### 建議參數

**Simple Auto Trend Lines**

- Pivot High/Low: `4`
- Max drawing distance: `500`
- Candles to cross for invalidation: `2–3`

**AR Signal + High Volume Filter**

- 急趨勢幅度: `3%`（crypto 可調 `4–5%`）
- 最少連續陰/陽線: `3`
- 量能 ≥ MA(20) × `1.2`

## 怎麼解讀

```
急跌 → 下降趨勢線仍壓在上方的空頭結構
         ↓
      第一根 AR 陽線（高量）→ 可能開始修正，不是馬上翻多
         ↓
   看 Simple Auto Trend Lines 的阻力線是否仍有效
```

- **AR 出現 + 仍在下降趨勢線下方** → 短線反彈，不是結構翻多
- **AR 出現 + 放量站回趨勢線** → 結構轉強機率較高
- **DR** 是 AR 的鏡像

## 掃描

TradingView Screener + `pine/ar_screener.pine`（1H、高量／大型股 filter）。

## 檔案

| 檔案 | 用途 |
|------|------|
| `index.html` | Web 圖表 |
| `server.py` | 靜態 + Yahoo / TV 搜尋代理 |
| `pine/ar_signal_volume_filter.pine` | 圖表 overlay |
| `pine/ar_screener.pine` | Screener |
| `.github/workflows/` | Pages 部署 + CI 煙霧測試 |
