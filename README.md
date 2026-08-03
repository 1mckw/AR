# AR/DR + Simple Auto Trend Lines 工作流

## 你的定義

| 信號 | 意思 |
|------|------|
| **AR** | 急**下跌**趨勢中，**第一根陽線**（1H） |
| **DR** | 急**上漲**趨勢中，**第一根陰線**（1H，反之） |

## 圖表設定（單一商品）

1. 開 **1H** 圖表（台指期 TXF1!、大型股、高成交量 crypto）
2. 加 community 指標：[Simple Auto Trend Lines](https://www.tradingview.com/script/UpsMdRXG-Simple-Auto-Trend-Lines/)
3. 再加本 repo 的 `pine/ar_signal_volume_filter.pine`
4. 看 **趨勢線結構 + AR/DR 標記** 一起判斷

### 建議參數

**Simple Auto Trend Lines**

- Pivot High/Low: `4`（1H 常用）
- Max drawing distance: `500`
- Candles to cross for invalidation: `2–3`

**AR Signal + High Volume Filter**

- 急趨勢幅度: `3%`（crypto 可調 `4–5%`）
- 最少連續陰/陽線: `3`
- 量能 ≥ MA(20) × `1.2`（過濾冷門標的）

## 怎麼解讀（配合你之前的平行高點邏輯）

```
急跌 → 下降趨勢線仍壓在上方的空頭結構
         ↓
      第一根 AR 陽線（高量）→ 可能開始修正，不是馬上翻多
         ↓
   看 Simple Auto Trend Lines 的阻力線是否仍有效
```

- **AR 出現 + 仍在下降趨勢線下方** → 短線反彈，不是結構翻多
- **AR 出現 + 放量站回趨勢線** → 結構轉強機率較高
- **DR** 是 AR 的鏡像，用在急漲後第一根陰線

## 掃描大型股 / 高量 Crypto

TradingView Screener：

1. **Stock**：Filter 加 `Market cap > 10B` 或自選權值股 watchlist
2. **Crypto**：Filter 加 `24h Volume` 排序前 20–50
3. Timeframe 設 **1 hour**
4. 加入 `pine/ar_screener.pine` 作為 screener 條件

Pine 無法直接讀「市值排名」，**大型股用 Screener 內建 filter，流動性用 volume 倍數過濾**。

## 安裝 Pine Script

1. TradingView → Pine Editor → 新建
2. 貼上 `pine/ar_signal_volume_filter.pine` 內容
3. Save → Add to chart
4. 可設 Alert：`AR — 急跌後首根陽線 + 高量`

## 檔案

| 檔案 | 用途 |
|------|------|
| `pine/ar_signal_volume_filter.pine` | 圖表 overlay，標 AR/DR + 高量過濾 |
| `pine/ar_screener.pine` | Screener 批量掃描 |
