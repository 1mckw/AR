#!/usr/bin/env python3
"""Hourly touch scanner: NDX100 + futures + top 50 crypto.

Reports only:
  - AR/DR base-level touch after >12 bars from signal (not new AR/DR within 12 bars)
  - Trend-line wick touches
"""

from __future__ import annotations

import csv
import html
import io
import json
import os
import time
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Any

ROOT = os.path.dirname(os.path.abspath(__file__))
OUT_DIR = os.path.join(ROOT, "signals")

LOOKBACK = 10
VOL_LEN = 20
DROP_PCT = 3.0
MIN_STREAK = 3
VOL_MULT = 1.2
USE_STRUCTURE = True
TOUCH_WINDOW_BARS = 12
FRESH_BARS = 2  # report touches on last N bars
BARS = 500

PIVOT_HIGH = 4
PIVOT_LOW = 4
MAX_LOOKBACK = 500
INVALIDATE_CROSS = 2
MAX_RESISTANCE = 1
MAX_SUPPORT = 1
MAX_LINES_PER_PIVOT = 1

UA = {"User-Agent": "Mozilla/5.0 (compatible; AR-Signal-Scanner/1.0)"}

FUTURES = [
    ("GC=F", "Gold"),
    ("SI=F", "Silver"),
    ("HG=F", "Copper"),
    ("CL=F", "Crude Oil"),
    ("NG=F", "Natural Gas"),
    ("ES=F", "E-mini S&P 500"),
    ("NQ=F", "E-mini Nasdaq"),
    ("YM=F", "E-mini Dow"),
    ("RTY=F", "E-mini Russell"),
    ("ZB=F", "US Treasury Bond"),
    ("ZN=F", "10Y T-Note"),
    ("6E=F", "Euro FX"),
    ("6J=F", "Japanese Yen"),
    ("BTC=F", "Bitcoin Futures"),
]


CRYPTO_FALLBACK = [
    ("BTCUSDT", "BTC"), ("ETHUSDT", "ETH"), ("SOLUSDT", "SOL"), ("BNBUSDT", "BNB"),
    ("XRPUSDT", "XRP"), ("DOGEUSDT", "DOGE"), ("ADAUSDT", "ADA"), ("AVAXUSDT", "AVAX"),
    ("LINKUSDT", "LINK"), ("DOTUSDT", "DOT"), ("TRXUSDT", "TRX"), ("MATICUSDT", "MATIC"),
    ("LTCUSDT", "LTC"), ("BCHUSDT", "BCH"), ("UNIUSDT", "UNI"), ("ATOMUSDT", "ATOM"),
    ("NEARUSDT", "NEAR"), ("APTUSDT", "APT"), ("ARBUSDT", "ARB"), ("OPUSDT", "OP"),
    ("SUIUSDT", "SUI"), ("PEPEUSDT", "PEPE"), ("WIFUSDT", "WIF"), ("FILUSDT", "FIL"),
    ("ICPUSDT", "ICP"), ("AAVEUSDT", "AAVE"), ("INJUSDT", "INJ"), ("TIAUSDT", "TIA"),
    ("RENDERUSDT", "RENDER"), ("FETUSDT", "FET"), ("SEIUSDT", "SEI"), ("STXUSDT", "STX"),
    ("IMXUSDT", "IMX"), ("RUNEUSDT", "RUNE"), ("GRTUSDT", "GRT"), ("MKRUSDT", "MKR"),
    ("EGLDUSDT", "EGLD"), ("ALGOUSDT", "ALGO"), ("FTMUSDT", "FTM"), ("HBARUSDT", "HBAR"),
    ("VETUSDT", "VET"), ("SANDUSDT", "SAND"), ("MANAUSDT", "MANA"), ("AXSUSDT", "AXS"),
    ("THETAUSDT", "THETA"), ("EOSUSDT", "EOS"), ("FLOWUSDT", "FLOW"), ("XTZUSDT", "XTZ"),
    ("CHZUSDT", "CHZ"), ("LDOUSDT", "LDO"),
]

BINANCE_BASES = [
    "https://data-api.binance.vision",
    "https://api.binance.com",
    "https://api1.binance.com",
]


def http_get(url: str, timeout: int = 30) -> bytes:
    req = urllib.request.Request(url, headers=UA)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read()


def http_get_json(url: str, timeout: int = 30) -> Any:
    return json.loads(http_get(url, timeout).decode())


def fetch_ndx100() -> list[tuple[str, str]]:
    url = "https://yfiua.github.io/index-constituents/constituents-nasdaq100.csv"
    try:
        raw = http_get(url, timeout=40).decode()
        rows = list(csv.DictReader(io.StringIO(raw)))
        out = []
        for r in rows:
            sym = (r.get("Symbol") or r.get("symbol") or "").strip().replace(".", "-")
            name = (r.get("Name") or r.get("Security") or sym).strip()
            if sym:
                out.append((sym, name))
        if len(out) >= 80:
            return out
    except Exception as exc:
        print(f"NDX100 fetch failed: {exc}", flush=True)
    return [
        (s, s)
        for s in [
            "AAPL", "MSFT", "NVDA", "AMZN", "META", "GOOGL", "GOOG", "AVGO", "TSLA", "COST",
            "NFLX", "AMD", "PEP", "ADBE", "CSCO", "TMUS", "INTC", "INTU", "AMAT", "QCOM",
        ]
    ]


def fetch_top50_crypto() -> list[tuple[str, str]]:
    skip_bases = {
        "USDC", "FDUSD", "TUSD", "BUSD", "DAI", "EUR", "AEUR", "BFUSD", "USD1", "XUSD",
    }
    for base in BINANCE_BASES:
        try:
            data = http_get_json(f"{base}/api/v3/ticker/24hr", timeout=40)
            usdt = []
            for t in data:
                sym = str(t.get("symbol", ""))
                if not sym.endswith("USDT"):
                    continue
                if any(x in sym for x in ("UPUSDT", "DOWNUSDT", "BULL", "BEAR")):
                    continue
                if sym[:-4] in skip_bases:
                    continue
                usdt.append(t)
            usdt.sort(key=lambda t: float(t.get("quoteVolume") or 0), reverse=True)
            out = [(t["symbol"], t["symbol"].replace("USDT", "")) for t in usdt[:50]]
            if out:
                print(f"Crypto universe via {base}: {len(out)}", flush=True)
                return out
        except Exception as exc:
            print(f"Crypto ticker failed ({base}): {exc}", flush=True)
    print("Crypto universe fallback list", flush=True)
    return list(CRYPTO_FALLBACK)


def parse_binance_klines(raw: list) -> list[dict]:
    return [
        {
            "time": int(k[0]) // 1000,
            "open": float(k[1]),
            "high": float(k[2]),
            "low": float(k[3]),
            "close": float(k[4]),
            "volume": float(k[5]),
        }
        for k in raw
    ]


def fetch_binance_1h(symbol: str, bars: int = BARS) -> list[dict]:
    params = urllib.parse.urlencode({"symbol": symbol, "interval": "1h", "limit": min(bars, 1000)})
    last_err: Exception | None = None
    for base in BINANCE_BASES:
        try:
            return parse_binance_klines(http_get_json(f"{base}/api/v3/klines?{params}"))
        except Exception as exc:  # noqa: BLE001
            last_err = exc
    raise last_err  # type: ignore[misc]


def fetch_yahoo_1h(symbol: str, bars: int = BARS) -> list[dict]:
    yrange = "3mo" if bars <= 700 else "6mo"
    hosts = [
        "https://query1.finance.yahoo.com",
        "https://query2.finance.yahoo.com",
    ]
    last_err: Exception | None = None
    for host in hosts:
        url = (
            f"{host}/v8/finance/chart/"
            + urllib.parse.quote(symbol, safe="=-.^")
            + f"?interval=60m&range={yrange}&includePrePost=false"
        )
        try:
            payload = http_get_json(url)
            result = (payload.get("chart") or {}).get("result") or []
            if not result:
                continue
            r0 = result[0]
            ts = r0.get("timestamp") or []
            q0 = ((r0.get("indicators") or {}).get("quote") or [{}])[0]
            out = []
            for i, t in enumerate(ts):
                o, h, l, c = (
                    (q0.get("open") or [None])[i],
                    (q0.get("high") or [None])[i],
                    (q0.get("low") or [None])[i],
                    (q0.get("close") or [None])[i],
                )
                v = (q0.get("volume") or [0])[i] or 0
                if None in (o, h, l, c):
                    continue
                out.append(
                    {
                        "time": int(t),
                        "open": float(o),
                        "high": float(h),
                        "low": float(l),
                        "close": float(c),
                        "volume": float(v),
                    }
                )
            if out:
                return out[-bars:] if len(out) > bars else out
        except Exception as exc:  # noqa: BLE001
            last_err = exc
    if last_err:
        raise last_err
    return []


def bear_bar(c: list[dict], i: int) -> bool:
    return c[i]["close"] < c[i]["open"]


def bull_bar(c: list[dict], i: int) -> bool:
    return c[i]["close"] > c[i]["open"]


def streak(c: list[dict], i: int, bear: bool, length: int) -> bool:
    for j in range(1, length + 1):
        idx = i - j
        if idx < 0:
            return False
        if bear and not bear_bar(c, idx):
            return False
        if not bear and not bull_bar(c, idx):
            return False
    return True


def sma_vol(c: list[dict], i: int, length: int) -> float | None:
    if i < length - 1:
        return None
    return sum(c[i - j]["volume"] for j in range(length)) / length


def detect_signals(candles: list[dict]) -> list[dict]:
    signals = []
    if len(candles) < LOOKBACK + MIN_STREAK + 2:
        return signals
    for i in range(LOOKBACK + MIN_STREAK, len(candles)):
        base = candles[i - LOOKBACK]["close"]
        if not base:
            continue
        drop_pct = (base - candles[i]["close"]) / base * 100
        rise_pct = (candles[i]["close"] - base) / base * 100
        vol_ma = sma_vol(candles, i, VOL_LEN)
        high_vol = vol_ma is None or candles[i]["volume"] >= vol_ma * VOL_MULT
        prev_drop = drop_pct >= DROP_PCT and streak(candles, i - 1, True, MIN_STREAK)
        prev_rise = rise_pct >= DROP_PCT and streak(candles, i - 1, False, MIN_STREAK)
        if USE_STRUCTURE:
            max_h = max(candles[i - k]["high"] for k in range(1, LOOKBACK + 1))
            min_l = min(candles[i - k]["low"] for k in range(1, LOOKBACK + 1))
            prev_drop = prev_drop and candles[i]["high"] < max_h
            prev_rise = prev_rise and candles[i]["low"] > min_l
        if high_vol and bull_bar(candles, i) and bear_bar(candles, i - 1) and prev_drop:
            signals.append(
                {
                    "type": "AR",
                    "index": i,
                    "time": candles[i]["time"],
                    "level": candles[i]["high"],
                    "close": candles[i]["close"],
                    "volume": candles[i]["volume"],
                }
            )
        if high_vol and bear_bar(candles, i) and bull_bar(candles, i - 1) and prev_rise:
            signals.append(
                {
                    "type": "DR",
                    "index": i,
                    "time": candles[i]["time"],
                    "level": candles[i]["low"],
                    "close": candles[i]["close"],
                    "volume": candles[i]["volume"],
                }
            )
    return signals


def resolve_horizontal_ray(candles: list[dict], item: dict) -> dict:
    """Return ray state. late_touch_index set when base touched after >12 bars."""
    is_ar = item["type"] == "AR"
    base_level = item["level"]
    signal_time = item["time"]
    for j in range(item["index"] + 1, len(candles)):
        bar = candles[j]
        within = j - item["index"] <= TOUCH_WINDOW_BARS
        hit_base = bar["high"] >= base_level if is_ar else bar["low"] <= base_level
        if hit_base and within:
            sig_bar = candles[item["index"]]
            ext_level = sig_bar["low"] if is_ar else sig_bar["high"]
            for m in range(j + 1, len(candles)):
                b2 = candles[m]
                hit_ext = b2["low"] <= ext_level if is_ar else b2["high"] >= ext_level
                if hit_ext:
                    return {
                        "baseLevel": base_level,
                        "level": ext_level,
                        "extended": True,
                        "active": False,
                        "late_touch_index": None,
                        "endTime": b2["time"],
                    }
            return {
                "baseLevel": base_level,
                "level": ext_level,
                "extended": True,
                "active": True,
                "late_touch_index": None,
                "endTime": candles[-1]["time"],
            }
        if hit_base and not within:
            return {
                "baseLevel": base_level,
                "level": base_level,
                "extended": False,
                "active": False,
                "late_touch_index": j,
                "endTime": bar["time"],
            }
    return {
        "baseLevel": base_level,
        "level": base_level,
        "extended": False,
        "active": True,
        "late_touch_index": None,
        "endTime": candles[-1]["time"],
        "startTime": signal_time,
    }


def fresh_range(n: int) -> tuple[int, int]:
    last = n - 1
    lo = max(0, last - (FRESH_BARS - 1))
    return lo, last


def collect_late_ar_dr_touches(candles: list[dict], signals: list[dict]) -> list[dict]:
    """AR/DR base touch after >12 bars, only if touch bar is fresh."""
    if not candles:
        return []
    lo, last = fresh_range(len(candles))
    hits = []
    for sig in signals:
        ray = resolve_horizontal_ray(candles, sig)
        ti = ray.get("late_touch_index")
        if ti is None:
            continue
        if not (lo <= ti <= last):
            continue
        hits.append(
            {
                "kind": "ar_dr_touch",
                "label": f"{sig['type']} 觸碰",
                "type": sig["type"],
                "signal_time": sig["time"],
                "signal_index": sig["index"],
                "bars_after_signal": ti - sig["index"],
                "time": candles[ti]["time"],
                "index": ti,
                "level": ray["baseLevel"],
                "close": candles[ti]["close"],
            }
        )
    return hits


def find_pivots(candles: list[dict], length: int, highs_only: bool, lows_only: bool):
    highs, lows = [], []
    for i in range(length, len(candles) - length):
        is_high = is_low = True
        for j in range(1, length + 1):
            if candles[i]["high"] <= candles[i - j]["high"] or candles[i]["high"] <= candles[i + j]["high"]:
                is_high = False
            if candles[i]["low"] >= candles[i - j]["low"] or candles[i]["low"] >= candles[i + j]["low"]:
                is_low = False
        if is_high and not lows_only:
            highs.append({"index": i, "time": candles[i]["time"], "price": candles[i]["high"]})
        if is_low and not highs_only:
            lows.append({"index": i, "time": candles[i]["time"], "price": candles[i]["low"]})
    return highs, lows


def line_price(p1: dict, slope: float, idx: int) -> float:
    return p1["price"] + slope * (idx - p1["index"])


def valid_between_pivots(candles: list[dict], p1: dict, p2: dict, resistance: bool) -> bool:
    if p2["index"] <= p1["index"]:
        return False
    slope = (p2["price"] - p1["price"]) / (p2["index"] - p1["index"])
    for i in range(p1["index"] + 1, p2["index"]):
        lp = line_price(p1, slope, i)
        body_hi = max(candles[i]["open"], candles[i]["close"])
        body_lo = min(candles[i]["open"], candles[i]["close"])
        if resistance and body_hi > lp:
            return False
        if not resistance and body_lo < lp:
            return False
    return True


def valid_to_current(candles: list[dict], p1: dict, p2: dict, resistance: bool) -> bool:
    slope = (p2["price"] - p1["price"]) / (p2["index"] - p1["index"])
    for i in range(p2["index"] + 1, len(candles)):
        lp = line_price(p1, slope, i)
        body_hi = max(candles[i]["open"], candles[i]["close"])
        body_lo = min(candles[i]["open"], candles[i]["close"])
        if resistance and body_hi > lp:
            return False
        if not resistance and body_lo < lp:
            return False
    return True


def build_auto_trend_lines(candles: list[dict]) -> list[dict]:
    start_idx = max(0, len(candles) - MAX_LOOKBACK)
    slice_c = candles[start_idx:]
    offset = start_idx
    piv_high, _ = find_pivots(slice_c, PIVOT_HIGH, True, False)
    _, piv_low = find_pivots(slice_c, PIVOT_LOW, False, True)
    piv_high = [{**p, "index": p["index"] + offset} for p in piv_high]
    piv_low = [{**p, "index": p["index"] + offset} for p in piv_low]

    def collect(pts: list[dict], resistance: bool) -> list[dict]:
        candidates = []
        for a, p1 in enumerate(pts):
            count_from = 0
            for b in range(a + 1, len(pts)):
                if count_from >= MAX_LINES_PER_PIVOT:
                    break
                p2 = pts[b]
                if resistance and p2["price"] >= p1["price"]:
                    continue
                if not resistance and p2["price"] <= p1["price"]:
                    continue
                if not valid_between_pivots(candles, p1, p2, resistance):
                    continue
                if not valid_to_current(candles, p1, p2, resistance):
                    continue
                slope = (p2["price"] - p1["price"]) / (p2["index"] - p1["index"])
                candidates.append(
                    {
                        "type": "resistance" if resistance else "support",
                        "p1": p1,
                        "p2": p2,
                        "slope": slope,
                        "span": p2["index"] - p1["index"],
                    }
                )
                count_from += 1
        candidates.sort(key=lambda c: (-c["span"], c["p1"]["index"]))
        picked, used = [], set()
        limit = MAX_RESISTANCE if resistance else MAX_SUPPORT
        for c in candidates:
            if len(picked) >= limit:
                break
            if c["p1"]["index"] in used:
                continue
            picked.append(c)
            used.add(c["p1"]["index"])
        return picked

    return collect(piv_high, True) + collect(piv_low, False)


def check_line_invalidation(candles: list[dict], line: dict) -> bool:
    consecutive = 0
    p1, slope, typ = line["p1"], line["slope"], line["type"]
    for i in range(len(candles) - 1, p1["index"], -1):
        lp = line_price(p1, slope, i)
        c = candles[i]
        crossed = (
            c["close"] > lp and c["open"] > lp * 0.999
            if typ == "resistance"
            else c["close"] < lp and c["open"] < lp * 1.001
        )
        if crossed:
            consecutive += 1
            if consecutive >= INVALIDATE_CROSS:
                return True
        else:
            consecutive = 0
    return False


def find_trend_touch(candles: list[dict], line: dict) -> dict | None:
    start = max(line["p2"]["index"], line["p1"]["index"]) + 1
    for i in range(start, len(candles)):
        lp = line_price(line["p1"], line["slope"], i)
        c = candles[i]
        touched = c["high"] >= lp if line["type"] == "resistance" else c["low"] <= lp
        if touched:
            return {"time": c["time"], "price": lp, "index": i, "close": c["close"]}
    return None


def collect_trend_touches(candles: list[dict], lines: list[dict]) -> list[dict]:
    if not candles:
        return []
    lo, last = fresh_range(len(candles))
    hits = []
    for line in lines:
        if check_line_invalidation(candles, line):
            continue
        touch = find_trend_touch(candles, line)
        if not touch:
            continue
        if not (lo <= touch["index"] <= last):
            continue
        label = "阻力趨勢線觸碰" if line["type"] == "resistance" else "支撐趨勢線觸碰"
        hits.append(
            {
                "kind": "trend_touch",
                "label": label,
                "type": line["type"],
                "time": touch["time"],
                "index": touch["index"],
                "level": touch["price"],
                "close": touch["close"],
            }
        )
    return hits


def with_retries(fn, retries: int = 3, pause: float = 0.8):
    last_err = None
    for attempt in range(retries):
        try:
            return fn()
        except Exception as exc:  # noqa: BLE001
            last_err = exc
            time.sleep(pause * (attempt + 1))
    raise last_err  # type: ignore[misc]


def scan_one(group: str, symbol: str, name: str, source: str) -> dict:
    try:
        if source == "binance":
            candles = with_retries(lambda: fetch_binance_1h(symbol))
        else:
            candles = with_retries(lambda: fetch_yahoo_1h(symbol))
        signals = detect_signals(candles)
        late = collect_late_ar_dr_touches(candles, signals)
        lines = build_auto_trend_lines(candles)
        trend = collect_trend_touches(candles, lines)
        events = late + trend
        return {
            "group": group,
            "symbol": symbol,
            "name": name,
            "source": source,
            "bars": len(candles),
            "events": events,
            "error": None,
        }
    except Exception as exc:  # noqa: BLE001
        return {
            "group": group,
            "symbol": symbol,
            "name": name,
            "source": source,
            "bars": 0,
            "events": [],
            "error": str(exc),
        }


def fmt_ts(ts: int) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def fmt_num(v: float) -> str:
    return f"{v:.6g}"


def render_html(payload: dict) -> str:
    hits = payload["hits"]
    ar_dr = [h for h in hits if h["kind"] == "ar_dr_touch"]
    trend = [h for h in hits if h["kind"] == "trend_touch"]
    errs = [r for r in payload["results"] if r.get("error")]
    c = payload["counts"]

    def rows_ar_dr() -> str:
        if not ar_dr:
            return '<tr><td colspan="7" class="empty">目前無 AR/DR 觸碰</td></tr>'
        out = []
        for h in ar_dr:
            cls = "ar" if h.get("type") == "AR" else "dr"
            out.append(
                "<tr>"
                f'<td><span class="tag {cls}">{html.escape(str(h.get("type", "")))}</span></td>'
                f"<td>{html.escape(h.get('group', ''))}</td>"
                f"<td><code>{html.escape(h.get('symbol', ''))}</code></td>"
                f"<td>{html.escape(h.get('name', ''))}</td>"
                f"<td class=\"num\">{fmt_num(float(h['level']))}</td>"
                f"<td class=\"num\">{int(h.get('bars_after_signal', 0))}</td>"
                f"<td>{html.escape(fmt_ts(int(h['time'])))}</td>"
                "</tr>"
            )
        return "\n".join(out)

    def rows_trend() -> str:
        if not trend:
            return '<tr><td colspan="6" class="empty">目前無趨勢線觸碰</td></tr>'
        out = []
        for h in trend:
            cls = "resist" if h.get("type") == "resistance" else "support"
            out.append(
                "<tr>"
                f'<td><span class="tag {cls}">{html.escape(str(h.get("type", "")))}</span></td>'
                f"<td>{html.escape(h.get('group', ''))}</td>"
                f"<td><code>{html.escape(h.get('symbol', ''))}</code></td>"
                f"<td>{html.escape(h.get('name', ''))}</td>"
                f"<td class=\"num\">{fmt_num(float(h['level']))}</td>"
                f"<td>{html.escape(fmt_ts(int(h['time'])))}</td>"
                "</tr>"
            )
        return "\n".join(out)

    err_block = ""
    if errs[:15]:
        items = "".join(
            f"<li><code>{html.escape(e['symbol'])}</code> — {html.escape(str(e['error']))}</li>"
            for e in errs[:15]
        )
        err_block = f"<h2>Sample errors</h2><ul class=\"errs\">{items}</ul>"

    return f"""<!DOCTYPE html>
<html lang="zh-Hant">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <meta http-equiv="refresh" content="3600" />
  <title>AR/DR Touch Alerts</title>
  <link rel="preconnect" href="https://fonts.googleapis.com" />
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin />
  <link href="https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;600;700&family=Space+Grotesk:wght@400;500;600;700&display=swap" rel="stylesheet" />
  <style>
    :root {{
      --bg: #05070b; --panel: #0c121c; --surface: #121a28; --border: rgba(0,240,200,.16);
      --text: #e8f7f4; --muted: #7a93a8; --primary: #00f0c8;
      --ar: #00e896; --dr: #ff4d6d; --support: #00e896; --resist: #ff4d6d;
    }}
    * {{ box-sizing: border-box; margin: 0; padding: 0; }}
    body {{
      font-family: "Space Grotesk", system-ui, sans-serif;
      background:
        radial-gradient(900px 420px at 10% -10%, rgba(0,240,200,.08), transparent 55%),
        linear-gradient(180deg, #070b12, var(--bg));
      color: var(--text); min-height: 100vh; padding: 28px 18px 48px;
      line-height: 1.45;
    }}
    .wrap {{ max-width: 1100px; margin: 0 auto; }}
    h1 {{
      font-size: 1.55rem; font-weight: 700; letter-spacing: -.03em;
      background: linear-gradient(90deg, #e8f7f4, var(--primary));
      -webkit-background-clip: text; background-clip: text; color: transparent;
    }}
    .meta {{ color: var(--muted); font-size: .9rem; margin: 8px 0 18px; }}
    .meta strong {{ color: var(--primary); font-weight: 600; }}
    .cards {{
      display: grid; grid-template-columns: repeat(4, 1fr); gap: 10px; margin-bottom: 22px;
    }}
    @media (max-width: 720px) {{ .cards {{ grid-template-columns: 1fr 1fr; }} }}
    .card {{
      background: var(--panel); border: 1px solid var(--border); border-radius: 12px; padding: 12px 14px;
    }}
    .card .lbl {{ font-size: .65rem; color: var(--muted); text-transform: uppercase; letter-spacing: .08em; }}
    .card .val {{ font-family: "JetBrains Mono", monospace; font-size: 1.25rem; font-weight: 700; margin-top: 4px; }}
    .rules {{
      background: rgba(0,240,200,.04); border: 1px solid var(--border); border-radius: 12px;
      padding: 12px 14px; margin-bottom: 22px; font-size: .85rem; color: var(--muted);
    }}
    .rules li {{ margin: 4px 0 4px 1.1em; }}
    h2 {{ font-size: 1.05rem; margin: 22px 0 10px; font-weight: 650; }}
    .panel {{
      background: var(--panel); border: 1px solid var(--border); border-radius: 14px; overflow: hidden;
    }}
    table {{ width: 100%; border-collapse: collapse; font-size: .84rem; }}
    th, td {{ padding: 9px 12px; text-align: left; border-bottom: 1px solid rgba(0,240,200,.08); }}
    th {{
      background: var(--surface); color: var(--muted); font-size: .68rem;
      text-transform: uppercase; letter-spacing: .06em; font-weight: 600;
    }}
    tr:hover td {{ background: rgba(0,240,200,.04); }}
    td.num, th.num {{ text-align: right; font-family: "JetBrains Mono", monospace; font-variant-numeric: tabular-nums; }}
    td.empty {{ text-align: center; color: var(--muted); padding: 22px; }}
    code {{ font-family: "JetBrains Mono", monospace; font-size: .82em; color: var(--primary); }}
    .tag {{
      display: inline-block; font-family: "JetBrains Mono", monospace; font-weight: 700;
      font-size: .72rem; padding: 2px 7px; border-radius: 5px;
    }}
    .tag.ar {{ background: rgba(0,232,150,.14); color: var(--ar); }}
    .tag.dr {{ background: rgba(255,77,109,.14); color: var(--dr); }}
    .tag.resist {{ background: rgba(255,77,109,.14); color: var(--resist); }}
    .tag.support {{ background: rgba(0,232,150,.14); color: var(--support); }}
    .errs {{ margin: 8px 0 0 1.1em; color: var(--muted); font-size: .82rem; }}
    footer {{ margin-top: 28px; color: var(--muted); font-size: .75rem; }}
    a {{ color: var(--primary); }}
  </style>
</head>
<body>
  <div class="wrap">
    <h1>Touch Alerts</h1>
    <p class="meta">Updated <strong>{html.escape(payload['generated_at'])}</strong> · TF <strong>1H</strong> · fresh last <strong>{FRESH_BARS}</strong> bar(s)</p>
    <div class="cards">
      <div class="card"><div class="lbl">Hits</div><div class="val">{c['hits']}</div></div>
      <div class="card"><div class="lbl">NDX100</div><div class="val">{c['ndx100']}</div></div>
      <div class="card"><div class="lbl">Futures</div><div class="val">{c['futures']}</div></div>
      <div class="card"><div class="lbl">Crypto</div><div class="val">{c['crypto']}</div></div>
    </div>
    <ul class="rules">
      <li><strong>不報告</strong> 近 {TOUCH_WINDOW_BARS} 根內新出現的 AR/DR</li>
      <li><strong>報告</strong> 信號後超過 {TOUCH_WINDOW_BARS} 根才觸碰 AR/DR 原價位</li>
      <li><strong>報告</strong> 趨勢線影線觸碰</li>
    </ul>

    <h2>AR/DR 觸碰（&gt;{TOUCH_WINDOW_BARS} 根後） · {len(ar_dr)}</h2>
    <div class="panel">
      <table>
        <thead>
          <tr><th>Type</th><th>Group</th><th>Symbol</th><th>Name</th><th class="num">Level</th><th class="num">Bars after</th><th>Time</th></tr>
        </thead>
        <tbody>
          {rows_ar_dr()}
        </tbody>
      </table>
    </div>

    <h2>趨勢線觸碰 · {len(trend)}</h2>
    <div class="panel">
      <table>
        <thead>
          <tr><th>Side</th><th>Group</th><th>Symbol</th><th>Name</th><th class="num">Level</th><th>Time</th></tr>
        </thead>
        <tbody>
          {rows_trend()}
        </tbody>
      </table>
    </div>

    <h2>Scan stats</h2>
    <p class="meta">OK symbols <strong>{c['ok']}</strong> · Errors <strong>{len(errs)}</strong> · Hits <strong>{c['hits']}</strong></p>
    {err_block}

    <footer>
      Source: <a href="https://github.com/1mckw/AR">1mckw/AR</a> ·
      JSON: <a href="./latest.json">latest.json</a>
    </footer>
  </div>
</body>
</html>
"""


def main() -> int:
    os.makedirs(OUT_DIR, exist_ok=True)
    print("Loading universes…", flush=True)
    try:
        ndx100 = fetch_ndx100()
        crypto = fetch_top50_crypto()
    except Exception as exc:  # noqa: BLE001
        print(f"Universe load failed: {exc}", flush=True)
        ndx100 = fetch_ndx100()  # has internal fallback
        crypto = list(CRYPTO_FALLBACK)

    jobs = (
        [("ndx100", s, n, "yahoo") for s, n in ndx100]
        + [("futures", s, n, "yahoo") for s, n in FUTURES]
        + [("crypto", s, n, "binance") for s, n in crypto]
    )
    print(f"Scanning {len(jobs)} symbols for touches…", flush=True)

    results: list[dict] = []
    workers = 4 if os.environ.get("GITHUB_ACTIONS") else 6
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futs = [pool.submit(scan_one, g, s, n, src) for g, s, n, src in jobs]
        done = 0
        for fut in as_completed(futs):
            try:
                results.append(fut.result())
            except Exception as exc:  # noqa: BLE001
                results.append(
                    {
                        "group": "unknown",
                        "symbol": "?",
                        "name": "?",
                        "source": "?",
                        "bars": 0,
                        "events": [],
                        "error": str(exc),
                    }
                )
            done += 1
            if done % 50 == 0:
                print(f"  progress {done}/{len(jobs)}", flush=True)

    hits = []
    for r in results:
        for ev in r.get("events") or []:
            hits.append(
                {
                    **ev,
                    "group": r["group"],
                    "symbol": r["symbol"],
                    "name": r["name"],
                }
            )
    hits.sort(key=lambda x: (x["kind"], x["group"], x["symbol"], x.get("type", "")))

    payload = {
        "generated_at": datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
        "timeframe": "1h",
        "params": {
            "touch_window_bars": TOUCH_WINDOW_BARS,
            "fresh_bars": FRESH_BARS,
            "drop_pct": DROP_PCT,
            "min_streak": MIN_STREAK,
            "vol_mult": VOL_MULT,
            "use_structure": USE_STRUCTURE,
            "pivot_high": PIVOT_HIGH,
            "pivot_low": PIVOT_LOW,
        },
        "counts": {
            "ndx100": len(ndx100),
            "futures": len(FUTURES),
            "crypto": len(crypto),
            "ok": sum(1 for r in results if not r.get("error")),
            "hits": len(hits),
        },
        "hits": hits,
        "results": results,
    }

    json_path = os.path.join(OUT_DIR, "latest.json")
    html_path = os.path.join(OUT_DIR, "latest.html")
    index_path = os.path.join(OUT_DIR, "index.html")
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    page = render_html(payload)
    with open(html_path, "w", encoding="utf-8") as f:
        f.write(page)
    with open(index_path, "w", encoding="utf-8") as f:
        f.write(page)

    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with open(summary, "a", encoding="utf-8") as f:
            f.write(f"### Touch Alerts\n\nHits: **{len(hits)}** · OK: **{payload['counts']['ok']}**\n")

    print(f"Hits: {len(hits)} · wrote {html_path} and {json_path}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
