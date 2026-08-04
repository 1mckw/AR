#!/usr/bin/env python3
"""Touch scanner: NDX100 + futures + top 50 crypto on 1H and 1D.

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

import ardr

ROOT = os.path.dirname(os.path.abspath(__file__))
OUT_DIR = os.path.join(ROOT, "signals")

LOOKBACK = ardr.LOOKBACK
VOL_LEN = ardr.VOL_LEN
DROP_PCT = ardr.DROP_PCT
MIN_STREAK = ardr.MIN_STREAK
VOL_MULT = ardr.VOL_MULT
USE_STRUCTURE = ardr.USE_STRUCTURE
TOUCH_WINDOW_BARS = ardr.TOUCH_WINDOW_BARS
FRESH_BARS = ardr.FRESH_BARS
BARS = 2000
CHART_BARS = 2000
TIMEFRAMES = ("1h", "1d")
TF_LABEL = {"1h": "1H", "1d": "1D"}
YAHOO_INTERVAL = {"1h": "60m", "1d": "1d"}
BINANCE_INTERVAL = {"1h": "1h", "1d": "1d"}

detect_signals = ardr.detect_signals
resolve_horizontal_ray = ardr.resolve_horizontal_ray
collect_late_ar_dr_touches = ardr.collect_late_ar_dr_touches
fresh_range = ardr.fresh_range

PIVOT_HIGH = 4
PIVOT_LOW = 4
MAX_LOOKBACK = 2000
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


def yahoo_range(interval: str, bars: int) -> str:
    if interval == "1d":
        if bars <= 500:
            return "2y"
        if bars <= 2000:
            return "5y"
        if bars <= 3000:
            return "10y"
        return "max"
    # 1h
    if bars <= 700:
        return "3mo"
    if bars <= 2000:
        return "6mo"
    if bars <= 3000:
        return "1y"
    return "730d"


def fetch_binance(symbol: str, timeframe: str = "1h", bars: int = BARS) -> list[dict]:
    """Paginate Binance klines (max 1000 per request) up to `bars`."""
    iv = BINANCE_INTERVAL[timeframe]
    by_time: dict[int, dict] = {}
    end_time: int | None = None
    last_err: Exception | None = None

    while len(by_time) < bars:
        need = min(1000, bars - len(by_time))
        params: dict[str, str | int] = {"symbol": symbol, "interval": iv, "limit": need}
        if end_time is not None:
            params["endTime"] = end_time
        query = urllib.parse.urlencode(params)
        batch = None
        for base in BINANCE_BASES:
            try:
                batch = http_get_json(f"{base}/api/v3/klines?{query}", timeout=45)
                last_err = None
                break
            except Exception as exc:  # noqa: BLE001
                last_err = exc
        if batch is None:
            if by_time:
                break
            raise last_err  # type: ignore[misc]
        if not batch:
            break
        for c in parse_binance_klines(batch):
            by_time[c["time"]] = c
        end_time = int(batch[0][0]) - 1
        if len(batch) < need:
            break

    out = sorted(by_time.values(), key=lambda c: c["time"])
    return out[-bars:] if len(out) > bars else out


def fetch_yahoo(symbol: str, timeframe: str = "1h", bars: int = BARS) -> list[dict]:
    iv = YAHOO_INTERVAL[timeframe]
    yrange = yahoo_range(timeframe, bars)
    hosts = [
        "https://query1.finance.yahoo.com",
        "https://query2.finance.yahoo.com",
    ]
    last_err: Exception | None = None
    for host in hosts:
        url = (
            f"{host}/v8/finance/chart/"
            + urllib.parse.quote(symbol, safe="=-.^")
            + f"?interval={iv}&range={yrange}&includePrePost=false"
        )
        try:
            payload = http_get_json(url, timeout=45)
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


def build_chart_pack(candles: list[dict], signals: list[dict], lines: list[dict]) -> dict:
    """Compact candles + AR/DR rays + trend lines for the HTML chart modal."""
    last_time = int(candles[-1]["time"]) if candles else 0
    rays = [
        ardr.signal_to_chart_ray(sig, resolve_horizontal_ray(candles, sig), last_time)
        for sig in signals
    ]

    trend = []
    last_i = len(candles) - 1
    for line in lines:
        invalidated = check_line_invalidation(candles, line)
        end_time = candles[last_i]["time"]
        end_price = line_price(line["p1"], line["slope"], last_i)
        if invalidated:
            consecutive = 0
            p1, slope, typ = line["p1"], line["slope"], line["type"]
            for i in range(last_i, p1["index"], -1):
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
                        end_time = c["time"]
                        end_price = lp
                        break
                else:
                    consecutive = 0
        trend.append(
            {
                "type": line["type"],
                "p1": {"time": int(line["p1"]["time"]), "price": float(line["p1"]["price"])},
                "p2": {"time": int(line["p2"]["time"]), "price": float(line["p2"]["price"])},
                "endTime": int(end_time),
                "endPrice": float(end_price),
                "invalidated": invalidated,
            }
        )

    trimmed = candles[-CHART_BARS:] if len(candles) > CHART_BARS else candles
    return {
        "candles": [
            {
                "time": int(c["time"]),
                "open": float(c["open"]),
                "high": float(c["high"]),
                "low": float(c["low"]),
                "close": float(c["close"]),
            }
            for c in trimmed
        ],
        "rays": rays,
        "trend_lines": trend,
    }


def chart_key(group: str, symbol: str, timeframe: str) -> str:
    return f"{group}|{symbol}|{timeframe}"


def with_retries(fn, retries: int = 3, pause: float = 0.8):
    last_err = None
    for attempt in range(retries):
        try:
            return fn()
        except Exception as exc:  # noqa: BLE001
            last_err = exc
            time.sleep(pause * (attempt + 1))
    raise last_err  # type: ignore[misc]


def scan_one(group: str, symbol: str, name: str, source: str, timeframe: str) -> dict:
    try:
        if source == "binance":
            candles = with_retries(lambda: fetch_binance(symbol, timeframe))
        else:
            candles = with_retries(lambda: fetch_yahoo(symbol, timeframe))
        signals = detect_signals(candles)
        late = collect_late_ar_dr_touches(candles, signals)
        lines = build_auto_trend_lines(candles)
        trend = collect_trend_touches(candles, lines)
        events = late + trend
        for ev in events:
            ev["timeframe"] = timeframe
        out = {
            "group": group,
            "symbol": symbol,
            "name": name,
            "source": source,
            "timeframe": timeframe,
            "bars": len(candles),
            "events": events,
            "error": None,
        }
        if events:
            out["chart"] = build_chart_pack(candles, signals, lines)
        return out
    except Exception as exc:  # noqa: BLE001
        return {
            "group": group,
            "symbol": symbol,
            "name": name,
            "source": source,
            "timeframe": timeframe,
            "bars": 0,
            "events": [],
            "error": str(exc),
        }


def fmt_ts(ts: int) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def fmt_num(v: float) -> str:
    return f"{v:.6g}"


def fmt_tf(tf: str) -> str:
    return TF_LABEL.get(tf, (tf or "?").upper())


CHART_MODAL_SCRIPT = r"""
<script src="https://unpkg.com/lightweight-charts@4.2.0/dist/lightweight-charts.standalone.production.js"></script>
<script>
(function () {
  const PACKS = window.CHART_PACKS || {};
  const FUTURES_TV = {
    "GC=F": "COMEX:GC1!", "SI=F": "COMEX:SI1!", "HG=F": "COMEX:HG1!",
    "CL=F": "NYMEX:CL1!", "NG=F": "NYMEX:NG1!", "ES=F": "CME_MINI:ES1!",
    "NQ=F": "CME_MINI:NQ1!", "YM=F": "CBOT_MINI:YM1!", "RTY=F": "CME_MINI:RTY1!",
    "ZB=F": "CBOT:ZB1!", "ZN=F": "CBOT:ZN1!", "6E=F": "CME:6E1!",
    "6J=F": "CME:6J1!", "BTC=F": "CME:BTC1!",
  };

  const modal = document.getElementById("chart-modal");
  const titleEl = document.getElementById("chart-title");
  const subEl = document.getElementById("chart-sub");
  const statusEl = document.getElementById("chart-status");
  const lwcEl = document.getElementById("lwc");
  const frameEl = document.getElementById("tv-frame");
  const closeBtn = document.getElementById("chart-close");
  let chart = null;
  let series = null;
  let overlays = [];
  let openToken = 0;
  let resizeObs = null;

  function packKey(group, symbol, tf) {
    return group + "|" + symbol + "|" + tf;
  }

  function tvSymbol(group, symbol) {
    if (group === "crypto") return "BINANCE:" + symbol;
    if (group === "futures") return FUTURES_TV[symbol] || symbol;
    return "NASDAQ:" + symbol;
  }

  function tvInterval(tf) {
    return tf === "1d" ? "D" : "60";
  }

  function fmtLevel(v) {
    const n = Number(v);
    if (!Number.isFinite(n)) return String(v || "");
    return n.toLocaleString(undefined, { maximumFractionDigits: 8 });
  }

  function typeLabel(type, kind) {
    if (kind === "ar_dr_touch") return type || "AR/DR";
    if (type === "resistance") return "阻力線";
    if (type === "support") return "支撐線";
    return type || "觸碰";
  }

  function destroyChart() {
    if (resizeObs) { resizeObs.disconnect(); resizeObs = null; }
    overlays = [];
    if (chart) {
      chart.remove();
      chart = null;
      series = null;
    }
    lwcEl.innerHTML = "";
    frameEl.removeAttribute("src");
    frameEl.hidden = true;
    lwcEl.hidden = true;
  }

  function closeModal() {
    openToken += 1;
    destroyChart();
    modal.classList.remove("open");
    modal.hidden = true;
    modal.setAttribute("aria-hidden", "true");
    document.body.style.overflow = "";
  }

  function showStatus(msg) {
    statusEl.hidden = false;
    statusEl.textContent = msg;
  }

  function hideStatus() {
    statusEl.hidden = true;
  }

  function drawSegment(t0, p0, t1, p1, color, width, style) {
    if (t0 == null || t1 == null || t0 === t1) return;
    const s = chart.addLineSeries({
      color,
      lineWidth: width,
      lineStyle: style ?? LightweightCharts.LineStyle.Solid,
      priceLineVisible: false,
      lastValueVisible: false,
      crosshairMarkerVisible: false,
    });
    s.setData([
      { time: t0, value: p0 },
      { time: t1, value: p1 },
    ]);
    overlays.push(s);
  }

  function rayColor(type, active) {
    const rgb = type === "AR" ? "0,232,150" : "255,77,109";
    return active ? `rgba(${rgb},1)` : `rgba(${rgb},0.35)`;
  }

  function markerColor(type, active) {
    if (type === "AR") return active ? "#00e896" : "rgba(0,232,150,0.35)";
    return active ? "#ff4d6d" : "rgba(255,77,109,0.35)";
  }

  function drawRaySegments(ray, lastTime) {
    const segs = ray.segments || [];
    for (const seg of segs) {
      const t1 = seg.t1 === lastTime && ray.active && !ray.extended ? lastTime : seg.t1;
      drawSegment(
        seg.t0,
        seg.price,
        t1,
        seg.price,
        rayColor(ray.type, seg.active),
        1,
        LightweightCharts.LineStyle.Dashed
      );
    }
  }

  function normalizeCandles(candles) {
    const out = [];
    let prev = null;
    for (const c of candles) {
      const t = c.time;
      if (prev != null && t <= prev) continue;
      out.push({
        time: t,
        open: c.open,
        high: c.high,
        low: c.low,
        close: c.close,
      });
      prev = t;
    }
    return out;
  }

  function renderPack(pack) {
    destroyChart();
    lwcEl.hidden = false;
    frameEl.hidden = true;
    const candles = normalizeCandles(pack.candles || []);
    if (!candles.length) throw new Error("no candles");

    chart = LightweightCharts.createChart(lwcEl, {
      layout: { background: { color: "#05070b" }, textColor: "#7a93a8" },
      grid: {
        vertLines: { color: "rgba(0,240,200,0.05)" },
        horzLines: { color: "rgba(0,240,200,0.05)" },
      },
      rightPriceScale: { borderColor: "#2a364d" },
      timeScale: { borderColor: "#2a364d", timeVisible: true, secondsVisible: false },
      crosshair: { mode: LightweightCharts.CrosshairMode.Normal },
    });
    series = chart.addCandlestickSeries({
      upColor: "#00e896",
      downColor: "#ff4d6d",
      borderUpColor: "#00e896",
      borderDownColor: "#ff4d6d",
      wickUpColor: "#5dffb1",
      wickDownColor: "#ff7a90",
    });
    series.setData(candles);

    (pack.trend_lines || []).forEach((line) => {
      const alpha = line.invalidated ? 0.3 : 0.95;
      const color = line.type === "resistance"
        ? `rgba(239,68,68,${alpha})`
        : `rgba(34,197,94,${alpha})`;
      drawSegment(line.p1.time, line.p1.price, line.endTime, line.endPrice, color, 2);
      drawSegment(line.p1.time, line.p1.price, line.p2.time, line.p2.price, color, 1, LightweightCharts.LineStyle.Dotted);
    });

    const markers = [];
    const lastTime = candles[candles.length - 1].time;
    (pack.rays || []).forEach((ray) => {
      drawRaySegments(ray, lastTime);
      markers.push({
        time: ray.time,
        position: ray.type === "AR" ? "belowBar" : "aboveBar",
        color: markerColor(ray.type, ray.active),
        shape: "circle",
        size: ray.active ? 2 : 1,
        text: ray.type,
      });
    });
    markers.sort((a, b) => a.time - b.time);
    series.setMarkers(markers);

    chart.timeScale().fitContent();
    resizeObs = new ResizeObserver(() => {
      if (!chart) return;
      chart.applyOptions({ width: lwcEl.clientWidth, height: lwcEl.clientHeight });
    });
    resizeObs.observe(lwcEl);
    chart.applyOptions({ width: lwcEl.clientWidth, height: lwcEl.clientHeight });
  }

  function renderTradingView(group, symbol, tf) {
    destroyChart();
    lwcEl.hidden = true;
    frameEl.hidden = false;
    const params = new URLSearchParams({
      symbol: tvSymbol(group, symbol),
      interval: tvInterval(tf),
      theme: "dark",
      style: "1",
      locale: "zh_TW",
      timezone: "Etc/UTC",
      toolbarbg: "0c121c",
      hideideas: "1",
      hidesidetoolbar: "0",
      symboledit: "1",
      saveimage: "0",
      withdateranges: "1",
    });
    frameEl.src = "https://s.tradingview.com/widgetembed/?" + params.toString();
  }

  async function openChart(btn) {
    const symbol = btn.dataset.symbol || "";
    const group = btn.dataset.group || "";
    const name = btn.dataset.name || symbol;
    const tf = btn.dataset.tf || "1h";
    const level = btn.dataset.level || "";
    const type = btn.dataset.type || "";
    const kind = btn.dataset.kind || "";
    const token = ++openToken;
    const key = packKey(group, symbol, tf);
    const pack = PACKS[key];

    titleEl.textContent = `${symbol} · ${tf === "1d" ? "1D" : "1H"}`;
    const rayN = pack && pack.rays ? pack.rays.length : 0;
    const tlN = pack && pack.trend_lines ? pack.trend_lines.length : 0;
    subEl.innerHTML =
      `${escapeHtml(name)} · ${escapeHtml(group)} · ${escapeHtml(typeLabel(type, kind))}` +
      (level ? ` · Level <strong>${escapeHtml(fmtLevel(level))}</strong>` : "") +
      (pack ? ` · AR/DR ${rayN} · 趨勢線 ${tlN}` : "");

    modal.hidden = false;
    modal.setAttribute("aria-hidden", "false");
    requestAnimationFrame(() => modal.classList.add("open"));
    document.body.style.overflow = "hidden";
    destroyChart();
    showStatus("載入蠟燭圖…");

    try {
      if (pack && pack.candles && pack.candles.length) {
        if (token !== openToken) return;
        hideStatus();
        renderPack(pack);
        return;
      }
      if (token !== openToken) return;
      hideStatus();
      renderTradingView(group, symbol, tf);
    } catch (err) {
      if (token !== openToken) return;
      showStatus("圖表載入失敗，改開 TradingView…");
      setTimeout(() => {
        if (token !== openToken) return;
        hideStatus();
        renderTradingView(group, symbol, tf);
      }, 400);
    }
  }

  function escapeHtml(s) {
    return String(s)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  document.addEventListener("click", (ev) => {
    const btn = ev.target.closest(".sym-btn");
    if (btn) {
      ev.preventDefault();
      openChart(btn);
      return;
    }
    if (ev.target === modal) closeModal();
  });
  closeBtn.addEventListener("click", closeModal);
  document.addEventListener("keydown", (ev) => {
    if (ev.key === "Escape" && modal.classList.contains("open")) closeModal();
  });
})();
</script>
"""


def render_html(payload: dict) -> str:
    hits = payload["hits"]
    ar_dr = [h for h in hits if h["kind"] == "ar_dr_touch"]
    trend = [h for h in hits if h["kind"] == "trend_touch"]
    errs = [r for r in payload["results"] if r.get("error")]
    c = payload["counts"]

    def sym_btn(h: dict) -> str:
        sym = str(h.get("symbol", ""))
        attrs = (
            f'data-symbol="{html.escape(sym, quote=True)}" '
            f'data-group="{html.escape(str(h.get("group", "")), quote=True)}" '
            f'data-name="{html.escape(str(h.get("name", "")), quote=True)}" '
            f'data-tf="{html.escape(str(h.get("timeframe", "")), quote=True)}" '
            f'data-level="{html.escape(str(h.get("level", "")), quote=True)}" '
            f'data-type="{html.escape(str(h.get("type", "")), quote=True)}" '
            f'data-kind="{html.escape(str(h.get("kind", "")), quote=True)}" '
            f'data-time="{html.escape(str(h.get("time", "")), quote=True)}"'
        )
        return (
            f'<button type="button" class="sym-btn" {attrs} '
            f'title="開啟蠟燭圖">'
            f"<code>{html.escape(sym)}</code></button>"
        )

    def rows_ar_dr() -> str:
        if not ar_dr:
            return '<tr><td colspan="8" class="empty">目前無 AR/DR 觸碰</td></tr>'
        out = []
        for h in ar_dr:
            cls = "ar" if h.get("type") == "AR" else "dr"
            out.append(
                "<tr>"
                f'<td><span class="tag {cls}">{html.escape(str(h.get("type", "")))}</span></td>'
                f"<td>{html.escape(fmt_tf(h.get('timeframe', '')))}</td>"
                f"<td>{html.escape(h.get('group', ''))}</td>"
                f"<td>{sym_btn(h)}</td>"
                f"<td>{html.escape(h.get('name', ''))}</td>"
                f"<td class=\"num\">{fmt_num(float(h['level']))}</td>"
                f"<td class=\"num\">{int(h.get('bars_after_signal', 0))}</td>"
                f"<td>{html.escape(fmt_ts(int(h['time'])))}</td>"
                "</tr>"
            )
        return "\n".join(out)

    def rows_trend() -> str:
        if not trend:
            return '<tr><td colspan="7" class="empty">目前無趨勢線觸碰</td></tr>'
        out = []
        for h in trend:
            cls = "resist" if h.get("type") == "resistance" else "support"
            out.append(
                "<tr>"
                f'<td><span class="tag {cls}">{html.escape(str(h.get("type", "")))}</span></td>'
                f"<td>{html.escape(fmt_tf(h.get('timeframe', '')))}</td>"
                f"<td>{html.escape(h.get('group', ''))}</td>"
                f"<td>{sym_btn(h)}</td>"
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

    tf_meta = " · ".join(fmt_tf(tf) for tf in payload.get("timeframes") or TIMEFRAMES)

    page_head = f"""<!DOCTYPE html>
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
    .sym-btn {{
      background: none; border: 0; padding: 0; cursor: pointer; color: inherit;
      border-radius: 4px; text-align: left;
    }}
    .sym-btn:hover code {{ text-decoration: underline; text-underline-offset: 3px; }}
    .sym-btn:focus-visible {{ outline: 2px solid var(--primary); outline-offset: 3px; }}
    .modal {{
      position: fixed; inset: 0; z-index: 80; display: flex; align-items: center; justify-content: center;
      padding: 16px; background: rgba(2, 6, 12, .72); backdrop-filter: blur(8px);
      opacity: 0; pointer-events: none; transition: opacity .2s ease;
    }}
    .modal.open {{ opacity: 1; pointer-events: auto; }}
    .modal-panel {{
      width: min(1100px, 100%); height: min(720px, 92vh);
      background: var(--panel); border: 1px solid var(--border); border-radius: 16px;
      display: flex; flex-direction: column; overflow: hidden;
      box-shadow: 0 24px 80px rgba(0,0,0,.45);
      transform: translateY(10px) scale(.985); transition: transform .22s ease;
    }}
    .modal.open .modal-panel {{ transform: none; }}
    .modal-head {{
      display: flex; align-items: flex-start; justify-content: space-between; gap: 12px;
      padding: 14px 16px; border-bottom: 1px solid rgba(0,240,200,.1); background: var(--surface);
    }}
    .modal-title {{ font-size: 1.05rem; font-weight: 650; letter-spacing: -.02em; }}
    .modal-sub {{ color: var(--muted); font-size: .82rem; margin-top: 4px; }}
    .modal-sub strong {{ color: var(--primary); font-weight: 600; }}
    .modal-close {{
      width: 40px; height: 40px; border-radius: 10px; border: 1px solid var(--border);
      background: transparent; color: var(--text); font-size: 1.25rem; cursor: pointer; flex: 0 0 auto;
    }}
    .modal-close:hover {{ border-color: var(--primary); color: var(--primary); }}
    .modal-chart {{ flex: 1; min-height: 0; position: relative; background: #05070b; }}
    .modal-chart iframe, .modal-chart #lwc {{ position: absolute; inset: 0; width: 100%; height: 100%; border: 0; }}
    .modal-status {{
      position: absolute; inset: 0; display: grid; place-items: center;
      color: var(--muted); font-size: .9rem; padding: 20px; text-align: center;
    }}
    @media (prefers-reduced-motion: reduce) {{
      .modal, .modal-panel {{ transition: none; }}
    }}
  </style>
</head>
<body>
  <div class="wrap">
    <h1>Touch Alerts</h1>
    <p class="meta">Updated <strong>{html.escape(payload['generated_at'])}</strong> · TF <strong>{html.escape(tf_meta)}</strong> · fresh last <strong>{FRESH_BARS}</strong> bar(s)</p>
    <div class="cards">
      <div class="card"><div class="lbl">Hits</div><div class="val">{c['hits']}</div></div>
      <div class="card"><div class="lbl">NDX100</div><div class="val">{c['ndx100']}</div></div>
      <div class="card"><div class="lbl">Futures</div><div class="val">{c['futures']}</div></div>
      <div class="card"><div class="lbl">Crypto</div><div class="val">{c['crypto']}</div></div>
    </div>
    <ul class="rules">
      <li><strong>AR</strong> 急跌反彈：水平射線在信號 K <strong>最高價</strong></li>
      <li><strong>DR</strong> 急漲回落：水平射線在信號 K <strong>最低價</strong></li>
      <li><strong>12 根內</strong>觸原價 → 延伸至信號 K 對側（AR→低、DR→高）</li>
      <li><strong>超過 12 根</strong>才觸原價 → 射線停在觸碰 K，<strong>不延伸</strong>（報告此類）</li>
      <li><strong>不報告</strong> 近 {TOUCH_WINDOW_BARS} 根內新出現的 AR/DR</li>
      <li><strong>報告</strong> 趨勢線影線觸碰</li>
      <li>點擊 <strong>Symbol</strong> 開啟蠟燭圖（含 AR/DR 與趨勢線）</li>
    </ul>

    <h2>AR/DR 晚觸碰（&gt;{TOUCH_WINDOW_BARS} 根後） · {len(ar_dr)}</h2>
    <div class="panel">
      <table>
        <thead>
          <tr><th>Type</th><th>TF</th><th>Group</th><th>Symbol</th><th>Name</th><th class="num">Level</th><th class="num">Bars after</th><th>Time</th></tr>
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
          <tr><th>Side</th><th>TF</th><th>Group</th><th>Symbol</th><th>Name</th><th class="num">Level</th><th>Time</th></tr>
        </thead>
        <tbody>
          {rows_trend()}
        </tbody>
      </table>
    </div>

    <h2>Scan stats</h2>
    <p class="meta">OK scans <strong>{c['ok']}</strong> · Errors <strong>{len(errs)}</strong> · Hits <strong>{c['hits']}</strong></p>
    {err_block}

    <footer>
      Source: <a href="https://github.com/1mckw/AR">1mckw/AR</a> ·
      JSON: <a href="./latest.json">latest.json</a>
    </footer>
  </div>

  <div id="chart-modal" class="modal" hidden aria-hidden="true">
    <div class="modal-panel" role="dialog" aria-modal="true" aria-labelledby="chart-title">
      <div class="modal-head">
        <div>
          <div id="chart-title" class="modal-title">Chart</div>
          <div id="chart-sub" class="modal-sub"></div>
        </div>
        <button type="button" class="modal-close" id="chart-close" aria-label="關閉">×</button>
      </div>
      <div class="modal-chart" id="chart-body">
        <div class="modal-status" id="chart-status">載入中…</div>
        <div id="lwc" hidden></div>
        <iframe id="tv-frame" title="TradingView chart" hidden></iframe>
      </div>
    </div>
  </div>
"""
    charts = payload.get("charts") or {}
    packs_js = (
        "<script>window.CHART_PACKS = "
        + json.dumps(charts, ensure_ascii=False, separators=(",", ":"))
        + ";</script>\n"
    )
    return page_head + packs_js + CHART_MODAL_SCRIPT + "\n</body>\n</html>\n"


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

    jobs = []
    for tf in TIMEFRAMES:
        jobs.extend(("ndx100", s, n, "yahoo", tf) for s, n in ndx100)
        jobs.extend(("futures", s, n, "yahoo", tf) for s, n in FUTURES)
        jobs.extend(("crypto", s, n, "binance", tf) for s, n in crypto)
    tf_label = " + ".join(fmt_tf(tf) for tf in TIMEFRAMES)
    print(f"Scanning {len(jobs)} jobs ({len(ndx100)+len(FUTURES)+len(crypto)} symbols × {tf_label})…", flush=True)

    results: list[dict] = []
    workers = 4 if os.environ.get("GITHUB_ACTIONS") else 6
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futs = [pool.submit(scan_one, g, s, n, src, tf) for g, s, n, src, tf in jobs]
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
                        "timeframe": "?",
                        "bars": 0,
                        "events": [],
                        "error": str(exc),
                    }
                )
            done += 1
            if done % 50 == 0:
                print(f"  progress {done}/{len(jobs)}", flush=True)

    hits = []
    charts: dict[str, dict] = {}
    slim_results: list[dict] = []
    for r in results:
        pack = r.pop("chart", None)
        if pack and r.get("events"):
            key = chart_key(r["group"], r["symbol"], r.get("timeframe") or "")
            charts.setdefault(key, pack)
        slim_results.append(r)
        for ev in r.get("events") or []:
            hits.append(
                {
                    **ev,
                    "group": r["group"],
                    "symbol": r["symbol"],
                    "name": r["name"],
                    "timeframe": ev.get("timeframe") or r.get("timeframe"),
                }
            )
    hits.sort(
        key=lambda x: (
            x["kind"],
            x.get("timeframe", ""),
            x["group"],
            x["symbol"],
            x.get("type", ""),
        )
    )

    payload = {
        "generated_at": datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
        "timeframes": list(TIMEFRAMES),
        "timeframe": "+".join(TIMEFRAMES),
        "params": {
            "bars": BARS,
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
            "jobs": len(jobs),
            "ok": sum(1 for r in slim_results if not r.get("error")),
            "hits": len(hits),
            "charts": len(charts),
        },
        "hits": hits,
        "charts": charts,
        "results": slim_results,
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
