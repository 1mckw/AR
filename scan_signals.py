#!/usr/bin/env python3
"""Touch scanner: NDX100 + SP500 + DJI30 + futures + FX + top 30 crypto on 1H and 1D.

Reports only:
  - AR/DR base-level touch after >12 bars from signal (not new AR/DR within 12 bars)
  - AR/DR near-miss after >12 bars: wick within 0.4% of primary ray, no touch
  - Trend-line wick touches
  - Latest 2–10 consecutive bars with body beyond a trend line
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
import trendlines as tl

ROOT = os.path.dirname(os.path.abspath(__file__))
OUT_DIR = os.path.join(ROOT, "signals")
HISTORY_PATH = os.path.join(OUT_DIR, "history.json")
HISTORY_MAX_ENTRIES = 500

LOOKBACK = ardr.LOOKBACK
VOL_LEN = ardr.VOL_LEN
DROP_PCT = ardr.DROP_PCT
MIN_STREAK = ardr.MIN_STREAK
VOL_MULT = ardr.VOL_MULT
USE_STRUCTURE = ardr.USE_STRUCTURE
TOUCH_WINDOW_BARS = ardr.TOUCH_WINDOW_BARS
NEAR_MISS_TOL_PCT = ardr.NEAR_MISS_TOL_PCT
FRESH_BARS = ardr.FRESH_BARS
BARS = 2000
CHART_BARS = 2000
TIMEFRAMES = ("1h", "1d")
TF_LABEL = {"1h": "1H", "1d": "1D"}
TF_ORDER = {tf: i for i, tf in enumerate(TIMEFRAMES)}
YAHOO_INTERVAL = {"1h": "60m", "1d": "1d"}
BINANCE_INTERVAL = {"1h": "1h", "1d": "1d"}

detect_signals = ardr.detect_signals
resolve_signal_rays = ardr.resolve_signal_rays
collect_late_ar_dr_touches = ardr.collect_late_ar_dr_touches
collect_late_ar_dr_near_misses = ardr.collect_late_ar_dr_near_misses
fresh_range = ardr.fresh_range

PIVOT_HIGH = tl.PIVOT_HIGH
PIVOT_LOW = tl.PIVOT_LOW
MAX_LOOKBACK = tl.MAX_LOOKBACK
MAX_RESISTANCE = tl.MAX_RESISTANCE
MAX_SUPPORT = tl.MAX_SUPPORT
MAX_LINES_PER_PIVOT = tl.MAX_LINES_PER_PIVOT
MIN_LINE_PIVOTS = tl.MIN_LINE_PIVOTS
SHARP_PIERCE_GRACE_BARS = tl.SHARP_PIERCE_GRACE_BARS
TREND_EXCEED_BARS = tl.TREND_EXCEED_BARS
TREND_EXCEED_MIN_BARS = tl.TREND_EXCEED_MIN_BARS
TREND_EXCEED_MAX_BARS = tl.TREND_EXCEED_MAX_BARS
find_pivots = tl.find_pivots
line_price = tl.line_price
build_auto_trend_lines = tl.build_auto_trend_lines
check_line_invalidation = tl.check_line_invalidation
find_trend_touch = tl.find_trend_touch
find_trend_exceed = tl.find_trend_exceed
find_line_break_index = tl.find_line_break_index
line_end_at_break = tl.line_end_at_break

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
    ("BTC=F", "Bitcoin Futures"),
]

FX = [
    ("EURUSD=X", "EUR/USD"),
    ("GBPUSD=X", "GBP/USD"),
    ("USDJPY=X", "USD/JPY"),
    ("AUDUSD=X", "AUD/USD"),
    ("USDCAD=X", "USD/CAD"),
    ("USDCHF=X", "USD/CHF"),
    ("NZDUSD=X", "NZD/USD"),
    ("EURJPY=X", "EUR/JPY"),
    ("GBPJPY=X", "GBP/JPY"),
    ("EURGBP=X", "EUR/GBP"),
    ("AUDJPY=X", "AUD/JPY"),
    ("EURCHF=X", "EUR/CHF"),
    ("USDCNH=X", "USD/CNH"),
    ("USDMXN=X", "USD/MXN"),
    ("USDTRY=X", "USD/TRY"),
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

CRYPTO_TOP_N = 30

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


def _parse_constituents_csv(raw: str) -> list[tuple[str, str]]:
    rows = list(csv.DictReader(io.StringIO(raw)))
    out: list[tuple[str, str]] = []
    for r in rows:
        sym = (r.get("Symbol") or r.get("symbol") or "").strip().replace(".", "-")
        name = (r.get("Name") or r.get("Security") or sym).strip()
        if sym:
            out.append((sym, name))
    return out


def fetch_ndx100() -> list[tuple[str, str]]:
    url = "https://yfiua.github.io/index-constituents/constituents-nasdaq100.csv"
    try:
        raw = http_get(url, timeout=40).decode()
        out = _parse_constituents_csv(raw)
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


def fetch_dji30() -> list[tuple[str, str]]:
    url = "https://yfiua.github.io/index-constituents/constituents-dowjones.csv"
    try:
        raw = http_get(url, timeout=40).decode()
        out = _parse_constituents_csv(raw)
        if len(out) >= 25:
            return out
    except Exception as exc:
        print(f"DJI30 fetch failed: {exc}", flush=True)
    return [
        (s, n)
        for s, n in [
            ("GS", "Goldman Sachs"), ("CAT", "Caterpillar"), ("MSFT", "Microsoft"),
            ("AMGN", "Amgen"), ("HD", "Home Depot"), ("SHW", "Sherwin-Williams"),
            ("MCD", "McDonald's"), ("AXP", "American Express"), ("V", "Visa"),
            ("JPM", "JPMorgan"), ("TRV", "Travelers"), ("UNH", "UnitedHealth"),
            ("AAPL", "Apple"), ("JNJ", "Johnson & Johnson"), ("IBM", "IBM"),
            ("HON", "Honeywell"), ("AMZN", "Amazon"), ("CVX", "Chevron"),
            ("BA", "Boeing"), ("CRM", "Salesforce"), ("NVDA", "Nvidia"),
            ("MMM", "3M"), ("PG", "Procter & Gamble"), ("WMT", "Walmart"),
            ("MRK", "Merck"), ("DIS", "Disney"), ("CSCO", "Cisco"),
            ("KO", "Coca-Cola"), ("VZ", "Verizon"), ("NKE", "Nike"),
        ]
    ]


def fetch_sp500() -> list[tuple[str, str]]:
    urls = [
        "https://yfiua.github.io/index-constituents/constituents-sp500.csv",
        "https://raw.githubusercontent.com/datasets/s-and-p-500-companies/master/data/constituents.csv",
    ]
    for url in urls:
        try:
            raw = http_get(url, timeout=40).decode()
            out = _parse_constituents_csv(raw)
            if len(out) >= 400:
                print(f"SP500 via {url}: {len(out)}", flush=True)
                return out
        except Exception as exc:
            print(f"SP500 fetch failed ({url}): {exc}", flush=True)
    print("SP500 fallback list", flush=True)
    return [
        (s, s)
        for s in [
            "AAPL", "MSFT", "NVDA", "AMZN", "META", "GOOGL", "GOOG", "BRK-B", "AVGO", "TSLA",
            "JPM", "V", "UNH", "XOM", "MA", "PG", "JNJ", "HD", "COST", "ABBV",
            "CRM", "CVX", "WMT", "MRK", "KO", "PEP", "BAC", "LIN", "TMO", "CSCO",
        ]
    ]


def fetch_top_crypto() -> list[tuple[str, str]]:
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
            out = [(t["symbol"], t["symbol"].replace("USDT", "")) for t in usdt[:CRYPTO_TOP_N]]
            if out:
                print(f"Crypto universe via {base}: {len(out)}", flush=True)
                return out
        except Exception as exc:
            print(f"Crypto ticker failed ({base}): {exc}", flush=True)
    print("Crypto universe fallback list", flush=True)
    return list(CRYPTO_FALLBACK[:CRYPTO_TOP_N])


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


def collect_trend_exceeds(candles: list[dict], lines: list[dict]) -> list[dict]:
    if not candles:
        return []
    hits = []
    for line in lines:
        exc = find_trend_exceed(candles, line)
        if not exc:
            continue
        label = "阻力趨勢線超出" if line["type"] == "resistance" else "支撐趨勢線超出"
        hits.append(
            {
                "kind": "trend_exceed",
                "label": label,
                "type": line["type"],
                "time": exc["time"],
                "index": exc["index"],
                "level": exc["price"],
                "close": exc["close"],
                "exceed_bars": exc["bars"],
            }
        )
    return hits


def build_chart_pack(candles: list[dict], signals: list[dict], lines: list[dict]) -> dict:
    """Compact candles + AR/DR rays + trend lines for the HTML chart modal."""
    last_time = int(candles[-1]["time"]) if candles else 0
    rays = [
        ardr.signal_to_chart_ray(sig, candles, last_time)
        for sig in signals
    ]

    trend = []
    for line in lines:
        invalidated = check_line_invalidation(candles, line)
        end_time, end_price = line_end_at_break(candles, line)
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


def hit_archive_key(h: dict) -> str:
    return (
        f"{h.get('group','')}:{h.get('symbol','')}:{h.get('timeframe','')}:"
        f"{h.get('kind','')}:{h.get('type','')}:{h.get('time','')}"
    )


def slim_hit_for_archive(h: dict, seen_at: str) -> dict:
    """Persist only fields needed for the history rail / archive JSON."""
    out = {
        "key": hit_archive_key(h),
        "kind": h.get("kind"),
        "type": h.get("type"),
        "group": h.get("group"),
        "symbol": h.get("symbol"),
        "name": h.get("name"),
        "timeframe": h.get("timeframe"),
        "level": h.get("level"),
        "time": h.get("time"),
        "seen_at": seen_at,
    }
    if h.get("kind") == "ar_dr_touch":
        out["bars_after_signal"] = h.get("bars_after_signal")
    if h.get("kind") == "ar_dr_near":
        out["bars_after_signal"] = h.get("bars_after_signal")
        out["gap_pct"] = h.get("gap_pct")
    if h.get("kind") == "trend_exceed":
        out["exceed_bars"] = h.get("exceed_bars")
    return out


def load_history_archive() -> dict:
    if not os.path.isfile(HISTORY_PATH):
        return {"updated_at": "", "count": 0, "hits": []}
    try:
        with open(HISTORY_PATH, encoding="utf-8") as f:
            raw = json.load(f)
        if isinstance(raw, dict):
            hits = list(raw.get("hits") or [])
            return {
                "updated_at": str(raw.get("updated_at") or ""),
                "count": int(raw.get("count") or len(hits)),
                "hits": hits,
            }
        if isinstance(raw, list):
            return {"updated_at": "", "count": len(raw), "hits": raw}
    except (OSError, json.JSONDecodeError, TypeError, ValueError):
        pass
    return {"updated_at": "", "count": 0, "hits": []}


def update_history_archive(hits: list[dict], generated_at: str) -> dict:
    """Merge scan hits into signals/history.json (deduped, newest first)."""
    os.makedirs(OUT_DIR, exist_ok=True)
    existing: list[dict] = []
    if os.path.isfile(HISTORY_PATH):
        try:
            with open(HISTORY_PATH, encoding="utf-8") as f:
                raw = json.load(f)
            if isinstance(raw, dict):
                existing = list(raw.get("hits") or [])
            elif isinstance(raw, list):
                existing = raw
        except (OSError, json.JSONDecodeError, TypeError):
            existing = []

    by_key: dict[str, dict] = {}
    for h in existing:
        key = str(h.get("key") or hit_archive_key(h))
        if not key or key == ":::::":
            continue
        entry = dict(h)
        entry["key"] = key
        by_key[key] = entry

    for h in hits:
        entry = slim_hit_for_archive(h, generated_at)
        key = entry["key"]
        prev = by_key.get(key)
        if prev:
            entry["first_seen"] = prev.get("first_seen") or prev.get("seen_at") or generated_at
        else:
            entry["first_seen"] = generated_at
        entry["seen_at"] = generated_at
        by_key[key] = entry

    merged = sorted(
        by_key.values(),
        key=lambda x: (int(x.get("time") or 0), str(x.get("seen_at") or "")),
        reverse=True,
    )[:HISTORY_MAX_ENTRIES]

    payload = {
        "updated_at": generated_at,
        "count": len(merged),
        "max": HISTORY_MAX_ENTRIES,
        "hits": merged,
    }
    with open(HISTORY_PATH, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    return payload


def chart_key(group: str, symbol: str, timeframe: str) -> str:
    return f"{group}|{symbol}|{timeframe}"


def build_symbol_catalog(results: list[dict], charts: dict) -> list[dict]:
    """All successfully scanned symbols for the report search FAB."""
    seen: set[tuple[str, str, str]] = set()
    catalog: list[dict] = []
    for r in results:
        if r.get("error"):
            continue
        group = str(r.get("group") or "")
        symbol = str(r.get("symbol") or "")
        tf = str(r.get("timeframe") or "")
        if not group or not symbol or not tf:
            continue
        key = (group, symbol, tf)
        if key in seen:
            continue
        seen.add(key)
        ck = chart_key(group, symbol, tf)
        has_hit = bool(r.get("events"))
        catalog.append(
            {
                "group": group,
                "symbol": symbol,
                "name": r.get("name") or symbol,
                "timeframe": tf,
                "hasHit": has_hit,
                "hasChart": ck in charts,
            }
        )
    catalog.sort(
        key=lambda x: (
            not x["hasHit"],
            TF_ORDER.get(x["timeframe"], 99),
            x["symbol"],
        )
    )
    return catalog


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
        near = collect_late_ar_dr_near_misses(candles, signals)
        lines = build_auto_trend_lines(candles)
        trend = collect_trend_touches(candles, lines)
        exceed = collect_trend_exceeds(candles, lines)
        events = late + near + trend + exceed
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
    "ZB=F": "CBOT:ZB1!", "ZN=F": "CBOT:ZN1!", "BTC=F": "CME:BTC1!",
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
    if (group === "fx") return "FX_IDC:" + symbol.replace("=X", "");
    if (group === "dji30" || group === "sp500") return symbol;
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
    if (kind === "ar_dr_near") return (type || "AR/DR") + " 接近未觸";
    if (kind === "trend_exceed") {
      if (type === "resistance") return "阻力超出";
      if (type === "support") return "支撐超出";
    }
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
      if (seg.t0 == null || seg.t1 == null || seg.t0 >= seg.t1) continue;
      drawSegment(
        seg.t0,
        seg.price,
        seg.t1,
        seg.price,
        rayColor(ray.type, seg.active),
        seg.side === "upper" ? 1 : 1,
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
      layout: { background: { color: "#000000" }, textColor: "#7a93a8" },
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

  const CATALOG = window.SYMBOL_CATALOG || [];
  const GROUP_LABEL = { ndx100: "NDX100", sp500: "SP500", dji30: "DJI30", crypto: "Crypto", futures: "Futures", fx: "FX" };
  const fab = document.getElementById("symbolFab");
  const searchOverlay = document.getElementById("symbolOverlay");
  const searchInput = document.getElementById("symbolSearch");
  const searchList = document.getElementById("symbolList");
  const searchClose = document.getElementById("symbolSearchClose");
  let searchHi = -1;
  let searchTimer = null;

  function tfLabel(tf) {
    return tf === "1d" ? "1D" : "1H";
  }

  function openSymbolSearch() {
    if (!searchOverlay) return;
    searchOverlay.hidden = false;
    searchInput.value = "";
    searchHi = -1;
    renderSearchList(CATALOG.slice(0, 80), CATALOG.length ? "輸入代碼或名稱篩選…" : "無掃描商品");
    requestAnimationFrame(() => searchInput.focus());
  }

  function closeSymbolSearch() {
    if (!searchOverlay) return;
    searchOverlay.hidden = true;
    searchHi = -1;
    clearTimeout(searchTimer);
  }

  function filterCatalog(q) {
    const s = q.trim().toLowerCase();
    if (!s) return CATALOG.slice(0, 80);
    return CATALOG.filter((item) => {
      const hay = [
        item.symbol,
        item.name,
        item.group,
        GROUP_LABEL[item.group] || item.group,
        tfLabel(item.timeframe),
      ].join(" ").toLowerCase();
      return hay.includes(s);
    }).slice(0, 80);
  }

  function renderSearchList(items, emptyMsg) {
    if (!searchList) return;
    searchList.innerHTML = "";
    if (!items.length) {
      searchList.innerHTML = `<li class="empty">${escapeHtml(emptyMsg)}</li>`;
      searchHi = -1;
      return;
    }
    searchHi = 0;
    items.forEach((item, i) => {
      const li = document.createElement("li");
      li.dataset.index = String(i);
      li.className = i === 0 ? "active" : "";
      const hit = item.hasHit ? '<span class="hit-badge">Hit</span>' : "";
      li.innerHTML =
        `<span class="sym">${escapeHtml(item.symbol)}</span>` +
        `<span class="ex">${escapeHtml(GROUP_LABEL[item.group] || item.group)} · ${escapeHtml(tfLabel(item.timeframe))}</span>` +
        hit +
        `<span class="desc">${escapeHtml(item.name || "")}</span>`;
      li.addEventListener("mousedown", (ev) => {
        ev.preventDefault();
        pickCatalogItem(item);
      });
      searchList.appendChild(li);
    });
  }

  function pickCatalogItem(item) {
    closeSymbolSearch();
    const sel = `.sym-btn[data-symbol="${CSS.escape(item.symbol)}"][data-group="${CSS.escape(item.group)}"][data-tf="${CSS.escape(item.timeframe)}"]`;
    const hitBtn = document.querySelector(sel);
    if (hitBtn) {
      hitBtn.scrollIntoView({ behavior: "smooth", block: "center" });
      hitBtn.classList.add("flash");
      setTimeout(() => hitBtn.classList.remove("flash"), 1600);
    }
    const btn = document.createElement("button");
    btn.dataset.symbol = item.symbol;
    btn.dataset.group = item.group;
    btn.dataset.name = item.name || item.symbol;
    btn.dataset.tf = item.timeframe;
    openChart(btn);
  }

  function scheduleSearch() {
    clearTimeout(searchTimer);
    searchTimer = setTimeout(() => {
      const items = filterCatalog(searchInput.value);
      renderSearchList(items, "無符合商品");
    }, 180);
  }

  if (fab) fab.addEventListener("click", openSymbolSearch);
  if (searchClose) searchClose.addEventListener("click", closeSymbolSearch);
  if (searchOverlay) {
    searchOverlay.addEventListener("mousedown", (ev) => {
      if (ev.target === searchOverlay) closeSymbolSearch();
    });
  }
  if (searchInput) {
    searchInput.addEventListener("input", scheduleSearch);
    searchInput.addEventListener("keydown", (ev) => {
      const opts = [...(searchList?.querySelectorAll("li[data-index]") || [])];
      if (ev.key === "ArrowDown" && opts.length) {
        ev.preventDefault();
        searchHi = Math.min(searchHi + 1, opts.length - 1);
        opts.forEach((li, i) => li.classList.toggle("active", i === searchHi));
        opts[searchHi].scrollIntoView({ block: "nearest" });
      } else if (ev.key === "ArrowUp" && opts.length) {
        ev.preventDefault();
        searchHi = Math.max(searchHi - 1, 0);
        opts.forEach((li, i) => li.classList.toggle("active", i === searchHi));
        opts[searchHi].scrollIntoView({ block: "nearest" });
      } else if (ev.key === "Enter") {
        const li = opts[searchHi] || opts[0];
        if (li) li.dispatchEvent(new MouseEvent("mousedown", { bubbles: true }));
      }
    });
  }

  document.addEventListener("keydown", (ev) => {
    if (ev.key === "Escape") {
      if (searchOverlay && !searchOverlay.hidden) {
        closeSymbolSearch();
        return;
      }
      if (modal.classList.contains("open")) closeModal();
    }
  });
})();
</script>
"""

GROUP_FILTER_SCRIPT = r"""
<script>
(function () {
  const KEY = "tv_ar_group_filter_v1";
  const ALLOWED = new Set(["all", "ndx100", "sp500", "dji30"]);
  const bar = document.getElementById("groupFilters");
  const modeBar = document.getElementById("viewMode");

  let current = "all";
  try {
    const saved = localStorage.getItem(KEY);
    if (saved && ALLOWED.has(saved)) current = saved;
  } catch (_) {}

  function apply(group) {
    current = ALLOWED.has(group) ? group : "all";
    try { localStorage.setItem(KEY, current); } catch (_) {}
    if (bar) {
      bar.querySelectorAll("button[data-group]").forEach((btn) => {
        btn.classList.toggle("active", btn.dataset.group === current);
      });
    }
    document.querySelectorAll("tbody[data-section]").forEach((tbody) => {
      let visible = 0;
      tbody.querySelectorAll("tr[data-group]").forEach((tr) => {
        const on = current === "all" || tr.dataset.group === current;
        tr.hidden = !on;
        if (on) visible += 1;
      });
      const empty = tbody.querySelector("tr:not([data-group])");
      if (empty) empty.hidden = visible > 0;
      const title = document.querySelector(`.section-title[data-section="${tbody.dataset.section}"]`);
      if (title) {
        const base = title.dataset.base || title.textContent.split(" · ")[0];
        title.textContent = current === "all"
          ? `${base} · ${title.dataset.total || visible}`
          : `${base} · ${visible}`;
      }
    });
  }

  if (modeBar) {
    modeBar.addEventListener("click", (ev) => {
      const btn = ev.target.closest("button[data-mode]");
      if (!btn) return;
      const target = document.getElementById(btn.dataset.mode === "history" ? "view-history" : "view-latest");
      modeBar.querySelectorAll("button[data-mode]").forEach((b) => {
        const on = b === btn;
        b.classList.toggle("active", on);
        b.setAttribute("aria-selected", on ? "true" : "false");
      });
      if (target) target.scrollIntoView({ behavior: "smooth", block: "start" });
    });
  }
  if (bar) {
    bar.addEventListener("click", (ev) => {
      const btn = ev.target.closest("button[data-group]");
      if (!btn) return;
      apply(btn.dataset.group);
    });
  }

  apply(current);
})();
</script>
"""


def render_html(payload: dict) -> str:
    hits = payload["hits"]
    ar_dr = [h for h in hits if h["kind"] == "ar_dr_touch"]
    ar_near = [h for h in hits if h["kind"] == "ar_dr_near"]
    trend = [h for h in hits if h["kind"] == "trend_touch"]
    exceed = [h for h in hits if h["kind"] == "trend_exceed"]
    errs = [r for r in payload["results"] if r.get("error")]
    c = payload["counts"]
    hist_payload = load_history_archive()
    hist_hits = list(hist_payload.get("hits") or [])

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
                f'<tr data-group="{html.escape(str(h.get("group", "")), quote=True)}">'
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

    def rows_ar_near() -> str:
        if not ar_near:
            return (
                f'<tr><td colspan="9" class="empty">目前無 AR/DR 接近未觸'
                f"（&gt;{TOUCH_WINDOW_BARS} 根後）</td></tr>"
            )
        out = []
        for h in ar_near:
            cls = "ar" if h.get("type") == "AR" else "dr"
            out.append(
                f'<tr data-group="{html.escape(str(h.get("group", "")), quote=True)}">'
                f'<td><span class="tag {cls}">{html.escape(str(h.get("type", "")))}</span></td>'
                f"<td>{html.escape(fmt_tf(h.get('timeframe', '')))}</td>"
                f"<td>{html.escape(h.get('group', ''))}</td>"
                f"<td>{sym_btn(h)}</td>"
                f"<td>{html.escape(h.get('name', ''))}</td>"
                f"<td class=\"num\">{fmt_num(float(h['level']))}</td>"
                f"<td class=\"num\">{float(h.get('gap_pct', 0)):.3g}%</td>"
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
                f'<tr data-group="{html.escape(str(h.get("group", "")), quote=True)}">'
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

    def rows_trend_exceed() -> str:
        if not exceed:
            return f'<tr><td colspan="8" class="empty">目前無最新 {TREND_EXCEED_MIN_BARS}–{TREND_EXCEED_MAX_BARS} 根超出趨勢線</td></tr>'
        out = []
        for h in exceed:
            cls = "resist" if h.get("type") == "resistance" else "support"
            out.append(
                f'<tr data-group="{html.escape(str(h.get("group", "")), quote=True)}">'
                f'<td><span class="tag {cls}">{html.escape(str(h.get("type", "")))}</span></td>'
                f"<td>{html.escape(fmt_tf(h.get('timeframe', '')))}</td>"
                f"<td>{html.escape(h.get('group', ''))}</td>"
                f"<td>{sym_btn(h)}</td>"
                f"<td>{html.escape(h.get('name', ''))}</td>"
                f"<td class=\"num\">{fmt_num(float(h['level']))}</td>"
                f"<td class=\"num\">{int(h.get('exceed_bars', TREND_EXCEED_BARS))}</td>"
                f"<td>{html.escape(fmt_ts(int(h['time'])))}</td>"
                "</tr>"
            )
        return "\n".join(out)

    def kind_label(h: dict) -> tuple[str, str]:
        kind = h.get("kind")
        typ = str(h.get("type") or "")
        if kind == "ar_dr_touch":
            cls = "ar" if typ == "AR" else "dr"
            return cls, f"{typ} 晚觸碰" if typ else "AR/DR 晚觸碰"
        if kind == "ar_dr_near":
            cls = "ar" if typ == "AR" else "dr"
            return cls, f"{typ} 接近未觸" if typ else "AR/DR 接近未觸"
        if kind == "trend_exceed":
            cls = "resist" if typ == "resistance" else "support"
            return cls, "阻力超出" if typ == "resistance" else "支撐超出"
        if kind == "trend_touch":
            cls = "resist" if typ == "resistance" else "support"
            return cls, "阻力觸碰" if typ == "resistance" else "支撐觸碰"
        return "ar", str(kind or "事件")

    def rows_history() -> str:
        if not hist_hits:
            return '<tr><td colspan="8" class="empty">尚無歷史紀錄</td></tr>'
        out = []
        for h in hist_hits[:200]:
            cls, label = kind_label(h)
            try:
                level = fmt_num(float(h["level"]))
            except (TypeError, ValueError, KeyError):
                level = "—"
            try:
                tstr = fmt_ts(int(h["time"]))
            except (TypeError, ValueError, KeyError):
                tstr = "—"
            out.append(
                f'<tr data-group="{html.escape(str(h.get("group", "")), quote=True)}">'
                f'<td><span class="tag {cls}">{html.escape(label)}</span></td>'
                f"<td>{html.escape(fmt_tf(h.get('timeframe', '')))}</td>"
                f"<td>{html.escape(str(h.get('group', '')))}</td>"
                f"<td>{sym_btn(h)}</td>"
                f"<td>{html.escape(str(h.get('name', '')))}</td>"
                f'<td class="num">{level}</td>'
                f"<td>{html.escape(tstr)}</td>"
                f"<td>{html.escape(str(h.get('seen_at', '') or '—'))}</td>"
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
  <meta http-equiv="refresh" content="1800" />
  <title>AR/DR Touch Alerts</title>
  <link rel="preconnect" href="https://fonts.googleapis.com" />
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin />
  <link href="https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;600;700&family=Space+Grotesk:wght@400;500;600;700&display=swap" rel="stylesheet" />
  <style>
    :root {{
      --bg: #000000; --bg-deep: #020408;
      --panel: rgba(8, 12, 20, 0.58); --surface: rgba(12, 18, 28, 0.68);
      --glass-blur: blur(24px) saturate(1.45);
      --border: rgba(0, 255, 213, 0.18); --border-strong: rgba(0, 255, 213, 0.34);
      --text: #eefdfb; --muted: #7a93a8; --primary: #00f0c8;
      --ar: #00e896; --dr: #ff4d6d; --support: #00e896; --resist: #ff4d6d;
      --glow: 0 0 28px rgba(0, 240, 200, 0.28);
      --glass-shadow: inset 0 1px 0 rgba(255,255,255,0.07), 0 10px 40px rgba(0,0,0,0.55), 0 0 30px rgba(0,240,200,0.07);
    }}
    * {{ box-sizing: border-box; margin: 0; padding: 0; }}
    body {{
      font-family: "Space Grotesk", system-ui, sans-serif;
      background-color: var(--bg);
      background-image:
        radial-gradient(900px 520px at 8% -15%, rgba(0, 240, 200, 0.14), transparent 58%),
        radial-gradient(700px 480px at 92% 108%, rgba(255, 45, 106, 0.11), transparent 55%),
        radial-gradient(600px 400px at 55% 42%, rgba(56, 189, 248, 0.06), transparent 60%),
        linear-gradient(180deg, #030508 0%, #000000 100%);
      color: var(--text); min-height: 100vh; padding: 28px 18px 48px;
      line-height: 1.45; position: relative;
    }}
    body::before {{
      content: ""; position: fixed; inset: 0; pointer-events: none; z-index: 0; opacity: 0.42;
      background-image:
        linear-gradient(rgba(0, 255, 213, 0.045) 1px, transparent 1px),
        linear-gradient(90deg, rgba(0, 255, 213, 0.045) 1px, transparent 1px);
      background-size: 48px 48px;
      mask-image: radial-gradient(ellipse at center, black 15%, transparent 78%);
    }}
    .wrap {{ max-width: 1100px; margin: 0 auto; position: relative; z-index: 1; }}
    h1 {{
      font-size: 1.55rem; font-weight: 700; letter-spacing: -.03em;
      background: linear-gradient(90deg, #e8f7f4, var(--primary));
      -webkit-background-clip: text; background-clip: text; color: transparent;
      filter: drop-shadow(0 0 18px rgba(0, 240, 200, 0.35));
    }}
    .meta {{ color: var(--muted); font-size: .9rem; margin: 8px 0 18px; }}
    .meta strong {{ color: var(--primary); font-weight: 600; }}
    .group-filters {{
      display: flex; flex-wrap: wrap; gap: 6px; margin: -6px 0 18px;
    }}
    .group-filters button {{
      font-family: inherit; cursor: pointer; height: 30px; padding: 0 12px;
      border-radius: 999px; border: 1px solid var(--border-strong);
      background: rgba(6, 10, 18, 0.55); color: var(--muted); font-size: .78rem; font-weight: 600;
      backdrop-filter: blur(12px); -webkit-backdrop-filter: blur(12px);
    }}
    .group-filters button:hover {{ color: var(--text); border-color: var(--primary); }}
    .group-filters button.active {{
      color: #04110e; border-color: transparent;
      background: linear-gradient(135deg, #00f0c8, #00b894);
      box-shadow: 0 2px 12px rgba(0, 240, 200, 0.28);
    }}
    .view-mode {{
      display: flex; gap: 8px; margin: 0 0 14px;
    }}
    .view-mode button {{
      flex: 1; max-width: 180px; height: 40px; padding: 0 16px; border-radius: 12px; cursor: pointer;
      border: 1px solid var(--border-strong); background: rgba(6, 10, 18, 0.55);
      color: var(--muted); font: inherit; font-size: .95rem; font-weight: 700;
      backdrop-filter: blur(12px); -webkit-backdrop-filter: blur(12px);
    }}
    .view-mode button:hover {{ color: var(--text); border-color: var(--primary); }}
    .view-mode button.active {{
      color: #04110e; border-color: transparent;
      background: linear-gradient(135deg, #00f0c8, #00b894);
      box-shadow: 0 2px 12px rgba(0, 240, 200, 0.28);
    }}
    .view-mode button .n {{
      margin-left: 6px; font-variant-numeric: tabular-nums; opacity: .85;
    }}
    #view-history {{
      margin-top: 28px; padding-top: 8px;
      border-top: 1px dashed rgba(0, 255, 213, 0.22);
      scroll-margin-top: 18px;
    }}
    #view-latest {{ scroll-margin-top: 18px; }}
    .cards {{
      display: grid; grid-template-columns: repeat(7, 1fr); gap: 10px; margin-bottom: 22px;
    }}
    @media (max-width: 1100px) {{ .cards {{ grid-template-columns: repeat(4, 1fr); }} }}
    @media (max-width: 720px) {{ .cards {{ grid-template-columns: 1fr 1fr; }} }}
    .card {{
      background: var(--panel); border: 1px solid var(--border-strong); border-radius: 12px; padding: 12px 14px;
      backdrop-filter: var(--glass-blur); -webkit-backdrop-filter: var(--glass-blur);
      box-shadow: var(--glass-shadow);
    }}
    .card .lbl {{ font-size: .65rem; color: var(--muted); text-transform: uppercase; letter-spacing: .08em; }}
    .card .val {{
      font-family: "JetBrains Mono", monospace; font-size: 1.25rem; font-weight: 700; margin-top: 4px;
      text-shadow: 0 0 16px rgba(0, 240, 200, 0.22);
    }}
    h2 {{
      font-size: 1.05rem; margin: 22px 0 10px; font-weight: 650;
      color: var(--text); text-shadow: 0 0 20px rgba(0, 240, 200, 0.28);
    }}
    .panel {{
      background: var(--panel); border: 1px solid var(--border-strong); border-radius: 14px; overflow: hidden;
      backdrop-filter: var(--glass-blur); -webkit-backdrop-filter: var(--glass-blur);
      box-shadow: var(--glass-shadow);
    }}
    table {{ width: 100%; border-collapse: collapse; font-size: .84rem; }}
    th, td {{ padding: 9px 12px; text-align: left; border-bottom: 1px solid rgba(0,240,200,.08); }}
    th {{
      background: rgba(12, 18, 28, 0.75); color: var(--muted); font-size: .68rem;
      text-transform: uppercase; letter-spacing: .06em; font-weight: 600;
      backdrop-filter: blur(12px); -webkit-backdrop-filter: blur(12px);
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
      padding: 16px; background: rgba(0, 0, 0, 0.62);
      backdrop-filter: blur(14px) saturate(1.2); -webkit-backdrop-filter: blur(14px) saturate(1.2);
      opacity: 0; pointer-events: none; transition: opacity .2s ease;
    }}
    .modal.open {{ opacity: 1; pointer-events: auto; }}
    .modal-panel {{
      width: min(1100px, 100%); height: min(720px, 92vh);
      background: rgba(8, 12, 20, 0.62); border: 1px solid var(--border-strong); border-radius: 16px;
      backdrop-filter: var(--glass-blur); -webkit-backdrop-filter: var(--glass-blur);
      display: flex; flex-direction: column; overflow: hidden;
      box-shadow: var(--glass-shadow), 0 0 48px rgba(0, 240, 200, 0.12);
      transform: translateY(10px) scale(.985); transition: transform .22s ease;
    }}
    .modal.open .modal-panel {{ transform: none; }}
    .modal-head {{
      display: flex; align-items: flex-start; justify-content: space-between; gap: 12px;
      padding: 14px 16px; border-bottom: 1px solid rgba(0,240,200,.1);
      background: rgba(12, 18, 28, 0.65); backdrop-filter: blur(12px);
    }}
    .modal-title {{ font-size: 1.05rem; font-weight: 650; letter-spacing: -.02em; }}
    .modal-sub {{ color: var(--muted); font-size: .82rem; margin-top: 4px; }}
    .modal-sub strong {{ color: var(--primary); font-weight: 600; }}
    .modal-close {{
      width: 40px; height: 40px; border-radius: 10px; border: 1px solid var(--border);
      background: transparent; color: var(--text); font-size: 1.25rem; cursor: pointer; flex: 0 0 auto;
    }}
    .modal-close:hover {{ border-color: var(--primary); color: var(--primary); }}
    .modal-chart {{ flex: 1; min-height: 0; position: relative; background: #000000; }}
    .modal-chart iframe, .modal-chart #lwc {{ position: absolute; inset: 0; width: 100%; height: 100%; border: 0; }}
    .modal-status {{
      position: absolute; inset: 0; display: grid; place-items: center;
      color: var(--muted); font-size: .9rem; padding: 20px; text-align: center;
    }}
    @media (prefers-reduced-motion: reduce) {{
      .modal, .modal-panel {{ transition: none; }}
    }}
    .sym-btn.flash code {{
      background: rgba(0,240,200,.18); border-radius: 4px;
      box-shadow: 0 0 0 2px rgba(0,240,200,.25);
    }}
    .search-fab {{
      position: fixed; right: 18px; bottom: 22px; z-index: 70;
      display: flex; align-items: center; gap: 10px;
      padding: 12px 16px 12px 14px; border-radius: 14px;
      border: 1px solid var(--border-strong);
      background: rgba(6, 10, 18, 0.58);
      backdrop-filter: var(--glass-blur); -webkit-backdrop-filter: var(--glass-blur);
      box-shadow: var(--glass-shadow), var(--glow);
      color: var(--text); cursor: pointer; font: inherit;
      transition: transform .15s ease, border-color .15s ease, box-shadow .15s ease;
    }}
    .search-fab:hover {{
      transform: translateY(-2px); border-color: var(--primary);
      box-shadow: 0 16px 36px rgba(0,0,0,.5), 0 0 28px rgba(0,240,200,.22);
    }}
    .search-fab-icon {{
      width: 36px; height: 36px; border-radius: 10px; flex-shrink: 0;
      display: grid; place-items: center;
      background: rgba(0,240,200,.1); border: 1px solid rgba(0,240,200,.2);
      color: var(--primary); font-size: 1.05rem;
    }}
    .search-fab-meta {{ display: flex; flex-direction: column; align-items: flex-start; gap: 2px; }}
    .search-fab-label {{
      font-size: .58rem; font-weight: 700; color: var(--primary);
      text-transform: uppercase; letter-spacing: .1em;
    }}
    .search-fab-hint {{
      font-family: "JetBrains Mono", monospace; font-size: .86rem; font-weight: 600;
    }}
    .search-overlay {{
      position: fixed; inset: 0; z-index: 85;
      background: rgba(0, 0, 0, 0.62);
      backdrop-filter: blur(14px) saturate(1.2); -webkit-backdrop-filter: blur(14px) saturate(1.2);
      display: flex; align-items: flex-start; justify-content: center;
      padding: 10vh 16px 16px;
    }}
    .search-overlay[hidden] {{ display: none !important; }}
    .search-modal {{
      width: min(520px, 100%); max-height: min(70vh, 560px);
      background: rgba(8, 12, 20, 0.62); border: 1px solid var(--border-strong); border-radius: 16px;
      backdrop-filter: var(--glass-blur); -webkit-backdrop-filter: var(--glass-blur);
      box-shadow: var(--glass-shadow), 0 0 48px rgba(0, 240, 200, 0.12);
      display: flex; flex-direction: column; overflow: hidden;
    }}
    .search-modal-head {{
      display: flex; align-items: center; gap: 8px;
      padding: 14px; border-bottom: 1px solid rgba(0,240,200,.1);
      background: rgba(12, 18, 28, 0.65); flex-shrink: 0;
      backdrop-filter: blur(12px);
    }}
    #symbolSearch {{
      flex: 1; height: 42px; padding: 8px 14px; border-radius: 10px;
      border: 1px solid rgba(0,240,200,.2); background: rgba(4, 8, 14, 0.55); color: var(--text);
      font-family: "JetBrains Mono", monospace; font-size: .92rem;
      backdrop-filter: blur(12px);
    }}
    #symbolSearch:focus {{
      outline: none; border-color: var(--primary);
      box-shadow: 0 0 0 3px rgba(0,240,200,.15);
    }}
    #symbolSearchClose {{
      height: 42px; padding: 0 14px; border-radius: 10px;
      border: 1px solid var(--border); background: transparent; color: var(--text);
      cursor: pointer; font: inherit;
    }}
    #symbolSearchClose:hover {{ border-color: var(--primary); color: var(--primary); }}
    #symbolList {{
      list-style: none; margin: 0; padding: 8px; overflow-y: auto; flex: 1;
    }}
    #symbolList li {{
      padding: 10px 12px; border-radius: 10px; cursor: pointer;
      border: 1px solid transparent;
    }}
    #symbolList li:hover, #symbolList li.active {{
      background: rgba(0,240,200,.06); border-color: rgba(0,240,200,.14);
    }}
    #symbolList li.empty {{ color: var(--muted); cursor: default; text-align: center; }}
    #symbolList .sym {{
      font-family: "JetBrains Mono", monospace; font-weight: 700; color: var(--primary);
      margin-right: 8px;
    }}
    #symbolList .ex {{ font-size: .78rem; color: var(--muted); }}
    #symbolList .desc {{
      display: block; font-size: .78rem; color: var(--muted); margin-top: 3px;
      white-space: nowrap; overflow: hidden; text-overflow: ellipsis;
    }}
    #symbolList .hit-badge {{
      display: inline-block; margin-left: 6px; font-size: .62rem; font-weight: 700;
      padding: 1px 6px; border-radius: 4px;
      background: rgba(0,240,200,.12); color: var(--primary);
      vertical-align: middle;
    }}
    @media (max-width: 520px) {{
      .search-fab-hint {{ display: none; }}
      .search-fab {{ padding: 12px; border-radius: 999px; }}
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
      <div class="card"><div class="lbl">SP500</div><div class="val">{c.get('sp500', 0)}</div></div>
      <div class="card"><div class="lbl">DJI30</div><div class="val">{c.get('dji30', 0)}</div></div>
      <div class="card"><div class="lbl">Futures</div><div class="val">{c['futures']}</div></div>
      <div class="card"><div class="lbl">FX</div><div class="val">{c.get('fx', 0)}</div></div>
      <div class="card"><div class="lbl">Crypto</div><div class="val">{c['crypto']}</div></div>
    </div>
    <div class="view-mode" id="viewMode" role="navigation" aria-label="跳至最新或歷史">
      <button type="button" data-mode="latest" class="active">最新<span class="n">{len(hits)}</span></button>
      <button type="button" data-mode="history">歷史<span class="n">{len(hist_hits)}</span></button>
    </div>
    <div class="group-filters" id="groupFilters" role="group" aria-label="指數篩選">
      <button type="button" data-group="all" class="active">全部</button>
      <button type="button" data-group="ndx100">NDX100</button>
      <button type="button" data-group="sp500">SP500</button>
      <button type="button" data-group="dji30">DJI30</button>
    </div>

    <div id="view-latest">
    <h2 class="section-title" data-section="exceed" data-base="最新 {TREND_EXCEED_MIN_BARS}–{TREND_EXCEED_MAX_BARS} 根超出趨勢線" data-total="{len(exceed)}">最新 {TREND_EXCEED_MIN_BARS}–{TREND_EXCEED_MAX_BARS} 根超出趨勢線 · {len(exceed)}</h2>
    <div class="panel">
      <table>
        <thead>
          <tr><th>Side</th><th>TF</th><th>Group</th><th>Symbol</th><th>Name</th><th class="num">Level</th><th class="num">Bars</th><th>Time</th></tr>
        </thead>
        <tbody data-section="exceed">
          {rows_trend_exceed()}
        </tbody>
      </table>
    </div>

    <h2 class="section-title" data-section="ar_dr" data-base="AR/DR 晚觸碰（&gt;{TOUCH_WINDOW_BARS} 根後）" data-total="{len(ar_dr)}">AR/DR 晚觸碰（&gt;{TOUCH_WINDOW_BARS} 根後） · {len(ar_dr)}</h2>
    <div class="panel">
      <table>
        <thead>
          <tr><th>Type</th><th>TF</th><th>Group</th><th>Symbol</th><th>Name</th><th class="num">Level</th><th class="num">Bars after</th><th>Time</th></tr>
        </thead>
        <tbody data-section="ar_dr">
          {rows_ar_dr()}
        </tbody>
      </table>
    </div>

    <h2 class="section-title" data-section="ar_near" data-base="AR/DR 接近未觸（&gt;{TOUCH_WINDOW_BARS} 根後）" data-total="{len(ar_near)}">AR/DR 接近未觸（&gt;{TOUCH_WINDOW_BARS} 根後） · {len(ar_near)}</h2>
    <p class="meta">主引線仍有效 · 影線距離 ≤ {NEAR_MISS_TOL_PCT * 100:g}% · 未觸碰</p>
    <div class="panel">
      <table>
        <thead>
          <tr><th>Type</th><th>TF</th><th>Group</th><th>Symbol</th><th>Name</th><th class="num">Level</th><th class="num">Gap</th><th class="num">Bars after</th><th>Time</th></tr>
        </thead>
        <tbody data-section="ar_near">
          {rows_ar_near()}
        </tbody>
      </table>
    </div>

    <h2 class="section-title" data-section="trend" data-base="趨勢線觸碰" data-total="{len(trend)}">趨勢線觸碰 · {len(trend)}</h2>
    <div class="panel">
      <table>
        <thead>
          <tr><th>Side</th><th>TF</th><th>Group</th><th>Symbol</th><th>Name</th><th class="num">Level</th><th>Time</th></tr>
        </thead>
        <tbody data-section="trend">
          {rows_trend()}
        </tbody>
      </table>
    </div>
    </div>

    <div id="view-history">
    <h2 class="section-title" data-section="history" data-base="歷史紀錄" data-total="{len(hist_hits)}">歷史紀錄 · {len(hist_hits)}</h2>
    <p class="meta">歸檔自 <code>history.json</code>{(' · 更新 ' + html.escape(str(hist_payload.get('updated_at') or ''))) if hist_payload.get('updated_at') else ''} · 累積最多 {HISTORY_MAX_ENTRIES} 筆</p>
    <div class="panel">
      <table>
        <thead>
          <tr><th>Kind</th><th>TF</th><th>Group</th><th>Symbol</th><th>Name</th><th class="num">Level</th><th>Time</th><th>Seen</th></tr>
        </thead>
        <tbody data-section="history">
          {rows_history()}
        </tbody>
      </table>
    </div>
    </div>

    <h2>Scan stats</h2>
    <p class="meta">OK scans <strong>{c['ok']}</strong> · Errors <strong>{len(errs)}</strong> · Hits <strong>{c['hits']}</strong></p>
    {err_block}

    <footer>
      Source: <a href="https://github.com/1mckw/AR">1mckw/AR</a> ·
      JSON: <a href="./latest.json">latest.json</a> ·
      History: <a href="./history.json">history.json</a>
    </footer>
  </div>

  <button type="button" class="search-fab" id="symbolFab" aria-haspopup="dialog" aria-controls="symbolOverlay" title="搜尋掃描商品">
    <span class="search-fab-icon" aria-hidden="true">⌕</span>
    <span class="search-fab-meta">
      <span class="search-fab-label">Search</span>
      <span class="search-fab-hint">搜尋商品…</span>
    </span>
  </button>
  <div class="search-overlay" id="symbolOverlay" hidden>
    <div class="search-modal" role="dialog" aria-modal="true" aria-label="搜尋掃描商品">
      <div class="search-modal-head">
        <input id="symbolSearch" type="search" autocomplete="off" spellcheck="false" placeholder="代碼、名稱、NDX100 / SP500 / DJI30 / FX / Futures / Crypto…" aria-controls="symbolList" />
        <button type="button" id="symbolSearchClose">關閉</button>
      </div>
      <ul id="symbolList" role="listbox"></ul>
    </div>
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
    catalog = build_symbol_catalog(payload.get("results") or [], charts)
    embed_js = (
        "<script>window.CHART_PACKS = "
        + json.dumps(charts, ensure_ascii=False, separators=(",", ":"))
        + ";window.SYMBOL_CATALOG = "
        + json.dumps(catalog, ensure_ascii=False, separators=(",", ":"))
        + ";</script>\n"
    )
    return page_head + embed_js + CHART_MODAL_SCRIPT + GROUP_FILTER_SCRIPT + "\n</body>\n</html>\n"


def main() -> int:
    os.makedirs(OUT_DIR, exist_ok=True)
    print("Loading universes…", flush=True)
    try:
        ndx100 = fetch_ndx100()
        sp500 = fetch_sp500()
        dji30 = fetch_dji30()
        crypto = fetch_top_crypto()
    except Exception as exc:  # noqa: BLE001
        print(f"Universe load failed: {exc}", flush=True)
        ndx100 = fetch_ndx100()  # has internal fallback
        sp500 = fetch_sp500()
        dji30 = fetch_dji30()
        crypto = list(CRYPTO_FALLBACK[:CRYPTO_TOP_N])

    jobs = []
    for tf in TIMEFRAMES:
        jobs.extend(("ndx100", s, n, "yahoo", tf) for s, n in ndx100)
        jobs.extend(("sp500", s, n, "yahoo", tf) for s, n in sp500)
        jobs.extend(("dji30", s, n, "yahoo", tf) for s, n in dji30)
        jobs.extend(("futures", s, n, "yahoo", tf) for s, n in FUTURES)
        jobs.extend(("fx", s, n, "yahoo", tf) for s, n in FX)
        jobs.extend(("crypto", s, n, "binance", tf) for s, n in crypto)
    tf_label = " + ".join(fmt_tf(tf) for tf in TIMEFRAMES)
    sym_n = len(ndx100) + len(sp500) + len(dji30) + len(FUTURES) + len(FX) + len(crypto)
    print(f"Scanning {len(jobs)} jobs ({sym_n} symbols × {tf_label})…", flush=True)

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
            TF_ORDER.get(x.get("timeframe", ""), 99),
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
            "near_miss_tol_pct": NEAR_MISS_TOL_PCT,
            "fresh_bars": FRESH_BARS,
            "drop_pct": DROP_PCT,
            "min_streak": MIN_STREAK,
            "vol_mult": VOL_MULT,
            "use_structure": USE_STRUCTURE,
            "pivot_high": PIVOT_HIGH,
            "pivot_low": PIVOT_LOW,
            "trend_exceed_bars": TREND_EXCEED_BARS,
            "trend_exceed_min_bars": TREND_EXCEED_MIN_BARS,
            "trend_exceed_max_bars": TREND_EXCEED_MAX_BARS,
        },
        "counts": {
            "ndx100": len(ndx100),
            "sp500": len(sp500),
            "dji30": len(dji30),
            "futures": len(FUTURES),
            "fx": len(FX),
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
    hist = update_history_archive(hits, payload["generated_at"])
    page = render_html(payload)
    with open(html_path, "w", encoding="utf-8") as f:
        f.write(page)
    with open(index_path, "w", encoding="utf-8") as f:
        f.write(page)

    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with open(summary, "a", encoding="utf-8") as f:
            f.write(
                f"### Touch Alerts\n\nHits: **{len(hits)}** · OK: **{payload['counts']['ok']}**"
                f" · History: **{hist['count']}**\n"
            )

    print(
        f"Hits: {len(hits)} · history {hist['count']} · wrote {html_path} and {json_path}",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
