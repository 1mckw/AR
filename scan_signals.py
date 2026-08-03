#!/usr/bin/env python3
"""Hourly touch scanner: NDX100 + futures + top 50 crypto.

Reports only:
  - AR/DR base-level touch after >12 bars from signal (not new AR/DR within 12 bars)
  - Trend-line wick touches
"""

from __future__ import annotations

import csv
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
        print(f"NDX100 fetch failed: {exc}")
    return [
        (s, s)
        for s in [
            "AAPL", "MSFT", "NVDA", "AMZN", "META", "GOOGL", "GOOG", "AVGO", "TSLA", "COST",
            "NFLX", "AMD", "PEP", "ADBE", "CSCO", "TMUS", "INTC", "INTU", "AMAT", "QCOM",
        ]
    ]


def fetch_top50_crypto() -> list[tuple[str, str]]:
    url = "https://api.binance.com/api/v3/ticker/24hr"
    data = http_get_json(url, timeout=40)
    skip_bases = {
        "USDC", "FDUSD", "TUSD", "BUSD", "DAI", "EUR", "AEUR", "BFUSD", "USD1", "XUSD",
    }
    usdt = []
    for t in data:
        sym = str(t.get("symbol", ""))
        if not sym.endswith("USDT"):
            continue
        if any(x in sym for x in ("UPUSDT", "DOWNUSDT", "BULL", "BEAR")):
            continue
        base = sym[:-4]
        if base in skip_bases:
            continue
        usdt.append(t)
    usdt.sort(key=lambda t: float(t.get("quoteVolume") or 0), reverse=True)
    return [(t["symbol"], t["symbol"].replace("USDT", "")) for t in usdt[:50]]


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
    url = (
        "https://api.binance.com/api/v3/klines?"
        + urllib.parse.urlencode({"symbol": symbol, "interval": "1h", "limit": min(bars, 1000)})
    )
    return parse_binance_klines(http_get_json(url))


def fetch_yahoo_1h(symbol: str, bars: int = BARS) -> list[dict]:
    yrange = "3mo" if bars <= 700 else "6mo"
    url = (
        "https://query1.finance.yahoo.com/v8/finance/chart/"
        + urllib.parse.quote(symbol, safe="=-.^")
        + f"?interval=60m&range={yrange}&includePrePost=false"
    )
    payload = http_get_json(url)
    result = (payload.get("chart") or {}).get("result") or []
    if not result:
        return []
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
    return out[-bars:] if len(out) > bars else out


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
        drop_pct = (candles[i - LOOKBACK]["close"] - candles[i]["close"]) / candles[i - LOOKBACK]["close"] * 100
        rise_pct = (candles[i]["close"] - candles[i - LOOKBACK]["close"]) / candles[i - LOOKBACK]["close"] * 100
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


def render_md(payload: dict) -> str:
    lines = [
        "# Touch Alerts",
        "",
        f"Updated: **{payload['generated_at']}** · TF **1H** · fresh last **{FRESH_BARS}** bar(s)",
        "",
        (
            f"Universe: NDX100 `{payload['counts']['ndx100']}` · "
            f"Futures `{payload['counts']['futures']}` · Crypto `{payload['counts']['crypto']}`"
        ),
        "",
        "Rules:",
        "- **不報告** 近 12 根內新出現的 AR/DR",
        "- **報告** 信號後超過 12 根才觸碰 AR/DR 原價位",
        "- **報告** 趨勢線影線觸碰",
        "",
    ]
    hits = payload["hits"]
    if not hits:
        lines += ["## No fresh touches", "", "_No late AR/DR or trend-line touches on latest bars._", ""]
    else:
        ar_dr = [h for h in hits if h["kind"] == "ar_dr_touch"]
        trend = [h for h in hits if h["kind"] == "trend_touch"]
        if ar_dr:
            lines += [f"## AR/DR 觸碰（>{TOUCH_WINDOW_BARS} 根後） ({len(ar_dr)})", ""]
            lines += [
                "| Type | Group | Symbol | Name | Level | Bars after | Time |",
                "|------|-------|--------|------|------:|----------:|------|",
            ]
            for h in ar_dr:
                lines.append(
                    f"| **{h['type']}** | {h['group']} | `{h['symbol']}` | {h['name']} | "
                    f"{h['level']:.4g} | {h['bars_after_signal']} | {fmt_ts(h['time'])} |"
                )
            lines.append("")
        if trend:
            lines += [f"## 趨勢線觸碰 ({len(trend)})", ""]
            lines += [
                "| Side | Group | Symbol | Name | Level | Time |",
                "|------|-------|--------|------|------:|------|",
            ]
            for h in trend:
                lines.append(
                    f"| **{h['type']}** | {h['group']} | `{h['symbol']}` | {h['name']} | "
                    f"{h['level']:.4g} | {fmt_ts(h['time'])} |"
                )
            lines.append("")

    errs = [r for r in payload["results"] if r.get("error")]
    lines += [
        "## Scan stats",
        "",
        f"- OK symbols: {payload['counts']['ok']}",
        f"- Errors: {len(errs)}",
        f"- Hits: {len(hits)}",
        "",
    ]
    if errs[:15]:
        lines += ["### Sample errors", ""]
        for e in errs[:15]:
            lines.append(f"- `{e['symbol']}`: {e['error']}")
        lines.append("")
    return "\n".join(lines) + "\n"


def main() -> None:
    os.makedirs(OUT_DIR, exist_ok=True)
    print("Loading universes…")
    ndx100 = fetch_ndx100()
    crypto = fetch_top50_crypto()
    jobs = (
        [("ndx100", s, n, "yahoo") for s, n in ndx100]
        + [("futures", s, n, "yahoo") for s, n in FUTURES]
        + [("crypto", s, n, "binance") for s, n in crypto]
    )
    print(f"Scanning {len(jobs)} symbols for touches…")

    results: list[dict] = []
    with ThreadPoolExecutor(max_workers=6) as pool:
        futs = [pool.submit(scan_one, g, s, n, src) for g, s, n, src in jobs]
        done = 0
        for fut in as_completed(futs):
            results.append(fut.result())
            done += 1
            if done % 50 == 0:
                print(f"  progress {done}/{len(jobs)}")

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
    md_path = os.path.join(OUT_DIR, "latest.md")
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    md = render_md(payload)
    with open(md_path, "w", encoding="utf-8") as f:
        f.write(md)
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with open(summary, "a", encoding="utf-8") as f:
            f.write(md)
    print(f"Hits: {len(hits)} · wrote {json_path} and {md_path}")


if __name__ == "__main__":
    main()
