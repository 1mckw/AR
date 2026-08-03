#!/usr/bin/env python3
"""Hourly AR/DR signal scanner: S&P 500 + futures + top 50 crypto."""

from __future__ import annotations

import csv
import io
import json
import os
import time
import urllib.error
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
BARS = 180  # ~7.5 days of 1H
FRESH_BARS = 2  # signal on last N closed bars

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


def fetch_sp500() -> list[tuple[str, str]]:
    url = "https://raw.githubusercontent.com/datasets/s-and-p-500-companies/master/data/constituents.csv"
    try:
        raw = http_get(url, timeout=40).decode()
        rows = list(csv.DictReader(io.StringIO(raw)))
        out = []
        for r in rows:
            sym = (r.get("Symbol") or r.get("symbol") or "").strip().replace(".", "-")
            name = (r.get("Security") or r.get("Name") or sym).strip()
            if sym:
                out.append((sym, name))
        if len(out) >= 400:
            return out
    except Exception as exc:
        print(f"SP500 fetch failed: {exc}")
    # fallback: liquid mega-caps if CSV unavailable
    return [
        (s, s)
        for s in [
            "AAPL", "MSFT", "NVDA", "AMZN", "META", "GOOGL", "BRK-B", "AVGO", "TSLA", "JPM",
            "V", "XOM", "UNH", "MA", "PG", "JNJ", "HD", "COST", "ABBV", "CRM",
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
    out = []
    for t in usdt[:50]:
        sym = t["symbol"]
        out.append((sym, sym.replace("USDT", "")))
    return out


def parse_binance_klines(raw: list) -> list[dict]:
    candles = []
    for k in raw:
        candles.append(
            {
                "time": int(k[0]) // 1000,
                "open": float(k[1]),
                "high": float(k[2]),
                "low": float(k[3]),
                "close": float(k[4]),
                "volume": float(k[5]),
            }
        )
    return candles


def fetch_binance_1h(symbol: str, bars: int = BARS) -> list[dict]:
    url = (
        "https://api.binance.com/api/v3/klines?"
        + urllib.parse.urlencode({"symbol": symbol, "interval": "1h", "limit": min(bars, 1000)})
    )
    return parse_binance_klines(http_get_json(url))


def fetch_yahoo_1h(symbol: str, bars: int = BARS) -> list[dict]:
    # Yahoo 60m max ~730d; 1mo ≈ enough for BARS
    yrange = "1mo" if bars <= 400 else "3mo"
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
    q = (r0.get("indicators") or {}).get("quote") or [{}]
    q0 = q[0] if q else {}
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
    if len(out) > bars:
        out = out[-bars:]
    return out


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


def fresh_signals(candles: list[dict], signals: list[dict]) -> list[dict]:
    if not candles:
        return []
    last = len(candles) - 1
    # ignore incomplete forming bar? Yahoo/Binance last bar may be open — still useful hourly
    lo = max(0, last - (FRESH_BARS - 1))
    return [s for s in signals if lo <= s["index"] <= last]


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
        sigs = fresh_signals(candles, detect_signals(candles))
        return {
            "group": group,
            "symbol": symbol,
            "name": name,
            "source": source,
            "bars": len(candles),
            "signals": sigs,
            "error": None,
        }
    except Exception as exc:  # noqa: BLE001
        return {
            "group": group,
            "symbol": symbol,
            "name": name,
            "source": source,
            "bars": 0,
            "signals": [],
            "error": str(exc),
        }


def fmt_ts(ts: int) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def render_md(payload: dict) -> str:
    lines = [
        f"# AR/DR Signals",
        "",
        f"Updated: **{payload['generated_at']}** · TF **1H** · fresh last **{FRESH_BARS}** bar(s)",
        "",
        f"Universe: SP500 `{payload['counts']['sp500']}` · Futures `{payload['counts']['futures']}` · Crypto `{payload['counts']['crypto']}`",
        "",
    ]
    hits = payload["hits"]
    if not hits:
        lines += ["## No fresh signals", "", "_No AR/DR on the latest bars._", ""]
    else:
        lines += [f"## Fresh signals ({len(hits)})", ""]
        lines += ["| Type | Group | Symbol | Name | Level | Close | Time |", "|------|-------|--------|------|------:|------:|------|"]
        for h in hits:
            lines.append(
                f"| **{h['type']}** | {h['group']} | `{h['symbol']}` | {h['name']} | "
                f"{h['level']:.4g} | {h['close']:.4g} | {fmt_ts(h['time'])} |"
            )
        lines.append("")

    errs = [r for r in payload["results"] if r.get("error")]
    lines += [f"## Scan stats", "", f"- OK symbols: {payload['counts']['ok']}", f"- Errors: {len(errs)}", ""]
    if errs[:15]:
        lines += ["### Sample errors", ""]
        for e in errs[:15]:
            lines.append(f"- `{e['symbol']}`: {e['error']}")
        lines.append("")
    return "\n".join(lines) + "\n"


def main() -> None:
    os.makedirs(OUT_DIR, exist_ok=True)
    print("Loading universes…")
    sp500 = fetch_sp500()
    crypto = fetch_top50_crypto()
    jobs: list[tuple[str, str, str, str]] = []
    jobs += [("sp500", s, n, "yahoo") for s, n in sp500]
    jobs += [("futures", s, n, "yahoo") for s, n in FUTURES]
    jobs += [("crypto", s, n, "binance") for s, n in crypto]
    print(f"Scanning {len(jobs)} symbols…")

    results: list[dict] = []
    # Yahoo is picky — keep concurrency modest
    with ThreadPoolExecutor(max_workers=6) as pool:
        futs = {
            pool.submit(scan_one, group, sym, name, src): (group, sym)
            for group, sym, name, src in jobs
        }
        done = 0
        for fut in as_completed(futs):
            results.append(fut.result())
            done += 1
            if done % 50 == 0:
                print(f"  progress {done}/{len(jobs)}")

    hits = []
    for r in results:
        for s in r["signals"]:
            hits.append(
                {
                    "group": r["group"],
                    "symbol": r["symbol"],
                    "name": r["name"],
                    "type": s["type"],
                    "time": s["time"],
                    "level": s["level"],
                    "close": s["close"],
                    "volume": s["volume"],
                }
            )
    hits.sort(key=lambda x: (0 if x["type"] == "AR" else 1, x["group"], x["symbol"]))

    payload = {
        "generated_at": datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
        "timeframe": "1h",
        "params": {
            "drop_pct": DROP_PCT,
            "min_streak": MIN_STREAK,
            "vol_mult": VOL_MULT,
            "lookback": LOOKBACK,
            "fresh_bars": FRESH_BARS,
            "use_structure": USE_STRUCTURE,
        },
        "counts": {
            "sp500": len(sp500),
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
    with open(md_path, "w", encoding="utf-8") as f:
        f.write(render_md(payload))

    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with open(summary, "a", encoding="utf-8") as f:
            f.write(render_md(payload))

    print(f"Hits: {len(hits)} · wrote {json_path} and {md_path}")


if __name__ == "__main__":
    main()
