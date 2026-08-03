#!/usr/bin/env python3
"""Static server + Yahoo Finance proxy + TradingView symbol search."""

from __future__ import annotations

import json
import os
import urllib.parse
import urllib.request
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer

ROOT = os.path.dirname(os.path.abspath(__file__))

YAHOO_INTERVAL = {"1h": "60m", "4h": "60m", "1d": "1d"}
PERIOD_SEC = {"1h": 3600, "4h": 14400, "1d": 86400}


def aggregate_bars(rows: list[list], period_sec: int) -> list[list]:
    buckets: dict[int, list] = {}
    for row in rows:
        ts_ms, o, h, l, c, v = row
        key = (ts_ms // 1000 // period_sec) * period_sec
        if key not in buckets:
            buckets[key] = [key * 1000, o, h, l, c, v]
        else:
            b = buckets[key]
            b[2] = max(b[2], h)
            b[3] = min(b[3], l)
            b[4] = c
            b[5] += v
    return [buckets[k] for k in sorted(buckets)]


def yahoo_range(interval: str, bars: int) -> str:
    if interval == "1d":
        if bars <= 500:
            return "2y"
        if bars <= 2000:
            return "5y"
        if bars <= 5000:
            return "10y"
        return "max"
    if interval == "4h":
        hours = bars * 4
        if hours <= 24 * 90:
            return "3mo"
        if hours <= 24 * 180:
            return "6mo"
        if hours <= 24 * 365:
            return "1y"
        return "730d"
    if bars <= 1000:
        return "3mo"
    if bars <= 2000:
        return "6mo"
    if bars <= 3000:
        return "1y"
    return "730d"


class Handler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=ROOT, **kwargs)

    def do_GET(self):
        if self.path.startswith("/api/yahoo?"):
            self._yahoo_proxy()
            return
        if self.path.startswith("/api/tv-search?"):
            self._tv_search()
            return
        return super().do_GET()

    def _json_ok(self, payload):
        body = json.dumps(payload, ensure_ascii=False).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _tv_search(self):
        qs = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
        text = (qs.get("q") or qs.get("text") or [""])[0].strip()
        if not text:
            self.send_error(400, "missing q")
            return
        params = urllib.parse.urlencode(
            {
                "text": text,
                "hl": "1",
                "exchange": "",
                "lang": "zh",
                "search_type": "undefined",
                "domain": "production",
                "sort_by_country": "US",
            }
        )
        url = "https://symbol-search.tradingview.com/symbol_search/v3/?" + params
        try:
            req = urllib.request.Request(
                url,
                headers={
                    "User-Agent": "Mozilla/5.0",
                    "Origin": "https://www.tradingview.com",
                    "Referer": "https://www.tradingview.com/",
                },
            )
            with urllib.request.urlopen(req, timeout=20) as resp:
                payload = json.loads(resp.read().decode())
        except Exception as exc:
            self.send_error(502, str(exc))
            return

        def strip_em(s: str) -> str:
            return (
                str(s or "")
                .replace("<em>", "")
                .replace("</em>", "")
                .replace("<EM>", "")
                .replace("</EM>", "")
            )

        out = []
        for item in payload.get("symbols") or []:
            symbol = strip_em(item.get("symbol"))
            exchange = strip_em(item.get("exchange") or item.get("source_id") or "")
            if not symbol:
                continue
            logo = item.get("logo") or {}
            logoid = (
                item.get("logoid")
                or logo.get("logoid")
                or item.get("base-currency-logoid")
                or ""
            )
            logo_url = (
                f"https://s3-symbol-logo.tradingview.com/{logoid}.svg" if logoid else ""
            )
            out.append(
                {
                    "symbol": symbol,
                    "description": strip_em(item.get("description")),
                    "type": item.get("type") or "",
                    "exchange": exchange,
                    "typespecs": item.get("typespecs") or [],
                    "currency": item.get("currency_code") or "",
                    "country": item.get("country") or "",
                    "logoid": logoid,
                    "logoUrl": logo_url,
                }
            )
        self._json_ok({"symbols": out})

    def _yahoo_proxy(self):
        qs = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
        symbol = (qs.get("symbol") or [""])[0]
        bars = int((qs.get("bars") or ["2000"])[0])
        interval = (qs.get("interval") or ["1h"])[0]
        if interval not in YAHOO_INTERVAL:
            self.send_error(400, "interval must be 1h, 4h, or 1d")
            return
        if not symbol:
            self.send_error(400, "missing symbol")
            return

        fetch_bars = bars * 4 if interval == "4h" else bars
        yahoo_iv = YAHOO_INTERVAL[interval]
        yrange = yahoo_range(interval, fetch_bars)

        url = (
            "https://query1.finance.yahoo.com/v8/finance/chart/"
            + urllib.parse.quote(symbol, safe=".")
            + f"?interval={yahoo_iv}&range={yrange}&includePrePost=false"
        )
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=30) as resp:
                payload = json.loads(resp.read().decode())
        except Exception as exc:
            self.send_error(502, str(exc))
            return

        result = payload.get("chart", {}).get("result")
        if not result:
            self.send_error(404, "no data")
            return

        r0 = result[0]
        ts = r0.get("timestamp") or []
        q = r0.get("indicators", {}).get("quote", [{}])[0]
        out = []
        for i, t in enumerate(ts):
            o, h, l, c, v = (
                q.get("open", [None])[i],
                q.get("high", [None])[i],
                q.get("low", [None])[i],
                q.get("close", [None])[i],
                q.get("volume", [None])[i],
            )
            if None in (o, h, l, c) or c is None:
                continue
            out.append([int(t) * 1000, o, h, l, c, v or 0])

        out.sort(key=lambda x: x[0])
        if interval == "4h":
            out = aggregate_bars(out, PERIOD_SEC["4h"])

        if len(out) > bars:
            out = out[-bars:]

        self._json_ok(out)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8765"))
    httpd = ThreadingHTTPServer(("127.0.0.1", port), Handler)
    print(f"Serving {ROOT} at http://127.0.0.1:{port}/")
    httpd.serve_forever()
