"""AR/DR signal detection, ray lifecycle, and chart segment helpers.

AR (Auto Rally)
  Sharp drop, consecutive bear bars, bullish reversal on high volume.
  Ray level = signal bar HIGH.

DR
  Sharp rise, consecutive bull bars, bearish reversal on high volume.
  Ray level = signal bar LOW.

Ray rules (TOUCH_WINDOW_BARS = 12)
  1. Untouched — horizontal ray at base level extends right (active).
  2. Base touched within 12 bars — switch to extended ray (AR→signal low, DR→signal high).
     Extended ray stays active until the opposite level is hit.
  3. Base touched after 12 bars (late touch) — ray stops at touch bar, no extension.
     This is what the hourly scanner reports.
"""

from __future__ import annotations

from typing import Any

LOOKBACK = 10
VOL_LEN = 20
DROP_PCT = 3.0
MIN_STREAK = 3
VOL_MULT = 1.2
USE_STRUCTURE = True
TOUCH_WINDOW_BARS = 12
FRESH_BARS = 2


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
    """Find AR/DR reversal bars."""
    signals: list[dict] = []
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


def resolve_horizontal_ray(candles: list[dict], item: dict) -> dict[str, Any]:
    """Resolve ray state after signal bar.

    Returns dict with:
      baseLevel, level (draw price), extended, active,
      startTime, extendTime, endTime, late_touch_index
    """
    is_ar = item["type"] == "AR"
    base_level = item["level"]
    signal_time = item["time"]
    sig_idx = item["index"]

    for j in range(sig_idx + 1, len(candles)):
        bar = candles[j]
        within = j - sig_idx <= TOUCH_WINDOW_BARS
        hit_base = bar["high"] >= base_level if is_ar else bar["low"] <= base_level

        if hit_base and within:
            sig_bar = candles[sig_idx]
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
                        "startTime": signal_time,
                        "extendTime": bar["time"],
                        "endTime": b2["time"],
                    }
            return {
                "baseLevel": base_level,
                "level": ext_level,
                "extended": True,
                "active": True,
                "late_touch_index": None,
                "startTime": signal_time,
                "extendTime": bar["time"],
                "endTime": candles[-1]["time"],
            }

        if hit_base and not within:
            return {
                "baseLevel": base_level,
                "level": base_level,
                "extended": False,
                "active": False,
                "late_touch_index": j,
                "startTime": signal_time,
                "extendTime": None,
                "endTime": bar["time"],
            }

    return {
        "baseLevel": base_level,
        "level": base_level,
        "extended": False,
        "active": True,
        "late_touch_index": None,
        "startTime": signal_time,
        "extendTime": None,
        "endTime": candles[-1]["time"],
    }


def fresh_range(n: int) -> tuple[int, int]:
    last = n - 1
    lo = max(0, last - (FRESH_BARS - 1))
    return lo, last


def collect_late_ar_dr_touches(candles: list[dict], signals: list[dict]) -> list[dict]:
    """Report base-level touch after >12 bars when touch bar is fresh."""
    if not candles:
        return []
    lo, last = fresh_range(len(candles))
    hits: list[dict] = []
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


def ray_chart_segments(ray: dict[str, Any], last_time: int) -> list[dict[str, Any]]:
    """Build drawable horizontal segments for lightweight-charts.

    - Untouched active: base level → last_time
    - Late touch (inactive, not extended): base level → endTime only
    - Within-12 extended: base → extendTime (faded), then ext level → end
    """
    segs: list[dict[str, Any]] = []
    base = float(ray["baseLevel"])
    t0 = int(ray["startTime"])

    if ray.get("extended") and ray.get("extendTime") is not None:
        ext_t = int(ray["extendTime"])
        if ext_t > t0:
            segs.append({"t0": t0, "t1": ext_t, "price": base, "active": False})
        ext_price = float(ray["level"])
        t1 = int(last_time if ray["active"] else ray["endTime"])
        if t1 > ext_t:
            segs.append(
                {
                    "t0": ext_t,
                    "t1": t1,
                    "price": ext_price,
                    "active": bool(ray["active"]),
                }
            )
        return segs

    # Base-level ray only
    if ray["active"]:
        t1 = int(last_time)
    else:
        t1 = int(ray["endTime"])  # late touch: stop at touch bar
    if t1 > t0:
        segs.append(
            {
                "t0": t0,
                "t1": t1,
                "price": base,
                "active": bool(ray["active"]),
            }
        )
    return segs


def signal_to_chart_ray(sig: dict, ray: dict[str, Any], last_time: int) -> dict[str, Any]:
    """Compact ray payload for HTML chart packs."""
    return {
        "type": sig["type"],
        "time": int(sig["time"]),
        "active": bool(ray["active"]),
        "extended": bool(ray["extended"]),
        "segments": ray_chart_segments(ray, last_time),
    }
