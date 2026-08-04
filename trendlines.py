"""Auto trend lines: pivot pairs, body validation, sharp pierce grace.

Sharp rally/drop bars may body-pierce a trend line (resistance←sharpUp,
support←sharpDown). After such a pierce, body may stay on the wrong side
for at most SHARP_PIERCE_GRACE_BARS additional bars; otherwise the line fails.
"""

from __future__ import annotations

from typing import Any

import ardr

PIVOT_HIGH = 4
PIVOT_LOW = 4
MAX_LOOKBACK = 2000
MAX_RESISTANCE = 1
MAX_SUPPORT = 1
MAX_LINES_PER_PIVOT = 1
SHARP_PIERCE_GRACE_BARS = 2


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


def body_crosses(candles: list[dict], i: int, lp: float, resistance: bool) -> bool:
    c = candles[i]
    body_hi = max(c["open"], c["close"])
    body_lo = min(c["open"], c["close"])
    if resistance:
        return body_hi > lp
    return body_lo < lp


def is_sharp_pierce_bar(candles: list[dict], i: int, resistance: bool) -> bool:
    if resistance:
        return ardr.sharp_up(candles, i)
    return ardr.sharp_down(candles, i)


def validate_line_body_segment(
    candles: list[dict],
    p1: dict,
    slope: float,
    start_i: int,
    end_i: int,
    resistance: bool,
) -> bool:
    """True if no disallowed body pierce between start_i and end_i inclusive."""
    if start_i > end_i:
        return True
    in_grace = False
    after_pierce = 0
    for i in range(start_i, end_i + 1):
        lp = line_price(p1, slope, i)
        if not body_crosses(candles, i, lp, resistance):
            in_grace = False
            after_pierce = 0
            continue
        if is_sharp_pierce_bar(candles, i, resistance):
            in_grace = True
            after_pierce = 0
            continue
        if in_grace:
            after_pierce += 1
            if after_pierce > SHARP_PIERCE_GRACE_BARS:
                return False
            continue
        return False
    return True


def find_line_break_index(candles: list[dict], line: dict) -> int | None:
    """First bar where body rules fail, or None if the line is still valid."""
    p1 = line["p1"]
    resistance = line["type"] == "resistance"
    slope = line["slope"]
    in_grace = False
    after_pierce = 0
    for i in range(p1["index"] + 1, len(candles)):
        lp = line_price(p1, slope, i)
        if not body_crosses(candles, i, lp, resistance):
            in_grace = False
            after_pierce = 0
            continue
        if is_sharp_pierce_bar(candles, i, resistance):
            in_grace = True
            after_pierce = 0
            continue
        if in_grace:
            after_pierce += 1
            if after_pierce > SHARP_PIERCE_GRACE_BARS:
                return i
            continue
        return i
    return None


def valid_between_pivots(candles: list[dict], p1: dict, p2: dict, resistance: bool) -> bool:
    if p2["index"] <= p1["index"]:
        return False
    slope = (p2["price"] - p1["price"]) / (p2["index"] - p1["index"])
    return validate_line_body_segment(
        candles, p1, slope, p1["index"] + 1, p2["index"] - 1, resistance
    )


def valid_to_current(candles: list[dict], p1: dict, p2: dict, resistance: bool) -> bool:
    slope = (p2["price"] - p1["price"]) / (p2["index"] - p1["index"])
    return validate_line_body_segment(
        candles, p1, slope, p2["index"] + 1, len(candles) - 1, resistance
    )


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
    return find_line_break_index(candles, line) is not None


def find_trend_touch(candles: list[dict], line: dict) -> dict | None:
    start = max(line["p2"]["index"], line["p1"]["index"]) + 1
    break_i = find_line_break_index(candles, line)
    end = (break_i - 1) if break_i is not None else len(candles) - 1
    for i in range(start, end + 1):
        lp = line_price(line["p1"], line["slope"], i)
        c = candles[i]
        touched = c["high"] >= lp if line["type"] == "resistance" else c["low"] <= lp
        if touched:
            return {"time": c["time"], "price": lp, "index": i, "close": c["close"]}
    return None


def line_end_at_break(candles: list[dict], line: dict) -> tuple[int, float]:
    """Return (end_time, end_price) for drawing; clip at break bar if invalidated."""
    last_i = len(candles) - 1
    break_i = find_line_break_index(candles, line)
    end_i = break_i if break_i is not None else last_i
    end_i = max(end_i, line["p2"]["index"])
    lp = line_price(line["p1"], line["slope"], end_i)
    return int(candles[end_i]["time"]), float(lp)
