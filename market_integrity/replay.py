"""Point-in-time replay with fault injection.

The replay walks an ``as_of`` clock hour by hour through the sample window. At
each step it calls the real Binance fetcher (``binance_public``) with a fake
transport that serves historical payloads, optionally corrupted by a fault
schedule (outage, geo-block, empty 200, literal-zero open interest, ...). The
resulting Records go through ``build_features`` and ``gate``.

Two pipelines are compared on identical inputs:
  * integrity pipeline: missing stays missing, gate refuses to call it normal
  * naive pipeline: ``fillna(0)`` style, which is what we must never do
"""
from __future__ import annotations

import json
import urllib.parse
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Callable, Optional

from . import binance_public as bp
from .schema import Availability, MarketType, Record, iso

HOUR = timedelta(hours=1)

# Required feeds for a trading decision on one symbol.
REQUIRED = (
    (MarketType.USDM_PERP, "close", timedelta(hours=2)),
    (MarketType.USDM_PERP, "open_interest", timedelta(hours=2)),
    (MarketType.USDM_PERP, "funding_rate", timedelta(hours=8, minutes=30)),
)


@dataclass
class Feature:
    name: str
    value: Optional[Decimal]
    status: str           # OK / STALE / an Availability value
    as_of_event: Optional[str]
    age_minutes: Optional[float]


def build_features(records, symbol: str, as_of: datetime) -> dict:
    feats = {}
    for market, metric, max_age in REQUIRED:
        rs = [r for r in records if r.symbol == symbol and r.market_type == market and r.metric == metric
              and r.event_time is not None and r.event_time <= as_of and _knowable(r, as_of)]
        rs.sort(key=lambda r: r.event_time)
        name = f"{market.value}.{metric}"
        if not rs:
            feats[name] = Feature(name, None, Availability.NOT_YET_PUBLISHED.value, None, None)
            continue
        latest = rs[-1]
        if latest.usable and latest.available_at <= as_of:
            age = (as_of - latest.event_time).total_seconds() / 60
            status = "OK" if age <= max_age.total_seconds() / 60 else "STALE"
            feats[name] = Feature(name, latest.value if status == "OK" else None, status,
                                  iso(latest.event_time), age)
        else:
            # Latest expected observation is not usable: report its status, never a number.
            last_ok = next((r for r in reversed(rs) if r.usable and r.available_at <= as_of), None)
            age = None if last_ok is None else (as_of - last_ok.event_time).total_seconds() / 60
            status = (Availability.NOT_YET_PUBLISHED.value if latest.usable
                      else latest.availability.value)
            feats[name] = Feature(name, None, status, iso(latest.event_time), age)
    return feats


def _knowable(r: Record, as_of: datetime) -> bool:
    """Drop observations that were not out yet at as_of (inside the publication lag).

    They are neither a fault nor a value: the previous observation stays current,
    subject to the staleness limit. Failures still count, so an outage still halts.
    """
    if r.availability == Availability.NOT_YET_PUBLISHED:
        return False
    return not (r.usable and r.available_at is not None and r.available_at > as_of)


def gate(feats: dict) -> str:
    """TRADEABLE only when every required feed is OK and fresh; otherwise HALT."""
    bad = [f for f in feats.values() if f.status != "OK" or f.value is None]
    return "TRADEABLE" if not bad else "HALT"


def naive_features(records, symbol: str, as_of: datetime) -> dict:
    """What a careless pipeline does: missing -> 0, any status ignored."""
    out = {}
    for market, metric, _ in REQUIRED:
        rs = [r for r in records if r.symbol == symbol and r.market_type == market and r.metric == metric
              and r.event_time is not None and r.event_time <= as_of]
        rs.sort(key=lambda r: r.event_time)
        out[f"{market.value}.{metric}"] = (rs[-1].value if rs and rs[-1].value is not None else Decimal(0))
    return out


def naive_gate(nf: dict) -> str:
    # A naive system has no notion of availability, so it always "trades".
    return "TRADEABLE"


# ---------------------------------------------------------------------------
# Historical payload server + fault injection
# ---------------------------------------------------------------------------

@dataclass
class Fault:
    kind: str                    # outage_5xx | geo_block | empty_200 | zero_oi | maintenance_code
    start: datetime
    end: datetime
    paths: tuple = ("/api/v3/klines", "/fapi/v1/klines", "/futures/data/openInterestHist", "/fapi/v1/fundingRate")
    symbols: tuple = ("BTCUSDT", "ETHUSDT")

    def active(self, as_of, path, symbol):
        return self.start <= as_of < self.end and path in self.paths and symbol in self.symbols


@dataclass
class HistoricalServer:
    """Serves Binance-shaped JSON built from captured history, honouring as_of."""
    closes: dict                 # (symbol, market) -> {open_time: close_str}
    oi: dict                     # symbol -> {ts: oi_str}
    funding: dict                # symbol -> {slot: rate_str}
    faults: list = field(default_factory=list)
    as_of: datetime = None
    calls: list = field(default_factory=list)

    def __call__(self, url: str):
        u = urllib.parse.urlparse(url)
        q = dict(urllib.parse.parse_qsl(u.query))
        sym = q.get("symbol")
        self.calls.append(u.path)
        for f in self.faults:
            if f.active(self.as_of, u.path, sym):
                if f.kind == "outage_5xx":
                    return 503, "Service Unavailable"
                if f.kind == "geo_block":
                    raise OSError("Tunnel connection failed: 403 Forbidden")
                if f.kind == "empty_200":
                    return 200, "[]"
                if f.kind == "maintenance_code":
                    return 200, json.dumps({"code": -1001, "msg": "Internal error; unable to process your request."})
        start = bp._from_ms(q["startTime"])
        end = bp._from_ms(q["endTime"])
        if u.path in ("/api/v3/klines", "/fapi/v1/klines"):
            market = MarketType.SPOT if u.path.startswith("/api") else MarketType.USDM_PERP
            rows = []
            for ot, c in sorted(self.closes.get((sym, market), {}).items()):
                if start <= ot <= end and ot <= self.as_of:
                    ct = ot + HOUR
                    rows.append([bp._ms(ot), c, c, c, c, "1", bp._ms(ct) - 1, "0", 0, "0", "0", "0"])
            return 200, json.dumps(rows)
        if u.path == "/futures/data/openInterestHist":
            zero = any(f.kind == "zero_oi" and f.start <= self.as_of < f.end and sym in f.symbols
                       for f in self.faults)
            rows = []
            for ts, v in sorted(self.oi.get(sym, {}).items()):
                if start <= ts <= end and ts <= self.as_of:
                    vv = "0.000" if zero and ts > self.as_of - HOUR else v
                    rows.append({"symbol": sym, "sumOpenInterest": vv, "sumOpenInterestValue": "0",
                                 "timestamp": bp._ms(ts)})
            return 200, json.dumps(rows)
        if u.path == "/fapi/v1/fundingRate":
            rows = []
            for slot, v in sorted(self.funding.get(sym, {}).items()):
                if start <= slot <= end and slot <= self.as_of:
                    rows.append({"symbol": sym, "fundingTime": bp._ms(slot) + 3, "fundingRate": v,
                                 "markPrice": "0"})
            return 200, json.dumps(rows)
        return 404, "{}"


def run_replay(server: HistoricalServer, start: datetime, end: datetime, steps,
               symbols=("BTCUSDT", "ETHUSDT")) -> list:
    """Returns one row per (as_of, symbol) with integrity vs naive outcomes."""
    out = []
    for as_of in steps:
        server.as_of = as_of
        recs = bp.collect(start, end, as_of, transport=server, symbols=symbols)
        for s in symbols:
            f = build_features(recs, s, as_of)
            nf = naive_features(recs, s, as_of)
            out.append({
                "as_of": iso(as_of), "symbol": s,
                "gate": gate(f),
                "naive_gate": naive_gate(nf),
                "features": {k: {"value": None if v.value is None else str(v.value), "status": v.status,
                                 "event": v.as_of_event,
                                 "age_min": None if v.age_minutes is None else round(v.age_minutes, 1)}
                             for k, v in f.items()},
                "naive": {k: str(v) for k, v in nf.items()},
                "records": recs,
            })
    return out
