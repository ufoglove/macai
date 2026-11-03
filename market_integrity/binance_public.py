"""Read-only Binance public REST fetcher that never turns a failure into a number.

Only unauthenticated market-data endpoints are used. No API key, no order
endpoints. Every *expected* observation in the requested window produces one
Record, so gaps and outages are visible instead of silently absent.
"""
from __future__ import annotations

import json
import ssl
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Callable, Optional, Tuple

from .schema import Availability, MarketType, Record, SourceTier

SPOT_BASE = "https://api.binance.com"
FUT_BASE = "https://fapi.binance.com"

INTERVAL = {"1h": timedelta(hours=1), "4h": timedelta(hours=4), "8h": timedelta(hours=8)}

# A transport returns (http_status, body_text) or raises an exception.
Transport = Callable[[str], Tuple[int, str]]


def urllib_transport(url: str, timeout: float = 15.0) -> Tuple[int, str]:
    req = urllib.request.Request(url, headers={"User-Agent": "market-integrity-sampler/0.1"})
    ctx = ssl.create_default_context()
    try:
        with urllib.request.urlopen(req, timeout=timeout, context=ctx) as resp:
            return resp.status, resp.read().decode("utf-8", "replace")
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode("utf-8", "replace")


class FetchFailure(Exception):
    def __init__(self, availability: Availability, detail: str):
        super().__init__(detail)
        self.availability = availability
        self.detail = detail


def classify_http(status: int, body: str) -> Optional[Availability]:
    """Map an HTTP result to a failure status, or None if it is a usable 2xx."""
    if 200 <= status < 300:
        try:
            payload = json.loads(body)
        except ValueError:
            return Availability.PARSE_ERROR
        # Binance error payloads look like {"code": -1121, "msg": "..."}
        if isinstance(payload, dict) and "code" in payload and payload.get("code") not in (0, 200):
            return Availability.EXCHANGE_ERROR
        return None
    if status in (418, 429):
        return Availability.RATE_LIMITED
    if status in (403, 451):  # WAF / restricted location
        return Availability.SOURCE_UNREACHABLE
    if status >= 500:
        return Availability.EXCHANGE_ERROR
    return Availability.EXCHANGE_ERROR


def get_json(url: str, transport: Transport):
    try:
        status, body = transport(url)
    except (urllib.error.URLError, OSError, TimeoutError) as e:  # DNS, TCP, proxy CONNECT 403
        raise FetchFailure(Availability.SOURCE_UNREACHABLE, f"{type(e).__name__}: {e}")
    fail = classify_http(status, body)
    if fail is not None:
        raise FetchFailure(fail, f"HTTP {status}: {body[:200]}")
    return json.loads(body)


def _ms(d: datetime) -> int:
    return int(d.timestamp() * 1000)


def _from_ms(v) -> datetime:
    return datetime.fromtimestamp(int(v) / 1000, tz=timezone.utc)


def _dec(s) -> Optional[Decimal]:
    try:
        d = Decimal(str(s))
    except (InvalidOperation, TypeError):
        return None
    return d if d.is_finite() else None


def _grid(start: datetime, end: datetime, step: timedelta):
    t = start
    while t < end:
        yield t
        t += step


def _url(base: str, path: str, **params) -> str:
    return f"{base}{path}?{urllib.parse.urlencode(params)}"


def fetch_klines(symbol: str, market: MarketType, interval: str, start: datetime, end: datetime,
                 now: datetime, transport: Transport = urllib_transport) -> list:
    """One close-price Record per expected bar in [start, end)."""
    step = INTERVAL[interval]
    if market == MarketType.SPOT:
        url = _url(SPOT_BASE, "/api/v3/klines", symbol=symbol, interval=interval,
                   startTime=_ms(start), endTime=_ms(end) - 1, limit=1000)
    else:
        url = _url(FUT_BASE, "/fapi/v1/klines", symbol=symbol, interval=interval,
                   startTime=_ms(start), endTime=_ms(end) - 1, limit=1500)
    base = dict(symbol=symbol, metric="close", market_type=market, venue="binance",
                source=url, source_tier=SourceTier.PRIMARY, fetched_at=now, unit="quote")
    try:
        rows = get_json(url, transport)
        failure = None
    except FetchFailure as f:
        rows, failure = [], f
    by_open = {}
    for row in rows or []:
        by_open[_from_ms(row[0])] = row
    out = []
    for open_t in _grid(start, end, step):
        close_t = open_t + step
        if open_t > now:
            out.append(Record(**base, availability=Availability.NOT_YET_PUBLISHED,
                              event_time=close_t, available_at=None,
                              note="bar has not opened at fetch time"))
            continue
        if failure is not None:
            out.append(Record(**base, availability=failure.availability, event_time=close_t,
                              available_at=None, note=failure.detail))
            continue
        row = by_open.get(open_t)
        if row is None:
            status = Availability.EMPTY_RESPONSE if not rows else Availability.MISSING_IN_SERIES
            out.append(Record(**base, availability=status, event_time=close_t, available_at=None,
                              note="bar absent from exchange response"))
            continue
        val = _dec(row[4])
        if close_t > now:
            out.append(Record(**base, availability=Availability.INCOMPLETE_PERIOD, event_time=close_t,
                              available_at=None, value=val, raw=str(row[4]),
                              note="bar still open; close is provisional"))
        elif val is None or val <= 0:
            out.append(Record(**base, availability=Availability.SUSPECT_VALUE, event_time=close_t,
                              available_at=close_t, value=val, raw=str(row[4]),
                              flags=["non_positive_price"]))
        else:
            out.append(Record(**base, availability=Availability.OK, event_time=close_t,
                              available_at=close_t, value=val, raw=str(row[4])))
    return out


def fetch_open_interest_hist(symbol: str, start: datetime, end: datetime, now: datetime,
                             period: str = "1h", transport: Transport = urllib_transport) -> list:
    """USD-M open interest snapshots (contracts). Binance keeps ~30 days of history."""
    step = INTERVAL[period]
    url = _url(FUT_BASE, "/futures/data/openInterestHist", symbol=symbol, period=period,
               startTime=_ms(start), endTime=_ms(end), limit=500)
    base = dict(symbol=symbol, metric="open_interest", market_type=MarketType.USDM_PERP,
                venue="binance", source=url, source_tier=SourceTier.PRIMARY, fetched_at=now,
                unit="contracts(base asset)")
    try:
        rows, failure = get_json(url, transport), None
    except FetchFailure as f:
        rows, failure = [], f
    by_ts = {_from_ms(r["timestamp"]): r for r in rows or []}
    out = []
    for t in _grid(start, end + step, step):
        if t > end:
            break
        if t > now:
            out.append(Record(**base, availability=Availability.NOT_YET_PUBLISHED, event_time=t,
                              available_at=None, note="snapshot time is after fetch time"))
            continue
        if failure is not None:
            out.append(Record(**base, availability=failure.availability, event_time=t,
                              available_at=None, note=failure.detail))
            continue
        r = by_ts.get(t)
        if r is None:
            status = Availability.EMPTY_RESPONSE if not rows else Availability.MISSING_IN_SERIES
            out.append(Record(**base, availability=status, event_time=t, available_at=None,
                              note="snapshot absent from exchange response"))
            continue
        val = _dec(r.get("sumOpenInterest"))
        if val is None or val <= 0:
            # Open interest of a live BTC/ETH perpetual cannot be zero; treat as a feed fault.
            out.append(Record(**base, availability=Availability.SUSPECT_VALUE, event_time=t,
                              available_at=t, value=val, raw=str(r.get("sumOpenInterest")),
                              flags=["zero_or_missing_open_interest"]))
        else:
            out.append(Record(**base, availability=Availability.OK, event_time=t, available_at=t,
                              value=val, raw=str(r["sumOpenInterest"])))
    return out


def expected_funding_times(start: datetime, end: datetime, hours=(0, 8, 16)) -> list:
    day = start.replace(hour=0, minute=0, second=0, microsecond=0)
    out = []
    while day <= end:
        for h in hours:
            t = day + timedelta(hours=h)
            if start <= t <= end:
                out.append(t)
        day += timedelta(days=1)
    return out


def fetch_funding(symbol: str, start: datetime, end: datetime, now: datetime,
                  transport: Transport = urllib_transport, hours=(0, 8, 16)) -> list:
    """Settled funding rates. A settlement that has not happened yet is NOT_YET_PUBLISHED, never 0."""
    url = _url(FUT_BASE, "/fapi/v1/fundingRate", symbol=symbol, startTime=_ms(start),
               endTime=_ms(end), limit=1000)
    base = dict(symbol=symbol, metric="funding_rate", market_type=MarketType.USDM_PERP,
                venue="binance", source=url, source_tier=SourceTier.PRIMARY, fetched_at=now,
                unit="rate per interval")
    try:
        rows, failure = get_json(url, transport), None
    except FetchFailure as f:
        rows, failure = [], f
    # Binance stamps fundingTime a few ms after the slot; snap within 60s.
    by_slot = {}
    for r in rows or []:
        ft = _from_ms(r["fundingTime"])
        slot = ft.replace(second=0, microsecond=0)
        if ft - slot <= timedelta(seconds=60):
            by_slot[slot] = r
    out = []
    for t in expected_funding_times(start, end, hours):
        if t > now:
            out.append(Record(**base, availability=Availability.NOT_YET_PUBLISHED, event_time=t,
                              available_at=None, note="funding not settled at fetch time"))
            continue
        if failure is not None:
            out.append(Record(**base, availability=failure.availability, event_time=t,
                              available_at=None, note=failure.detail))
            continue
        r = by_slot.get(t)
        if r is None:
            status = Availability.EMPTY_RESPONSE if not rows else Availability.MISSING_IN_SERIES
            out.append(Record(**base, availability=status, event_time=t, available_at=None,
                              note="settlement absent from exchange response"))
            continue
        raw = r.get("fundingRate")
        val = _dec(raw)
        if val is None or raw in ("", None):
            out.append(Record(**base, availability=Availability.PARSE_ERROR, event_time=t,
                              available_at=None, raw=str(raw)))
        elif val == 0:
            out.append(Record(**base, availability=Availability.SUSPECT_VALUE, event_time=t,
                              available_at=_from_ms(r["fundingTime"]), value=val, raw=str(raw),
                              flags=["literal_zero_funding"],
                              note="exact 0 is possible but rare; needs cross-check before use"))
        else:
            out.append(Record(**base, availability=Availability.OK, event_time=t,
                              available_at=_from_ms(r["fundingTime"]), value=val, raw=str(raw)))
    return out


def collect(start: datetime, end: datetime, now: datetime, transport: Transport = urllib_transport,
            symbols=("BTCUSDT", "ETHUSDT")) -> list:
    out = []
    for s in symbols:
        out += fetch_klines(s, MarketType.SPOT, "1h", start, end, now, transport)
        out += fetch_klines(s, MarketType.USDM_PERP, "1h", start, end, now, transport)
        out += fetch_open_interest_hist(s, start, end, now, transport=transport)
        out += fetch_funding(s, start, end, now, transport)
    return out
