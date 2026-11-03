"""Record schema for market-data provenance and availability.

Core rule: a value is either a real number that a source actually published,
or it is ``None`` with an explicit availability status explaining why it is
missing. Nothing in this package ever substitutes 0 (or the last value) for a
missing observation.
"""
from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum
from typing import Optional


class Availability(str, Enum):
    OK = "OK"                                  # published, parsed, timestamped
    INCOMPLETE_PERIOD = "INCOMPLETE_PERIOD"    # bar/interval still open at fetch time
    NOT_YET_PUBLISHED = "NOT_YET_PUBLISHED"    # event time is after as-of / fetch time
    SOURCE_UNREACHABLE = "SOURCE_UNREACHABLE"  # DNS/TCP/proxy/geo-block (403/451)
    EXCHANGE_ERROR = "EXCHANGE_ERROR"          # 5xx, maintenance, exchange error code
    RATE_LIMITED = "RATE_LIMITED"              # 418/429
    EMPTY_RESPONSE = "EMPTY_RESPONSE"          # 200 OK but no rows for a past period
    MISSING_IN_SERIES = "MISSING_IN_SERIES"    # gap in an otherwise returned series
    PARSE_ERROR = "PARSE_ERROR"                # payload not parseable (e.g. JS-rendered)
    NO_TIMESTAMP = "NO_TIMESTAMP"              # value exists but as-of time unknown
    SUSPECT_VALUE = "SUSPECT_VALUE"            # parsed, but fails plausibility checks
    NOT_PROVIDED = "NOT_PROVIDED"              # source does not offer this metric


# Statuses whose value may be kept for inspection but must never feed a signal.
QUARANTINED_WITH_VALUE = {Availability.NO_TIMESTAMP, Availability.SUSPECT_VALUE,
                          Availability.INCOMPLETE_PERIOD}


class MarketType(str, Enum):
    SPOT = "spot"
    USDM_PERP = "usdm_perpetual"
    INVERSE_PERP = "inverse_perpetual"
    PERP = "perpetual"                  # venue does not distinguish margin type here
    AGGREGATE_DERIVATIVES = "aggregate_derivatives"
    EXCHANGE_ATTESTATION = "exchange_attestation"   # proof of reserves


class SourceTier(str, Enum):
    PRIMARY = "primary"         # the exchange's own API / page
    VENUE_PROXY = "venue_proxy"  # a different exchange used as a stand-in
    AGGREGATOR = "aggregator"   # CMC/CoinGecko style blended data
    SECONDARY = "secondary"     # news / third-party report about the primary


def utc(ts: str | datetime) -> datetime:
    if isinstance(ts, datetime):
        return ts.astimezone(timezone.utc) if ts.tzinfo else ts.replace(tzinfo=timezone.utc)
    ts = ts.replace("Z", "+00:00")
    d = datetime.fromisoformat(ts)
    return d if d.tzinfo else d.replace(tzinfo=timezone.utc)


def iso(d: Optional[datetime]) -> Optional[str]:
    return None if d is None else d.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


@dataclass
class Record:
    symbol: str                     # BTC, ETH, or asset for PoR
    metric: str                     # close, open_interest, funding_rate, reserve_ratio_pct ...
    market_type: MarketType
    venue: str                      # binance, crypto.com, coinmarketcap ...
    source: str                     # endpoint or URL
    source_tier: SourceTier
    availability: Availability
    event_time: Optional[datetime]  # time the value describes (bar close, funding time, snapshot)
    available_at: Optional[datetime]  # earliest time a live system could have known it
    fetched_at: datetime
    value: Optional[Decimal] = None
    unit: str = ""
    raw: Optional[str] = None       # verbatim string from source, for audit
    note: str = ""
    flags: list = field(default_factory=list)

    def __post_init__(self):
        self.validate()

    def validate(self) -> None:
        a = self.availability
        if a == Availability.OK:
            if self.value is None:
                raise ValueError(f"{self.key()}: OK record without value")
            if self.event_time is None or self.available_at is None:
                raise ValueError(f"{self.key()}: OK record must carry event_time and available_at")
        elif a in QUARANTINED_WITH_VALUE:
            pass  # value optional, kept for audit only
        else:
            if self.value is not None:
                raise ValueError(f"{self.key()}: {a.value} record must have value=None, got {self.value}")

    @property
    def usable(self) -> bool:
        return self.availability == Availability.OK

    def key(self) -> str:
        return f"{self.venue}:{self.market_type.value}:{self.symbol}:{self.metric}@{iso(self.event_time)}"

    def to_dict(self) -> dict:
        d = asdict(self)
        for k in ("event_time", "available_at", "fetched_at"):
            d[k] = iso(getattr(self, k))
        d["value"] = None if self.value is None else str(self.value)
        d["availability"] = self.availability.value
        d["market_type"] = self.market_type.value
        d["source_tier"] = self.source_tier.value
        d["usable"] = self.usable
        return d


def write_jsonl(records, path) -> None:
    with open(path, "w", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r.to_dict(), ensure_ascii=False) + "\n")


def read_jsonl(path) -> list:
    out = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            d = json.loads(line)
            out.append(Record(
                symbol=d["symbol"], metric=d["metric"], market_type=MarketType(d["market_type"]),
                venue=d["venue"], source=d["source"], source_tier=SourceTier(d["source_tier"]),
                availability=Availability(d["availability"]),
                event_time=utc(d["event_time"]) if d["event_time"] else None,
                available_at=utc(d["available_at"]) if d["available_at"] else None,
                fetched_at=utc(d["fetched_at"]),
                value=None if d["value"] is None else Decimal(d["value"]),
                unit=d.get("unit", ""), raw=d.get("raw"), note=d.get("note", ""),
                flags=d.get("flags", []),
            ))
    return out
