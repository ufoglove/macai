"""Normalise public fallback captures (Crypto.com, CoinMarketCap, PoR reports) into Records.

These captures were taken through read-only MCP connectors / web fetch because the
cloud sandbox cannot reach Binance directly. They are stored verbatim under
``fixtures/`` so the normalisation is reproducible.
"""
from __future__ import annotations

import json
import re
from datetime import timedelta
from decimal import Decimal
from pathlib import Path

from .binance_public import _dec, expected_funding_times
from .schema import Availability, MarketType, Record, SourceTier, utc

TF = {"1h": timedelta(hours=1), "4h": timedelta(hours=4)}
CDC = "crypto.com"


def _load(p: Path) -> dict:
    return json.loads(p.read_text(encoding="utf-8"))


def _candles(symbol, market, instrument, tf, rows, fetched_at, source):
    out = []
    step = TF[tf]
    for r in sorted(rows, key=lambda r: r["timestamp"]):
        open_t = utc(r["timestamp"])
        close_t = open_t + step
        val = _dec(r["close"])
        common = dict(symbol=symbol, metric="close", market_type=market, venue=CDC,
                      source=f"{source} {instrument} {tf}", source_tier=SourceTier.VENUE_PROXY,
                      fetched_at=fetched_at, unit="USD" if market == MarketType.PERP else "USDT",
                      raw=r["close"], event_time=close_t)
        if close_t > fetched_at:
            out.append(Record(**common, availability=Availability.INCOMPLETE_PERIOD, value=val,
                              available_at=None, note="bar still open at capture time"))
        else:
            out.append(Record(**common, availability=Availability.OK, value=val, available_at=close_t))
    return out


def load_cryptocom(fx: Path) -> list:
    out = []
    for sym, inst in (("BTC", "BTCUSD-PERP"), ("ETH", "ETHUSD-PERP")):
        d = _load(fx / f"cryptocom_{inst}_1h.json")
        out += _candles(sym, MarketType.PERP, inst, "1h", d["data"], utc(d["_capture"]["fetched_at"]),
                        d["_capture"]["channel"])
    d = _load(fx / "cryptocom_spot_4h.json")
    for sym, inst in (("BTC", "BTC_USDT"), ("ETH", "ETH_USDT")):
        out += _candles(sym, MarketType.SPOT, inst, "4h", d[inst]["data"],
                        utc(d["_capture"]["fetched_at"]), d["_capture"]["channel"])
    t = _load(fx / "cryptocom_tickers.json")
    for sym, inst in (("BTC", "BTCUSD-PERP"), ("ETH", "ETHUSD-PERP")):
        tk = t[inst]
        ts = utc(tk["timestamp"])
        val = _dec(tk["open_interest"])
        ok = val is not None and val > 0
        out.append(Record(symbol=sym, metric="open_interest", market_type=MarketType.PERP, venue=CDC,
                          source=f"{t['_capture']['channel']} {inst}", source_tier=SourceTier.VENUE_PROXY,
                          availability=Availability.OK if ok else Availability.SUSPECT_VALUE,
                          event_time=ts, available_at=ts, fetched_at=ts, value=val,
                          unit=f"{sym} contracts", raw=tk["open_interest"],
                          note="current snapshot only; this channel has no OI history"))
        # The 11/02-11/03 OI history and funding are not offered by this channel.
        out.append(Record(symbol=sym, metric="open_interest_history", market_type=MarketType.PERP,
                          venue=CDC, source=t["_capture"]["channel"], source_tier=SourceTier.VENUE_PROXY,
                          availability=Availability.NOT_PROVIDED, event_time=None, available_at=None,
                          fetched_at=ts, note="hourly OI history not exposed by the read-only connector"))
        for slot in expected_funding_times(utc("2026-11-02T00:00Z"), utc("2026-11-03T16:00Z")):
            out.append(Record(symbol=sym, metric="funding_rate", market_type=MarketType.PERP, venue=CDC,
                              source=t["_capture"]["channel"], source_tier=SourceTier.VENUE_PROXY,
                              availability=(Availability.NOT_YET_PUBLISHED if slot > ts
                                            else Availability.NOT_PROVIDED),
                              event_time=slot, available_at=None, fetched_at=ts,
                              note="funding history not exposed by the read-only connector"))
    return out


_SCALE = {"": 1, "K": 10**3, "M": 10**6, "B": 10**9, "T": 10**12}


def parse_display_number(s: str):
    m = re.fullmatch(r"\s*([+-]?[\d,]*\.?\d+)\s*([KMBT]?)\s*", s or "")
    if not m:
        return None
    return Decimal(m.group(1).replace(",", "")) * _SCALE[m.group(2)]


def load_cmc(fx: Path) -> list:
    d = _load(fx / "cmc_global_derivatives.json")
    fetched = utc(d["_capture"]["fetched_at"])
    src = d["_capture"]["channel"]
    out = []

    def rec(symbol, metric, raw, unit, flags=(), note=""):
        out.append(Record(symbol=symbol, metric=metric, market_type=MarketType.AGGREGATE_DERIVATIVES,
                          venue="coinmarketcap", source=src, source_tier=SourceTier.AGGREGATOR,
                          availability=Availability.NO_TIMESTAMP, event_time=None, available_at=None,
                          fetched_at=fetched, value=parse_display_number(raw), raw=raw, unit=unit,
                          flags=list(flags), note=note or "payload has no as-of time; not usable point-in-time"))

    rec("ALL", "open_interest_total", d["totalOpenInterest"]["current"], "USD")
    rec("ALL", "funding_rate_blended", d["fundingRate"]["current"], "unspecified",
        flags=["unit_unspecified", "averaging_method_undisclosed"])
    fut_vol = parse_display_number(d["futures"]["volume"]["total_usd_24h"])
    perp_vol = parse_display_number(d["perpetuals"]["volume"]["total_usd_24h"])
    flags = []
    if fut_vol and perp_vol and perp_vol / fut_vol > 100:
        flags.append("perp_volume_implausible_vs_futures")
    out.append(Record(symbol="ALL", metric="volume_24h_perpetuals", market_type=MarketType.AGGREGATE_DERIVATIVES,
                      venue="coinmarketcap", source=src, source_tier=SourceTier.AGGREGATOR,
                      availability=Availability.SUSPECT_VALUE if flags else Availability.NO_TIMESTAMP,
                      event_time=None, available_at=None, fetched_at=fetched, value=perp_vol,
                      raw=d["perpetuals"]["volume"]["total_usd_24h"], unit="USD", flags=flags + ["no_timestamp"],
                      note="162 T USD/24h perpetual volume vs 96.65 B futures: magnitude error at source"))
    liq = d["btc_liquidations"]
    for w in ("1h", "4h", "12h", "24h"):
        raw = liq[f"total_usd_{w}"]["total"]
        fl = []
        if parse_display_number(raw) == 0:
            fl.append("ambiguous_zero")
        rec("BTC", f"liquidations_usd_{w}", raw, "USD", flags=fl,
            note=("source reports 0 while the 12h window shows 61.64 M; a 0 here cannot be "
                  "distinguished from 'not yet aggregated'") if fl else "")
    return out


def load_por(fx: Path) -> list:
    d = _load(fx / "binance_por_public_reports.json")
    fetched = utc(d["_capture"]["fetched_at"])
    prim = d["primary_page"]
    out = [Record(symbol="ALL", metric="por_page", market_type=MarketType.EXCHANGE_ATTESTATION,
                  venue="binance", source=prim["url"], source_tier=SourceTier.PRIMARY,
                  availability=Availability.PARSE_ERROR, event_time=None, available_at=None,
                  fetched_at=fetched, note=f"{prim['result']}; direct HTTP: {prim['direct_http_from_cloud']}")]
    cn, ct = d["reports"][0], d["reports"][1]
    snap = utc(cn["snapshot_date"] + "T00:00:00Z")
    pub = utc(cn["published_at"])
    for asset, fig in cn["figures"].items():
        for metric, key, unit in (("reserve_holdings", "holdings", asset), ("reserve_ratio_pct", "reserve_ratio_pct", "%")):
            raw = fig.get(key)
            common = dict(symbol=asset, metric=metric, market_type=MarketType.EXCHANGE_ATTESTATION,
                          venue="binance", source=cn["url"], source_tier=SourceTier.SECONDARY,
                          fetched_at=fetched, unit=unit)
            if raw is None:
                other = ct["figures"].get(asset, {}).get(key)
                out.append(Record(**common, availability=Availability.NO_TIMESTAMP if other else Availability.NOT_PROVIDED,
                                  event_time=None, available_at=None,
                                  value=_dec(other) if other else None, raw=other,
                                  flags=["single_secondary_source", "snapshot_date_not_stated"] if other else [],
                                  note=(f"not stated by {cn['source']}; {ct['source']} gives {other} without a snapshot date"
                                        if other else "not stated by source")))
            else:
                out.append(Record(**common, availability=Availability.OK, event_time=snap, available_at=pub,
                                  value=_dec(raw), raw=raw, flags=["snapshot_time_of_day_not_stated", "secondary_source"],
                                  note="snapshot date 2026-10-31 per report; event_time set to 00:00Z of that date"))
    for metric, expected, note in (
        ("zk_proof_full", utc("2026-11-03T00:00Z"), "full ZK reserve proof expected by 2026-11-03 (40-50h generation)"),
        ("por_snapshot_next", utc("2026-11-04T00:00Z"), "next reserves snapshot announced for 2026-11-04"),
    ):
        out.append(Record(symbol="ALL", metric=metric, market_type=MarketType.EXCHANGE_ATTESTATION,
                          venue="binance", source=f"{cn['url']} | {ct['url']}", source_tier=SourceTier.SECONDARY,
                          availability=Availability.NOT_YET_PUBLISHED, event_time=expected, available_at=None,
                          fetched_at=fetched, note=note))
    return out


def load_all(fx: Path) -> list:
    return load_cryptocom(fx) + load_cmc(fx) + load_por(fx)
