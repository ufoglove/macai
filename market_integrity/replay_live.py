"""Replay 2026-11-02..11-03 on the real Binance capture (run on a host that reaches Binance).

    python -m market_integrity.replay_live --sample out/20261103_mac/sample_records.jsonl --out out/20261103_mac

Differences from the fixture replay in run_sample:
  * closes, open interest and funding are the real Binance values captured by run_sample
  * the server honours the publication lag measured on 2026-11-03 (OI hist snapshot
    appears 40 s to 3 min after its timestamp, funding settlement within ~25 s), so
    steps placed a few seconds after the hour see what a live system would see
  * two extra faults on 11-03: a frozen OI feed (200 OK, newest rows missing) and a
    funding settlement published 45 minutes late
"""
from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

from . import binance_public as bp
from .replay import Fault, HistoricalServer, run_replay
from .run_sample import FAULTS, WINDOW_END, WINDOW_START, audit_rows
from .schema import MarketType, iso, read_jsonl, utc

# Measured with a 15 s poll on 2026-11-03 15:00 / 16:00 UTC (see REPORT).
MEASURED_OI_LAG = {"BTCUSDT": timedelta(seconds=55), "ETHUSDT": timedelta(minutes=3)}
MEASURED_FUNDING_LAG = timedelta(seconds=25)

LIVE_FAULTS = FAULTS + [
    Fault("stale_oi", utc("2026-11-03T06:00Z"), utc("2026-11-03T09:00Z"),
          paths=("/futures/data/openInterestHist",)),
    Fault("late_funding", utc("2026-11-03T16:00Z"), utc("2026-11-03T16:45Z"),
          paths=("/fapi/v1/fundingRate",)),
]


@dataclass
class LaggedServer(HistoricalServer):
    """HistoricalServer that only serves an OI snapshot / funding settlement once it was really out."""
    oi_lag: dict = None
    funding_lag: timedelta = MEASURED_FUNDING_LAG

    def __call__(self, url: str):
        status, body = super().__call__(url)
        if status != 200 or not body.startswith("["):
            return status, body
        rows = json.loads(body)
        if "openInterestHist" in url:
            sym = rows[0]["symbol"] if rows else None
            lag = (self.oi_lag or {}).get(sym, timedelta(0))
            frozen = next((f for f in self.faults if f.kind == "stale_oi"
                           and f.start <= self.as_of < f.end), None)
            keep = []
            for r in rows:
                ts = bp._from_ms(r["timestamp"])
                if ts + lag > self.as_of:
                    continue
                if frozen and ts >= frozen.start:
                    continue
                keep.append(r)
            return 200, json.dumps(keep)
        if "fundingRate" in url:
            late = next((f for f in self.faults if f.kind == "late_funding"
                         and f.start <= self.as_of < f.end), None)
            keep = []
            for r in rows:
                slot = bp._from_ms(r["fundingTime"]).replace(second=0, microsecond=0)
                if slot + self.funding_lag > self.as_of:
                    continue
                if late and slot >= late.start:
                    continue
                keep.append(r)
            return 200, json.dumps(keep)
        return status, body


def server_from_capture(records) -> LaggedServer:
    closes, oi, funding = {}, {}, {}
    for r in records:
        if r.venue != "binance" or not r.usable:
            continue
        if r.metric == "close" and r.market_type in (MarketType.SPOT, MarketType.USDM_PERP):
            # HistoricalServer keys bars by open time; Record.event_time is the close.
            closes.setdefault((r.symbol, r.market_type), {})[r.event_time - timedelta(hours=1)] = r.raw
        elif r.metric == "open_interest":
            oi.setdefault(r.symbol, {})[r.event_time] = r.raw
        elif r.metric == "funding_rate":
            funding.setdefault(r.symbol, {})[r.event_time] = r.raw
    return LaggedServer(closes=closes, oi=oi, funding=funding, faults=LIVE_FAULTS,
                        oi_lag=MEASURED_OI_LAG)


def steps(end: datetime):
    """HH:05 every hour, plus HH:00:20 (inside the OI publication lag) every hour."""
    t = WINDOW_START + timedelta(hours=1)
    while t <= end:
        yield t + timedelta(seconds=20)
        if t + timedelta(minutes=5) <= end:
            yield t + timedelta(minutes=5)
        t += timedelta(hours=1)


# A fault can only be noticed once the data it withholds was due.
DETECTABLE_AFTER = {"stale_oi": bp.OI_PUBLISH_LAG, "zero_oi": bp.OI_PUBLISH_LAG,
                    "late_funding": bp.FUNDING_PUBLISH_LAG}


def expected_gate(as_of: datetime, symbol: str) -> str:
    for f in LIVE_FAULTS:
        if not (f.start <= as_of < f.end) or symbol not in f.symbols:
            continue
        if as_of < f.start + DETECTABLE_AFTER.get(f.kind, timedelta(0)):
            continue
        return "HALT"
    return "TRADEABLE"


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--sample", required=True)
    ap.add_argument("--out", required=True)
    a = ap.parse_args(argv)
    records = read_jsonl(a.sample)
    captured = max(r.fetched_at for r in records if r.venue == "binance" and r.fetched_at)
    end = min(captured, WINDOW_END)
    rows = run_replay(server_from_capture(records), WINDOW_START, WINDOW_END, list(steps(end)))
    result = audit_rows(rows, expected_gate)
    out = Path(a.out)
    out.mkdir(parents=True, exist_ok=True)
    timeline = [{k: v for k, v in r.items() if k != "records"} for r in rows]
    (out / "replay_live_timeline.json").write_text(json.dumps(timeline, indent=1, ensure_ascii=False))
    (out / "replay_live_audit.json").write_text(json.dumps(
        {"capture": iso(captured), "replay_end": iso(end), **result}, indent=1, ensure_ascii=False))
    print(json.dumps({k: v for k, v in result.items() if k != "problems"}, ensure_ascii=False))
    print(f"problems: {len(result['problems'])}")
    for p in result["problems"][:30]:
        print("  ", p)
    return 0 if not result["problems"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
