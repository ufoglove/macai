"""Build the 2026-11-02..11-03 market-data credibility sample and run the replay.

    python -m market_integrity.run_sample --out out/20261103

Step 1 tries Binance public endpoints live (works on the Mac mini; in a sandbox
that blocks Binance it records SOURCE_UNREACHABLE instead of numbers).
Step 2 adds the fallback captures in fixtures/. Step 3 replays the window
hour by hour with injected faults and checks that no missing value ever
becomes 0 or "normal".
"""
from __future__ import annotations

import argparse
import collections
import csv
import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

from . import binance_public as bp
from .fallback_sources import load_all
from .replay import Fault, HistoricalServer, run_replay
from .schema import Availability, MarketType, iso, utc, write_jsonl

HERE = Path(__file__).parent
FIX = HERE / "fixtures" / "20261103"
WINDOW_START = utc("2026-11-02T00:00:00Z")
WINDOW_END = utc("2026-11-04T00:00:00Z")      # covers all of 11-03 so future slots show up
REPLAY_END = utc("2026-11-03T01:35:00Z")       # capture time: replay stops at "now"

FAULTS = [
    Fault("outage_5xx", utc("2026-11-02T14:00Z"), utc("2026-11-02T16:00Z")),
    Fault("geo_block", utc("2026-11-02T18:00Z"), utc("2026-11-02T19:00Z")),
    Fault("empty_200", utc("2026-11-02T21:00Z"), utc("2026-11-02T22:00Z"),
          paths=("/futures/data/openInterestHist",)),
    Fault("zero_oi", utc("2026-11-02T23:00Z"), utc("2026-11-03T00:00Z"), symbols=("BTCUSDT",)),
    Fault("maintenance_code", utc("2026-11-03T00:00Z"), utc("2026-11-03T00:30Z"),
          paths=("/fapi/v1/fundingRate",)),
]


def replay_server() -> HistoricalServer:
    """Binance-shaped history. Prices = real Crypto.com perp closes; OI/funding = labelled synthetic."""
    closes = {}
    for sym, inst in (("BTCUSDT", "BTCUSD-PERP"), ("ETHUSDT", "ETHUSD-PERP")):
        d = json.loads((FIX / f"cryptocom_{inst}_1h.json").read_text())
        series = {utc(r["timestamp"]): r["close"] for r in d["data"]}
        closes[(sym, MarketType.USDM_PERP)] = series
        closes[(sym, MarketType.SPOT)] = series
    oi, funding = {}, {}
    for sym, base in (("BTCUSDT", 90000), ("ETHUSDT", 2000000)):
        oi[sym] = {WINDOW_START + timedelta(hours=h): f"{base + 37 * h:.3f}" for h in range(0, 49)}
        funding[sym] = {t: "0.00010000" for t in bp.expected_funding_times(WINDOW_START, WINDOW_END)}
    return HistoricalServer(closes=closes, oi=oi, funding=funding, faults=FAULTS)


def replay_steps():
    t = WINDOW_START + timedelta(hours=1, minutes=5)
    while t <= REPLAY_END:
        yield t
        t += timedelta(hours=1)
    yield REPLAY_END


def expected_gate(as_of: datetime) -> str:
    for f in FAULTS:
        if f.start <= as_of < f.end:
            return "HALT"
    return "TRADEABLE"


def audit(rows) -> dict:
    """Checks that hold for every step of the replay."""
    problems = []
    misreads = collections.Counter()
    for row in rows:
        for r in row["records"]:
            if r.availability not in (Availability.OK, Availability.SUSPECT_VALUE,
                                      Availability.INCOMPLETE_PERIOD) and r.value is not None:
                problems.append(f"{row['as_of']} {r.key()} {r.availability.value} carries value {r.value}")
            if r.event_time and r.event_time > utc(row["as_of"]) and r.availability == Availability.OK:
                problems.append(f"{row['as_of']} {r.key()} future observation marked OK")
        for name, f in row["features"].items():
            if f["status"] != "OK" and f["value"] is not None:
                problems.append(f"{row['as_of']} {row['symbol']} {name} non-OK feature has value")
            if f["status"] != "OK" and Decimal(row["naive"][name]) == 0:
                misreads[f"{name}:{f['status']}"] += 1
        exp = expected_gate(utc(row["as_of"]))
        # zero_oi only hits BTC
        if exp == "HALT" and row["symbol"] == "ETHUSDT" and _only_fault(utc(row["as_of"])) == "zero_oi":
            exp = "TRADEABLE"
        if row["gate"] != exp:
            problems.append(f"{row['as_of']} {row['symbol']} gate={row['gate']} expected={exp}")
    halts = sum(r["gate"] == "HALT" for r in rows)
    naive_trades_in_fault = sum(r["gate"] == "HALT" and r["naive_gate"] == "TRADEABLE" for r in rows)
    return {"steps": len(rows), "halts": halts, "problems": problems,
            "naive_would_trade_on_bad_data": naive_trades_in_fault,
            "naive_zero_misreads": dict(misreads)}


def _only_fault(as_of):
    hits = [f.kind for f in FAULTS if f.start <= as_of < f.end]
    return hits[0] if len(hits) == 1 else None


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="out/20261103")
    ap.add_argument("--skip-live", action="store_true")
    a = ap.parse_args(argv)
    out = Path(a.out)
    out.mkdir(parents=True, exist_ok=True)

    now = datetime.now(timezone.utc)
    live = [] if a.skip_live else bp.collect(WINDOW_START, WINDOW_END, now)
    sample = live + load_all(FIX)
    write_jsonl(sample, out / "sample_records.jsonl")
    with open(out / "sample_records.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        cols = ["venue", "market_type", "symbol", "metric", "event_time", "available_at", "fetched_at",
                "value", "unit", "availability", "usable", "source_tier", "flags", "source", "note"]
        w.writerow(cols)
        for r in sample:
            d = r.to_dict()
            d["flags"] = "|".join(d["flags"])
            w.writerow([d[c] for c in cols])

    summary = collections.Counter((r.venue, r.market_type.value, r.metric, r.availability.value) for r in sample)
    rows = run_replay(replay_server(), WINDOW_START, WINDOW_END, list(replay_steps()))
    result = audit(rows)
    timeline = [{k: v for k, v in r.items() if k != "records"} for r in rows]
    (out / "replay_timeline.json").write_text(json.dumps(timeline, indent=1, ensure_ascii=False))
    (out / "replay_audit.json").write_text(json.dumps(result, indent=1, ensure_ascii=False))
    (out / "sample_summary.json").write_text(json.dumps(
        {"captured_at": iso(now), "live_binance_attempted": not a.skip_live,
         "counts": [{"venue": k[0], "market_type": k[1], "metric": k[2], "availability": k[3], "n": n}
                    for k, n in sorted(summary.items())]}, indent=1, ensure_ascii=False))
    print(json.dumps({k: v for k, v in result.items() if k != "problems"}, ensure_ascii=False))
    print(f"problems: {len(result['problems'])}")
    for p in result["problems"][:20]:
        print("  ", p)
    return 0 if not result["problems"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
