import json
import unittest
from datetime import timedelta
from decimal import Decimal

from market_integrity import binance_public as bp
from market_integrity.fallback_sources import load_cmc, load_cryptocom, load_por
from market_integrity.replay import Fault, build_features, gate, run_replay
from market_integrity.run_sample import (FIX, REPLAY_END, WINDOW_END, WINDOW_START, audit,
                                         replay_server, replay_steps)
from market_integrity.schema import Availability, MarketType, Record, SourceTier, utc

T0 = utc("2026-11-02T00:00Z")


def fixed(status, body):
    return lambda url: (status, body)


def raising(exc):
    def t(url):
        raise exc
    return t


class SchemaTests(unittest.TestCase):
    def _rec(self, **kw):
        base = dict(symbol="BTCUSDT", metric="funding_rate", market_type=MarketType.USDM_PERP, venue="binance",
                    source="x", source_tier=SourceTier.PRIMARY, event_time=T0, available_at=T0, fetched_at=T0)
        base.update(kw)
        return Record(**base)

    def test_missing_status_cannot_carry_a_number(self):
        for a in (Availability.SOURCE_UNREACHABLE, Availability.EXCHANGE_ERROR, Availability.NOT_YET_PUBLISHED,
                  Availability.EMPTY_RESPONSE, Availability.MISSING_IN_SERIES, Availability.RATE_LIMITED):
            with self.assertRaises(ValueError):
                self._rec(availability=a, value=Decimal(0))

    def test_ok_requires_value_and_timestamps(self):
        with self.assertRaises(ValueError):
            self._rec(availability=Availability.OK, value=None)
        with self.assertRaises(ValueError):
            self._rec(availability=Availability.OK, value=Decimal("0.0001"), event_time=None)


class HttpClassificationTests(unittest.TestCase):
    def test_mapping(self):
        self.assertEqual(bp.classify_http(451, ""), Availability.SOURCE_UNREACHABLE)
        self.assertEqual(bp.classify_http(403, ""), Availability.SOURCE_UNREACHABLE)
        self.assertEqual(bp.classify_http(429, ""), Availability.RATE_LIMITED)
        self.assertEqual(bp.classify_http(418, ""), Availability.RATE_LIMITED)
        self.assertEqual(bp.classify_http(503, ""), Availability.EXCHANGE_ERROR)
        self.assertEqual(bp.classify_http(200, '{"code":-1001,"msg":"x"}'), Availability.EXCHANGE_ERROR)
        self.assertEqual(bp.classify_http(200, "<html>"), Availability.PARSE_ERROR)
        self.assertIsNone(bp.classify_http(200, "[]"))


class FetcherTests(unittest.TestCase):
    now = utc("2026-11-03T01:35Z")
    end = utc("2026-11-03T16:00Z")

    def test_unsettled_funding_is_not_published_not_zero(self):
        rows = [{"fundingTime": bp._ms(t) + 2, "fundingRate": "0.00008"} for t in
                bp.expected_funding_times(T0, self.now)]
        recs = bp.fetch_funding("BTCUSDT", T0, self.end, self.now, fixed(200, json.dumps(rows)))
        future = [r for r in recs if r.event_time > self.now]
        self.assertEqual(len(future), 2)  # 08:00 and 16:00 on 11-03
        for r in future:
            self.assertEqual(r.availability, Availability.NOT_YET_PUBLISHED)
            self.assertIsNone(r.value)
        self.assertTrue(all(r.usable for r in recs if r.event_time <= self.now))

    def test_outage_marks_every_past_slot_and_keeps_value_none(self):
        for transport, expected in ((fixed(503, "busy"), Availability.EXCHANGE_ERROR),
                                    (fixed(451, "restricted"), Availability.SOURCE_UNREACHABLE),
                                    (raising(OSError("Tunnel connection failed: 403")), Availability.SOURCE_UNREACHABLE),
                                    (fixed(429, ""), Availability.RATE_LIMITED)):
            recs = bp.fetch_open_interest_hist("BTCUSDT", T0, self.end, self.now, transport=transport)
            past = [r for r in recs if r.event_time <= self.now]
            self.assertTrue(past)
            self.assertTrue(all(r.availability == expected and r.value is None for r in past))

    def test_gap_in_series_is_flagged(self):
        rows = [[bp._ms(T0 + timedelta(hours=h)), "1", "1", "1", "77000", "1"] for h in range(0, 25) if h != 5]
        recs = bp.fetch_klines("BTCUSDT", MarketType.USDM_PERP, "1h", T0, T0 + timedelta(hours=25),
                               self.now, fixed(200, json.dumps(rows)))
        gap = [r for r in recs if r.availability == Availability.MISSING_IN_SERIES]
        self.assertEqual([r.event_time for r in gap], [T0 + timedelta(hours=6)])
        self.assertIsNone(gap[0].value)

    def test_empty_200_is_not_normal(self):
        recs = bp.fetch_klines("ETHUSDT", MarketType.SPOT, "1h", T0, T0 + timedelta(hours=3), self.now,
                               fixed(200, "[]"))
        self.assertTrue(all(r.availability == Availability.EMPTY_RESPONSE for r in recs))

    def test_open_bar_is_incomplete(self):
        open_t = utc("2026-11-03T01:00Z")
        rows = [[bp._ms(open_t), "1", "1", "1", "77172.1", "1"]]
        recs = bp.fetch_klines("BTCUSDT", MarketType.USDM_PERP, "1h", open_t, open_t + timedelta(hours=1),
                               self.now, fixed(200, json.dumps(rows)))
        self.assertEqual(recs[0].availability, Availability.INCOMPLETE_PERIOD)
        self.assertFalse(recs[0].usable)

    def test_literal_zero_open_interest_is_suspect_and_halts(self):
        rows = [{"timestamp": bp._ms(T0 + timedelta(hours=h)), "sumOpenInterest": "0.000"} for h in range(3)]
        now = T0 + timedelta(hours=2, minutes=5)
        recs = bp.fetch_open_interest_hist("BTCUSDT", T0, T0 + timedelta(hours=2), now,
                                           transport=fixed(200, json.dumps(rows)))
        self.assertTrue(all(r.availability == Availability.SUSPECT_VALUE for r in recs))
        self.assertEqual(build_features(recs, "BTCUSDT", now)["usdm_perpetual.open_interest"].status,
                         "SUSPECT_VALUE")


class ReplayTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.rows = run_replay(replay_server(), WINDOW_START, WINDOW_END, list(replay_steps()))
        cls.result = audit(cls.rows)

    def test_no_invariant_violations(self):
        self.assertEqual(self.result["problems"], [])

    def test_every_fault_step_halts(self):
        self.assertEqual(self.result["halts"], 11)
        self.assertEqual(self.result["naive_would_trade_on_bad_data"], 11)

    def test_naive_pipeline_turns_outage_into_zero(self):
        row = next(r for r in self.rows if r["as_of"].startswith("2026-11-02T14:05") and r["symbol"] == "BTCUSDT")
        self.assertEqual(row["gate"], "HALT")
        self.assertEqual(row["naive"]["usdm_perpetual.funding_rate"], "0")
        self.assertIsNone(row["features"]["usdm_perpetual.funding_rate"]["value"])

    def test_missed_funding_settlement_halts(self):
        srv = replay_server()
        srv.funding = {s: {t: v for t, v in d.items() if t <= T0} for s, d in srv.funding.items()}
        rows = run_replay(srv, WINDOW_START, WINDOW_END, [utc("2026-11-02T09:05Z")], symbols=("BTCUSDT",))
        f = rows[0]["features"]["usdm_perpetual.funding_rate"]
        self.assertEqual(f["status"], "MISSING_IN_SERIES")
        self.assertIsNone(f["value"])
        self.assertEqual(rows[0]["gate"], "HALT")


class FallbackTests(unittest.TestCase):
    def test_cmc_zero_liquidation_is_not_usable(self):
        recs = {r.metric: r for r in load_cmc(FIX)}
        r = recs["liquidations_usd_1h"]
        self.assertIn("ambiguous_zero", r.flags)
        self.assertFalse(r.usable)
        self.assertEqual(recs["volume_24h_perpetuals"].availability, Availability.SUSPECT_VALUE)
        self.assertTrue(all(not r.usable for r in recs.values()))

    def test_cryptocom_last_bar_incomplete_and_funding_not_provided(self):
        recs = load_cryptocom(FIX)
        perp = [r for r in recs if r.metric == "close" and r.market_type == MarketType.PERP and r.symbol == "BTC"]
        self.assertEqual(perp[-1].availability, Availability.INCOMPLETE_PERIOD)
        self.assertEqual(sum(r.usable for r in perp), 25)
        fund = [r for r in recs if r.metric == "funding_rate"]
        self.assertTrue(fund and all(r.value is None and not r.usable for r in fund))

    def test_por_future_publications(self):
        recs = load_por(FIX)
        nyp = {r.metric for r in recs if r.availability == Availability.NOT_YET_PUBLISHED}
        self.assertEqual(nyp, {"zk_proof_full", "por_snapshot_next"})
        btc = next(r for r in recs if r.symbol == "BTC" and r.metric == "reserve_ratio_pct")
        self.assertEqual(btc.value, Decimal("100.04"))
        self.assertEqual(btc.source_tier, SourceTier.SECONDARY)
        eth = next(r for r in recs if r.symbol == "ETH" and r.metric == "reserve_ratio_pct")
        self.assertFalse(eth.usable)


if __name__ == "__main__":
    unittest.main()


class PublicationLagTests(unittest.TestCase):
    """Binance serves an OI snapshot 40 s to 3 min after its timestamp (measured 2026-11-03)."""

    def test_oi_inside_lag_is_not_yet_published_and_previous_snapshot_is_used(self):
        rows = [{"timestamp": bp._ms(T0 + timedelta(hours=h)), "sumOpenInterest": "95000"} for h in range(5)]
        now = T0 + timedelta(hours=5, seconds=20)       # 05:00 snapshot not out yet
        recs = bp.fetch_open_interest_hist("BTCUSDT", T0, T0 + timedelta(hours=5), now,
                                           transport=fixed(200, json.dumps(rows)))
        last = recs[-1]
        self.assertEqual(last.availability, Availability.NOT_YET_PUBLISHED)
        self.assertIsNone(last.value)
        f = build_features(recs, "BTCUSDT", now)["usdm_perpetual.open_interest"]
        self.assertEqual(f.status, "OK")
        self.assertEqual(f.as_of_event, "2026-11-02T04:00:00.000Z")

    def test_oi_available_at_includes_publication_lag(self):
        rows = [{"timestamp": bp._ms(T0 + timedelta(hours=h)), "sumOpenInterest": "95000"} for h in range(2)]
        now = T0 + timedelta(hours=1, minutes=1)        # served early, but a live system could not count on it
        recs = bp.fetch_open_interest_hist("BTCUSDT", T0, T0 + timedelta(hours=1), now,
                                           transport=fixed(200, json.dumps(rows)))
        self.assertEqual(recs[-1].available_at, T0 + timedelta(hours=1) + bp.OI_PUBLISH_LAG)
        f = build_features(recs, "BTCUSDT", now)["usdm_perpetual.open_interest"]
        self.assertEqual(f.as_of_event, "2026-11-02T00:00:00.000Z")

    def test_oi_absent_after_lag_is_missing(self):
        rows = [{"timestamp": bp._ms(T0 + timedelta(hours=h)), "sumOpenInterest": "95000"} for h in range(5)]
        now = T0 + timedelta(hours=5) + bp.OI_PUBLISH_LAG
        recs = bp.fetch_open_interest_hist("BTCUSDT", T0, T0 + timedelta(hours=5), now,
                                           transport=fixed(200, json.dumps(rows)))
        self.assertEqual(recs[-1].availability, Availability.MISSING_IN_SERIES)
        self.assertEqual(gate(build_features(recs, "BTCUSDT", now)), "HALT")

    def test_funding_inside_lag_is_not_yet_published(self):
        rows = [{"fundingTime": bp._ms(T0) + 2, "fundingRate": "0.0001"}]
        now = T0 + timedelta(hours=8, seconds=10)
        recs = bp.fetch_funding("BTCUSDT", T0, T0 + timedelta(hours=8), now, fixed(200, json.dumps(rows)))
        self.assertEqual(recs[-1].availability, Availability.NOT_YET_PUBLISHED)
        recs = bp.fetch_funding("BTCUSDT", T0, T0 + timedelta(hours=8), now + timedelta(minutes=5),
                                fixed(200, json.dumps(rows)))
        self.assertEqual(recs[-1].availability, Availability.MISSING_IN_SERIES)


class LiveCaptureReplayTests(unittest.TestCase):
    """Replay on the real Binance capture taken from the Mac mini after the window closed."""

    @classmethod
    def setUpClass(cls):
        from market_integrity import replay_live as rl
        from market_integrity.schema import read_jsonl
        recs = read_jsonl(FIX / "binance_live_capture.jsonl")
        cls.end = min(max(r.fetched_at for r in recs), WINDOW_END)
        cls.rows = run_replay(rl.server_from_capture(recs), WINDOW_START, WINDOW_END, list(rl.steps(cls.end)))
        cls.result = rl.audit_rows(cls.rows, rl.expected_gate)

    def test_no_invariant_violations(self):
        self.assertEqual(self.result["problems"], [])

    def test_steps_inside_publication_lag_stay_tradeable(self):
        quiet = [r for r in self.rows if r["as_of"].endswith(":00:20.000Z") and r["as_of"] < "2026-11-02T14:00"]
        self.assertTrue(quiet)
        self.assertTrue(all(r["gate"] == "TRADEABLE" for r in quiet))

    def test_frozen_oi_feed_halts(self):
        row = next(r for r in self.rows if r["as_of"].startswith("2026-11-03T07:05") and r["symbol"] == "ETHUSDT")
        self.assertEqual(row["gate"], "HALT")
        self.assertEqual(row["features"]["usdm_perpetual.open_interest"]["status"], "MISSING_IN_SERIES")

    def test_late_funding_halts_even_though_previous_rate_is_fresh_enough(self):
        row = next(r for r in self.rows if r["as_of"].startswith("2026-11-03T16:05") and r["symbol"] == "BTCUSDT")
        self.assertEqual(row["gate"], "HALT")
        self.assertEqual(row["features"]["usdm_perpetual.funding_rate"]["status"], "MISSING_IN_SERIES")
