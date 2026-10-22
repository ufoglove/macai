"""ETF 資金流 vintage：Farside 表解析、未公布判定、只追加紀錄、as-of 不偷看（2026-10-22）。"""
import json

import pandas as pd
import pytest

from ai_layer import etf_vintages as ev

# 2026 版 /btc/ 頁：Total 在上一列表頭；BTC 迷你信託 MSBT 中途上市；20 Oct IBIT 未公布
NEW_LAYOUT = """
<table class="etf">
<tr><td></td><td></td><td></td><td></td><td></td><td>Total</td></tr>
<tr><td></td><td>IBIT</td><td>FBTC</td><td>GBTC</td><td>MSBT</td><td></td></tr>
<tr><td>Fee</td><td>0.25%</td><td>0.25%</td><td>1.50%</td><td>0.14%</td><td></td></tr>
<tr><td>15 Oct 2026</td><td>16.8</td><td>0.0</td><td>(7.6)</td><td>-</td><td>9.2</td></tr>
<tr><td>16 Oct 2026</td><td>-</td><td>-</td><td>-</td><td>-</td><td>0.0</td></tr>
<tr><td>19 Oct 2026</td><td>1,665.2</td><td>(83.6)</td><td>(54.9)</td><td>2.0</td><td>1,528.7</td></tr>
<tr><td>20 Oct 2026</td><td>-</td><td>3.6</td><td>0.0</td><td>0.0</td><td>3.6</td></tr>
<tr><td>Total</td><td>66,233</td><td>10,385</td><td>(27,998)</td><td>800</td><td>57,469</td></tr>
<tr><td>Average</td><td>95.3</td><td>14.9</td><td>(40.2)</td><td>5.9</td><td>82.7</td></tr>
</table>
"""

# 2024 版 ?p=997：單列表頭，第一欄寫 Date、最後一欄寫 Total
OLD_LAYOUT = """
<table class="etf">
<tr><th>Date</th><th>IBIT</th><th>FBTC</th><th>GBTC</th><th>Total</th></tr>
<tr></tr>
<tr><td>28 Mar 2024</td><td>95.1</td><td>68.1</td><td>(104.9)</td><td>58.3</td></tr>
<tr><td>01 Apr 2024</td><td>165.9</td><td>44.0</td><td>(302.6)</td><td>(92.7)</td></tr>
</table>
"""

# 2024-01 版：英式日／月、整數、減號負數、未公布留白、尚未到的日期整列空白
JAN_2024_LAYOUT = """
<table><tr><td>Date</td><td>IBIT</td><td>FBTC</td><td>GBTC</td><td>Total</td></tr>
<tr><td>11/01/2024</td><td>112</td><td>227</td><td>-95</td><td>655</td></tr>
<tr><td>29/01/2024</td><td></td><td>208</td><td>-192</td><td>56</td></tr>
<tr><td>30/01/2024</td><td></td><td></td><td></td><td></td></tr>
</table>
"""

ETH_LAYOUT = """
<table class="etf"><tr><td></td><td>ETHA</td><td>FETH</td><td>ETHE</td><td></td></tr>
<tr><td>23 Jul 2024</td><td>266.5</td><td>71.3</td><td>(484.1)</td><td>(146.3)</td></tr></table>
"""


def test_parse_new_layout_units_negatives_holiday_and_pending():
    [table] = ev.parse_farside(NEW_LAYOUT)
    assert table["asset"] == "BTC" and table["tickers"] == ["IBIT", "FBTC", "GBTC", "MSBT"]
    rows = {r["day"]: r for r in table["rows"]}
    assert "2026-10-16" not in rows                       # 整列「-」＝休市，不記
    assert rows["2026-10-19"]["total_usd"] == 1528.7e6
    assert rows["2026-10-19"]["funds_usd"]["IBIT"] == 1665.2e6 and rows["2026-10-19"]["funds_usd"]["GBTC"] == -54.9e6
    assert rows["2026-10-15"]["pending"] == []            # MSBT 上市前的「-」不算未公布
    assert "MSBT" not in rows["2026-10-15"]["funds_usd"]
    assert rows["2026-10-20"]["pending"] == ["IBIT"]      # 部分加總
    assert "Total" not in rows and len(rows) == 3


def test_parse_old_layout_and_eth():
    [btc] = ev.parse_farside(OLD_LAYOUT)
    assert btc["asset"] == "BTC" and [r["total_usd"] for r in btc["rows"]] == [58.3e6, -92.7e6]
    [eth] = ev.parse_farside(ETH_LAYOUT)
    assert eth["asset"] == "ETH" and eth["rows"][0]["total_usd"] == -146.3e6


def test_parse_january_2024_layout():
    [table] = ev.parse_farside(JAN_2024_LAYOUT)
    rows = table["rows"]
    assert [r["day"] for r in rows] == ["2024-01-11", "2024-01-29"]
    assert rows[0]["total_usd"] == 655e6 and rows[0]["funds_usd"]["GBTC"] == -95e6 and rows[0]["pending"] == []
    assert rows[1]["pending"] == ["IBIT"] and rows[1]["total_usd"] == 56e6


def test_challenge_page_and_unknown_tables_yield_nothing():
    assert ev.parse_farside("<html><title>Just a moment...</title></html>") == []
    assert ev.parse_farside("<table><tr><td></td><td>2800</td><td>3067</td><td>9042</td></tr></table>") == []


def test_long_trailing_dash_is_delisting_not_pending():
    rows = [{"day": f"2024-04-0{i}"} for i in range(1, 7)]
    values = [{"A": 1.0, "B": 1.0}] * 2 + [{"A": 1.0, "B": None}] * 4
    ev.mark_pending(rows, values)
    assert all(r["pending"] == [] for r in rows)          # 表尾 4 天「-」> 3 → 停止申報
    rows = rows[:4]
    ev.mark_pending(rows, values[:4])
    assert [r["pending"] for r in rows] == [[], [], ["B"], ["B"]]
    values = [{"A": 1.0}, {"A": None}, {"A": 2.0}]        # 表中間的「-」之後又有數字：既定缺口
    rows = [{"day": d} for d in ("a", "b", "c")]
    ev.mark_pending(rows, values)
    assert all(r["pending"] == [] for r in rows)


def _ev(day, total, known, pending=(), source="farside", page="p"):
    return ev.event(source=source, page=page, asset="BTC", row={"day": day, "total_usd": total, "funds_usd": {},
                    "pending": list(pending) if pending is not None else None},
                    known_at=known, basis="test", fetched_at=known, evidence="t")


def test_store_appends_only_changes_and_asof_never_looks_ahead(tmp_path):
    path = tmp_path / ev.OBS_NAME
    store = ev.Store(path)
    assert store.add(_ev("2026-10-20", 3.6e6, "2026-10-21T00:36:41+00:00", pending=["IBIT"]))
    assert store.add(_ev("2026-10-20", 33.5e6, "2026-10-21T09:00:00+00:00"))
    assert not store.add(_ev("2026-10-20", 33.5e6, "2026-10-21T12:00:00+00:00"))   # 沒變不記
    assert store.add(_ev("2026-10-20", 30.0e6, "2026-10-22T09:00:00+00:00"))       # 修訂
    assert store.flush() == 3 and len(ev.load(path)) == 3
    day = pd.Timestamp("2026-10-20", tz="UTC")
    assert ev.asof("BTC", "2026-10-21T00:00:00Z", path=path).empty
    assert ev.asof("BTC", "2026-10-21T01:00:00Z", path=path).empty                 # 只有部分加總
    assert ev.asof("BTC", "2026-10-21T01:00:00Z", path=path, complete_only=False)[day] == 3.6e6
    assert ev.asof("BTC", "2026-10-21T09:00:00Z", path=path)[day] == 33.5e6
    assert ev.asof("BTC", "2026-10-23T00:00:00Z", path=path)[day] == 30.0e6
    with pytest.raises(ValueError):
        ev.asof("BTC", "2026-10-23", path=path)
    # 重新載入後去重狀態仍在
    assert not ev.Store(path).add(_ev("2026-10-20", 30.0e6, "2026-10-23T00:00:00+00:00"))


def test_backfill_after_collect_keeps_earliest_and_daily_table_merges_duplicates(tmp_path):
    path = tmp_path / ev.OBS_NAME
    store = ev.Store(path)
    store.add(_ev("2024-08-09", -89.7e6, "2024-08-20T00:00:00+00:00"))             # 較晚的存檔先處理
    assert store.add(_ev("2024-08-09", -89.7e6, "2024-08-12T09:00:32+00:00"))      # 回補到更早的存檔：要記
    store.flush()
    events = ev.load(path)
    table = ev.daily_table("BTC", events=events)
    row = table.iloc[0]
    assert row.first_complete_at == pd.Timestamp("2024-08-12T09:00:32Z") and row.revisions == 0
    # 2024-08-09 美東夏令 16:00 = 20:00 UTC → 延遲 61 小時
    assert row.complete_lag_hours == pytest.approx(61.0, abs=0.01)
    assert ev.known_at("BTC", path=path)["2024-08-09"] == pd.Timestamp("2024-08-12T09:00:32Z")


def test_unknown_pending_is_not_complete(tmp_path):
    path = tmp_path / ev.OBS_NAME
    store = ev.Store(path)
    store.add(_ev("2026-10-07", -484.9e6, "2026-10-09T01:47:22+00:00", pending=None, source="coinglass"))
    store.flush()
    assert ev.asof("BTC", "2026-10-10T00:00:00Z", source="coinglass", path=path).empty
    assert len(ev.asof("BTC", "2026-10-10T00:00:00Z", source="coinglass", path=path, complete_only=False)) == 1
    assert ev.daily_table("BTC", source="coinglass", path=path).iloc[0].revisions == 0


class FakeWayback:
    def __init__(self, pages):
        self.pages = pages          # page -> [(ts, digest, html)]
        self.fetched = []

    def captures(self, page, match, since=None):
        return [{"ts": ts, "original": f"https://{page}", "digest": d} for ts, d, _ in self.pages.get(page, [])]

    def fetch(self, ts, original):
        page = original.removeprefix("https://")
        html = next(h for t, _, h in self.pages[page] if t == ts)
        self.fetched.append(ts)
        if html is None:
            raise ev.WaybackError("http 503")
        return ts, f"https://web.archive.org/web/{ts}id_/{original}", html.encode()


def test_process_wayback_uses_capture_time_skips_same_digest_and_resumes(tmp_path):
    later = NEW_LAYOUT.replace("<td>-</td><td>3.6</td>", "<td>29.9</td><td>3.6</td>").replace(
        "<td>3.6</td></tr>\n<tr><td>Total", "<td>33.5</td></tr>\n<tr><td>Total")
    wb = FakeWayback({"farside.co.uk/btc/": [("20261021003641", "D1", NEW_LAYOUT), ("20261021120000", "D1", NEW_LAYOUT),
                                             ("20261021180000", "D2", later)],
                      "farside.co.uk/?p=1508": [("20240601000000", "HK", "<p>HK ETF</p>")]})
    store, progress = ev.Store(tmp_path / ev.OBS_NAME), {"pages": {}, "digests": {}}
    stats = ev.process_wayback(store, progress, wb, pages={"farside.co.uk/btc/": "exact", "farside.co.uk/?p=1508": "exact"},
                               raw_dir=tmp_path / "raw", max_fetch=10)
    store.flush()
    assert stats["fetched"] == 3 - 1 + 1 and stats["same_digest"] == 1 and stats["no_table"] == 1
    assert (tmp_path / "raw" / "farside_co_uk_btc" / "20261021003641.html.gz").exists()
    events = ev.load(tmp_path / ev.OBS_NAME)
    assert {e["known_at"] for e in events} == {"2026-10-21T00:36:41+00:00", "2026-10-21T18:00:00+00:00"}
    assert all(e["known_at_basis"] == "wayback_capture" and e["fetched_at"] for e in events)
    day = pd.Timestamp("2026-10-20", tz="UTC")
    assert ev.asof("BTC", "2026-10-21T17:00:00Z", path=tmp_path / ev.OBS_NAME).get(day) is None
    assert ev.asof("BTC", "2026-10-21T18:00:00Z", path=tmp_path / ev.OBS_NAME)[day] == 33.5e6
    # 第二輪不重抓
    wb.fetched.clear()
    again = ev.process_wayback(store, progress, wb, pages={"farside.co.uk/btc/": "exact"}, raw_dir=None)
    assert again["todo"] == 0 and wb.fetched == []
    # 「沒有表」的存檔可重跑，讀本機原始檔、不重抓
    retry = ev.process_wayback(store, progress, wb, pages={"farside.co.uk/?p=1508": "exact"}, raw_dir=tmp_path / "raw",
                               retry_no_table=True)
    assert retry["from_raw"] == 1 and retry["fetched"] == 0 and wb.fetched == [] and retry["no_table"] == 1


def test_process_wayback_gives_up_after_three_failures(tmp_path):
    wb = FakeWayback({"farside.co.uk/btc/": [("20240101000000", "X", None)]})
    store, progress = ev.Store(tmp_path / ev.OBS_NAME), {"pages": {}, "digests": {}}
    for _ in range(3):
        stats = ev.process_wayback(store, progress, wb, pages={"farside.co.uk/btc/": "exact"}, raw_dir=None)
    assert stats["failed"] == 1
    assert progress["pages"]["farside.co.uk/btc/"]["skipped"]["20240101000000"].startswith("fetch_failed")
    assert ev.process_wayback(store, progress, wb, pages={"farside.co.uk/btc/": "exact"}, raw_dir=None)["todo"] == 0


class FakeCoinglass:
    def __init__(self, body, observed_at="2026-10-22T02:23:00+00:00"):
        self.body, self.last_result = body, {"observed_at": observed_at}

    def get(self, endpoint, params):
        if "ethereum" in endpoint:
            self.last_result = {"error": "http_404", "http": 404}
            return None
        self.last_result = {"observed_at": "2026-10-22T02:23:00+00:00"}
        return self.body


def test_coinglass_rows_pending_and_store(tmp_path):
    body = {"data": [
        {"timestamp": 1792454400000, "flow_usd": 23.2e6, "etf_flows": [{"etf_ticker": "IBIT", "flow_usd": 29.9e6},
                                                                       {"etf_ticker": "BITB", "flow_usd": -10.3e6},
                                                                       {"etf_ticker": "MSBT"}]},
        {"timestamp": 1792540800000, "flow_usd": 289.8e6, "etf_flows": [{"etf_ticker": "IBIT"},
                                                                        {"etf_ticker": "BITB", "flow_usd": 19.2e6},
                                                                        {"etf_ticker": "MSBT"}]}]}
    rows = ev.coinglass_rows(body)
    assert [r["day"] for r in rows] == ["2026-10-20", "2026-10-21"]
    assert rows[0]["pending"] == [] and rows[1]["pending"] == ["IBIT"]   # MSBT 從沒有數字：不是未公布
    store = ev.Store(tmp_path / ev.OBS_NAME)
    out = ev.process_coinglass(store, FakeCoinglass(body))
    assert out["BTC"]["events"] == 2 and "error" in out["ETH"]
    assert ev.process_coinglass(store, FakeCoinglass(body))["BTC"]["events"] == 0
    store.flush()
    s = ev.asof("BTC", "2026-10-22T03:00:00Z", source="coinglass", path=tmp_path / ev.OBS_NAME)
    assert list(s.index.strftime("%Y-%m-%d")) == ["2026-10-20"]


def test_import_macro_vintages_once(tmp_path):
    log = tmp_path / "macro_vintages.jsonl"
    log.write_text("\n".join(json.dumps(r) for r in [
        {"metric": "etf_flow_usd", "day": "2026-10-07", "value": -277.2e6, "kind": "first_seen",
         "observed_at": "2026-10-08T01:47:00+00:00", "source_collected_at": "2026-10-08T01:40:07+00:00"},
        {"metric": "fear_greed", "day": "2026-10-07", "value": 40, "kind": "first_seen", "observed_at": "2026-10-08T01:47:00+00:00"},
        {"metric": "etf_flow_usd", "day": "2026-10-07", "value": -484.9e6, "kind": "revised",
         "observed_at": "2026-10-09T01:47:22+00:00"}]) + "\n")
    store, progress = ev.Store(tmp_path / ev.OBS_NAME), {}
    assert ev.import_macro_vintages(store, progress, log)["events"] == 2
    assert ev.import_macro_vintages(store, progress, log)["events"] == 0
    store.flush()
    s = ev.asof("BTC", "2026-10-08T12:00:00Z", source="coinglass", complete_only=False, path=tmp_path / ev.OBS_NAME)
    assert s.iloc[0] == -277.2e6


def test_report_lists_monthly_lag(tmp_path):
    path = tmp_path / ev.OBS_NAME
    store = ev.Store(path)
    store.add(_ev("2024-08-09", -89.7e6, "2024-08-12T09:00:32+00:00"))
    store.add(_ev("2024-08-12", 10e6, "2024-08-14T00:00:00+00:00"))
    store.flush()
    text = ev.report_markdown(path)
    # 08-09（五）存檔在 08-12 09:00，晚於 D+3（08-12 00:00）；08-12 在 08-14 已完整
    assert "## farside · BTC" in text and "| 2024-08 | 2 | 2 | 45 | 58 | 50% |" in text
