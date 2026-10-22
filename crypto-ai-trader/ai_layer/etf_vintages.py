"""ETF 資金流 vintage 紀錄：每小時收集＋歷史回補，研究只用「當時已公開」的值（2026-10-22）。

起因：ETF 淨流歷史庫只存最新修訂值；10/21 起研究只認本機記下的時刻（macro_vintages，10/09 才開始），
2024–25 研究窗內 ETF 一律缺值，結論只能寫「無法評估」（reports/vintage_etf_rerun_20261021/REPORT.md）。

來源與 known_at 口徑（一律是「可證明已公開」的時刻上限：只會比真正公布晚、不會早）：
- farside：Internet Archive（Wayback）抓到 Farside 頁面的存檔時刻。BTC 從 2024-01-30、ETH 從 2024-05 起有存檔；
  頁面本身不寫公布時間。存檔稀疏的月份（2025-02～2026-06 每月個位數）known_at 會晚好幾天，這是誠實代價，不補、不內插。
  Farside 直連有 Cloudflare 驗證頁，本模組不繞過，只讀 Internet Archive 已公開的存檔。
- coinglass：本機呼叫 CoinGlass 中轉的那一刻（每小時）；另把 macro_vintages 10/09 起每日 01:40 UTC 的紀錄併進來
  （那批只有總額、不知道哪些基金未公布，complete_only 不採用）。

紀錄只追加（state/etf_vintages/observations.jsonl）：同一來源頁、同一觀測日，內容和時間上前一筆不同才記一筆。
「-」＝未公布或休市，不當 0；整列都是「-」的休市日不記。某檔基金在同表較早日期有數字、這一天卻是「-」→ 記進 pending
（當日總額只是部分加總）。研究用 asof(..., complete_only=True) 只取當時已完整的值；kind 欄以寫入當下為準，
回補可能晚於收集寫入，研究一律以 known_at 排序重算（daily_table 已處理）。

只顯示／研究，不接交易、不改 macro 管線。
    .venv/bin/python -m ai_layer.etf_vintages collect               # 每小時（launchd com.cryptoai.etf-vintage）
    .venv/bin/python -m ai_layer.etf_vintages backfill --max 300    # Wayback 歷史回補（可續跑，同一把鎖）
    .venv/bin/python -m ai_layer.etf_vintages status | report | asof BTC 2025-03-01T00:00:00Z
"""
from __future__ import annotations

import argparse
import bisect
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import fcntl
import gzip
import json
import math
from pathlib import Path
import re
import sys
import time
from typing import Any, Callable, Iterable
from zoneinfo import ZoneInfo

import pandas as pd
import requests

from ai_layer.common import ROOT, heartbeat, read_json, write_json

STORE_DIR = ROOT / "state" / "etf_vintages"
OBS_NAME = "observations.jsonl"
RAW_DIR = ROOT / "user_data" / "data_ext" / "etf_vintages" / "raw"
CDX_URL = "https://web.archive.org/cdx/search/cdx"
WAYBACK_URL = "https://web.archive.org/web"
USER_AGENT = "crypto-ai-trader etf_vintages/1.0 (research; reads public Internet Archive captures)"
FETCH_SPACING_S = 2.0
NY = ZoneInfo("America/New_York")

# Farside 頁面（Wayback 網址 → CDX 比對方式）。資產看表頭代號判斷，不靠網址；香港 ETF 頁（?p=1508）沒有美國代號會被略過。
# 只用 exact：CDX 的 prefix 查詢實測回 503（2026-10-22）；帶 ?ref= 等參數的變體存檔很少，不收。
PAGES = {
    "farside.co.uk/?p=997": "exact",                       # 2024-01～08 BTC（舊網址，近期視窗）
    "farside.co.uk/?p=1321": "exact",                      # 2024-03～07 BTC All Data
    "farside.co.uk/bitcoin-etf-flow-all-data/": "exact",   # 2024-08～ BTC 全歷史
    "farside.co.uk/btc/": "exact",                         # 2024-08～ BTC 近期視窗
    "farside.co.uk/?p=1518": "exact",                      # 2024-05～ ETH（舊網址）
    "farside.co.uk/eth/": "exact",
    "farside.co.uk/ethereum-etf-flow-all-data/": "exact",
    "farside.co.uk/sol/": "exact",
}
ASSET_TICKERS = {
    "BTC": {"IBIT", "FBTC", "GBTC", "ARKB", "BITB", "HODL", "BRRR", "EZBC", "BTCO", "BTCW"},
    "ETH": {"ETHA", "ETHE", "FETH", "ETHW", "CETH", "ETHV", "QETH", "EZET"},
    "SOL": {"BSOL", "GSOL", "VSOL", "FSOL", "SOEZ", "TSOL", "SOLC"},
}
COINGLASS_ENDPOINTS = {"BTC": "/api/etf/bitcoin/flow-history", "ETH": "/api/etf/ethereum/flow-history"}
# 2024-01～02 的頁面寫「11/01/2024」（英式日／月）、整數百萬、負數用減號、未公布留白；之後改「11 Jan 2024」、括號負數、「-」
DATE_FORMATS = ("%d %b %Y", "%d/%m/%Y", "%d %B %Y")
TICKER_RE = re.compile(r"^[A-Z][A-Z0-9]{1,5}$")
REVISION_TOLERANCE_USD = 50_000.0   # Farside 以 0.1 百萬美元四捨五入
MAX_PENDING_RUN = 3                 # 表尾連續「-」超過 3 個交易日視為停止申報（下市／轉換），不再算未公布


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat(timespec="seconds")


def _ns(value) -> int:
    ts = pd.Timestamp(value)
    return (ts.tz_localize("UTC") if ts.tzinfo is None else ts.tz_convert("UTC")).value


def wayback_time(ts: str) -> datetime:
    return datetime.strptime(ts[:14], "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)


def parse_number(text: Any) -> float | None:
    """Farside 儲存格（百萬美元）：「(26.2)」負數、「1,119.9」千分位；「-」／空白＝未公布，回傳 None（不是 0）。"""
    t = str(text if text is not None else "").strip().replace(",", "").replace("\xa0", "").replace("$", "")
    if t in ("", "-", "–", "—", "n/a", "N/A"):
        return None
    negative = t.startswith("(") and t.endswith(")")
    if negative:
        t = t[1:-1].strip()
    try:
        value = float(t)
    except ValueError:
        return None
    if not math.isfinite(value):
        return None
    return -value if negative else value


def number(value: Any) -> float | None:
    if isinstance(value, bool) or value is None:
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


def parse_day(text: str):
    t = " ".join(str(text).split())
    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(t, fmt).date()
        except ValueError:
            continue
    return None


def detect_asset(cells: Iterable[str]) -> str | None:
    found = {c.strip() for c in cells}
    hits = {asset: len(found & tickers) for asset, tickers in ASSET_TICKERS.items()}
    asset, n = max(hits.items(), key=lambda kv: kv[1])
    return asset if n >= 2 else None


def _musd(value: float | None) -> float | None:
    return None if value is None else round(value * 1e6, 2)


def mark_pending(rows: list[dict], values: list[dict]) -> None:
    """pending＝表尾那段「-」：基金在較早日期有數字，之後到表尾都是「-」，且這段不超過 MAX_PENDING_RUN 個交易日。

    尚未上市的基金在上市前是「-」（之前沒有數字）不算；表中間的「-」（之後又有數字）是既定缺口不算；
    表尾很長的「-」是停止申報。rows 與 values 一一對應、日期由舊到新。
    """
    for row in rows:
        row["pending"] = []
    for tk in set().union(*values) if values else ():
        numeric = [i for i, v in enumerate(values) if v.get(tk) is not None]
        if numeric and 0 < len(rows) - 1 - numeric[-1] <= MAX_PENDING_RUN:
            for i in range(numeric[-1] + 1, len(rows)):
                rows[i]["pending"].append(tk)
    for row in rows:
        row["pending"].sort()


def parse_farside(html: str) -> list[dict]:
    """Farside 資金流表 → [{asset, tickers, rows: [{day, total_usd, funds_usd, pending}]}]（日期由舊到新）。

    Total／Average／Maximum／Minimum 等非日期列略過；整列基金都是「-」（休市）不記；pending 規則見 mark_pending。
    """
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html, "lxml")
    out = []
    for table in soup.find_all("table"):
        rows = [[c.get_text(" ", strip=True) for c in tr.find_all(["td", "th"])] for tr in table.find_all("tr")]
        header_i = next((i for i, cells in enumerate(rows)
                         if sum(bool(TICKER_RE.match(c)) for c in cells[1:]) >= 3), None)
        if header_i is None:
            continue
        header = rows[header_i]
        asset = detect_asset(header)
        if asset is None:
            continue
        total_col = None
        for cells in rows[max(0, header_i - 1): header_i + 1]:
            for j, c in enumerate(cells):
                if c.strip().lower() == "total":
                    total_col = j
        if total_col is None and header and not header[-1].strip():
            total_col = len(header) - 1
        fund_cols = {j: c.strip() for j, c in enumerate(header)
                     if j > 0 and j != total_col and TICKER_RE.match(c.strip())}
        parsed, values_by_row = [], []
        for cells in rows[header_i + 1:]:
            day = parse_day(cells[0]) if cells else None
            if day is None or len(cells) < len(header):
                continue
            values = {tk: parse_number(cells[j]) for j, tk in fund_cols.items()}
            if all(v is None for v in values.values()):
                continue
            total = parse_number(cells[total_col]) if total_col is not None and total_col < len(cells) else None
            parsed.append({"day": day.isoformat(), "total_usd": _musd(total),
                           "funds_usd": {tk: _musd(v) for tk, v in values.items() if v is not None}})
            values_by_row.append(values)
        mark_pending(parsed, values_by_row)
        if parsed:
            out.append({"asset": asset, "tickers": list(fund_cols.values()), "rows": parsed})
    return out


def _decode(body: bytes) -> str:
    if body[:2] == b"\x1f\x8b":
        body = gzip.decompress(body)
    return body.decode("utf-8", "replace")


# ---------------------------------------------------------------- store

def load(path: Path | None = None) -> list[dict]:
    path = Path(path) if path is not None else STORE_DIR / OBS_NAME
    if not path.exists():
        return []
    rows = []
    for line in path.read_text(encoding="utf-8").splitlines():
        try:
            rows.append(json.loads(line))
        except ValueError:
            continue
    return rows


def _state(ev: dict) -> tuple:
    pending = ev.get("pending")
    return (ev.get("total_usd"), tuple(sorted((ev.get("funds_usd") or {}).items())),
            None if pending is None else tuple(sorted(pending)))


def _complete(ev: dict) -> bool:
    """總額有數字、且明確知道沒有未公布的基金（pending 是空列表；None＝不知道，不算完整）。"""
    return ev.get("total_usd") is not None and ev.get("pending") == []


class Store:
    """observations.jsonl 的只追加寫入；add() 只在和時間上前一筆（known_at ≤ 本筆）內容不同時記。"""

    def __init__(self, path: Path | None = None):
        self.path = Path(path) if path is not None else STORE_DIR / OBS_NAME
        self._times: dict[tuple, list[int]] = {}
        self._states: dict[tuple, list[tuple]] = {}
        self.pending: list[dict] = []
        for ev in load(self.path):
            self._index(ev)

    @staticmethod
    def key(ev: dict) -> tuple:
        return (ev["source"], ev["page"], ev["asset"], ev["day"])

    def _index(self, ev: dict) -> None:
        key, t = self.key(ev), _ns(ev["known_at"])
        times, states = self._times.setdefault(key, []), self._states.setdefault(key, [])
        i = bisect.bisect_right(times, t)
        times.insert(i, t)
        states.insert(i, _state(ev))

    def add(self, ev: dict) -> bool:
        key, t = self.key(ev), _ns(ev["known_at"])
        times = self._times.get(key, [])
        i = bisect.bisect_right(times, t) - 1
        if i >= 0 and self._states[key][i] == _state(ev):
            return False
        ev = {**ev, "kind": "first_seen" if i < 0 else "changed"}
        self._index(ev)
        self.pending.append(ev)
        return True

    def flush(self) -> int:
        if not self.pending:
            return 0
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open("a", encoding="utf-8") as f:
            f.write("".join(json.dumps(ev, ensure_ascii=False, sort_keys=True) + "\n" for ev in self.pending))
        n, self.pending = len(self.pending), []
        return n


def event(*, source: str, page: str, asset: str, row: dict, known_at: str, basis: str, fetched_at: str,
          evidence: str) -> dict:
    return {"source": source, "page": page, "asset": asset, "day": row["day"], "total_usd": row.get("total_usd"),
            "funds_usd": row.get("funds_usd") or {}, "pending": row.get("pending"), "known_at": known_at,
            "known_at_basis": basis, "fetched_at": fetched_at, "evidence": evidence}


@contextmanager
def store_lock(blocking: bool):
    STORE_DIR.mkdir(parents=True, exist_ok=True)
    with (STORE_DIR / "collect.lock").open("a", encoding="utf-8") as f:
        try:
            fcntl.flock(f, fcntl.LOCK_EX | (0 if blocking else fcntl.LOCK_NB))
        except BlockingIOError:
            yield False
            return
        try:
            yield True
        finally:
            fcntl.flock(f, fcntl.LOCK_UN)


def _progress_path() -> Path:
    return STORE_DIR / "progress.json"


def load_progress() -> dict:
    data = read_json(_progress_path(), {})
    data.setdefault("pages", {})
    data.setdefault("digests", {})
    return data


# ---------------------------------------------------------------- Wayback

class WaybackError(RuntimeError):
    pass


class Wayback:
    """Internet Archive 的 CDX 清單與原始存檔（id_）讀取；每次請求間隔 FETCH_SPACING_S 秒，429／5xx 退避重試。"""

    def __init__(self, session: Any = None, sleeper: Callable[[float], None] = time.sleep,
                 clock: Callable[[], float] = time.monotonic, spacing: float = FETCH_SPACING_S):
        self.session = session if session is not None else requests.Session()
        self.session.headers.update({"User-Agent": USER_AGENT})
        self._sleep, self._clock, self.spacing = sleeper, clock, spacing
        self._last = None

    def _get(self, url: str, params: dict | None = None, timeout: float = 90.0, attempts: int = 4):
        delay = 15.0
        for attempt in range(attempts):
            if self._last is not None:
                wait = self.spacing - (self._clock() - self._last)
                if wait > 0:
                    self._sleep(wait)
            self._last = self._clock()
            try:
                r = self.session.get(url, params=params, timeout=timeout)
            except requests.RequestException as e:
                err = f"{type(e).__name__}"
            else:
                if r.status_code == 200:
                    return r
                err = f"http {r.status_code}"
                if r.status_code not in (429, 500, 502, 503, 504):
                    raise WaybackError(err)
                retry = number(r.headers.get("Retry-After"))
                if retry:
                    delay = max(delay, min(retry, 300.0))
            if attempt < attempts - 1:
                self._sleep(delay)
                delay *= 2
        raise WaybackError(err)

    def captures(self, page: str, match: str, since: str | None = None) -> list[dict]:
        params = {"url": page, "matchType": match, "output": "json", "fl": "timestamp,original,digest,mimetype",
                  "filter": "statuscode:200"}
        if since:
            params["from"] = since
        try:
            rows = self._get(CDX_URL, params, attempts=2).json()
        except ValueError:      # 沒有存檔時 CDX 回空字串
            rows = []
        out = []
        for r in rows[1:] if rows else []:
            ts, original, digest, mime = (r + [None] * 4)[:4]
            if mime and "html" not in str(mime):
                continue
            out.append({"ts": ts, "original": original, "digest": digest})
        return out

    def fetch(self, ts: str, original: str) -> tuple[str, str, bytes]:
        """回傳（實際存檔時刻 14 碼、最終網址、內容）。Wayback 若轉到最近的存檔，以實際存檔時刻為準。"""
        r = self._get(f"{WAYBACK_URL}/{ts}id_/{original}")
        m = re.search(r"/web/(\d{14})id_/", r.url)
        return (m.group(1) if m else ts), r.url, r.content


def _slug(page: str) -> str:
    return re.sub(r"[^A-Za-z0-9]+", "_", page).strip("_")


def process_wayback(store: Store, progress: dict, wb: Wayback, *, pages: dict | None = None, max_fetch: int = 30,
                    since_days: int | None = None, now: datetime | None = None, raw_dir: Path | None = RAW_DIR,
                    retry_no_table: bool = False, on_checkpoint: Callable[[], None] = lambda: None, log: Callable[[str], None] = print) -> dict:
    """處理還沒看過的存檔（由舊到新）。同內容（digest）且更早的存檔已處理過就略過；原始檔已在 raw_dir 就不重抓。

    retry_no_table：把先前「沒有 ETF 表」的存檔放回待處理（解析器支援新版面後重跑，讀本機原始檔、不重抓）。
    """
    pages = PAGES if pages is None else pages
    now = now or utc_now()
    since = (now - timedelta(days=since_days)).strftime("%Y%m%d") if since_days else None
    todo, errors = [], {}
    for page, match in pages.items():
        info = progress["pages"].setdefault(page, {"done": [], "skipped": {}})
        if retry_no_table:
            info["skipped"] = {ts: why for ts, why in info["skipped"].items() if why != "no_etf_table"}
        try:
            caps = wb.captures(page, match, since)
        except WaybackError as e:
            errors[page] = f"cdx: {e}"
            log(f"[etf_vintages] CDX {page} 失敗：{e}")
            continue
        done = set(info["done"]) | set(info["skipped"])
        fresh = [(c["ts"], page, c) for c in caps if c["ts"] not in done]
        todo += fresh
        log(f"[etf_vintages] CDX {page}：{len(caps)} 筆存檔，待處理 {len(fresh)}")
        info["last_cdx"] = _iso(now)
        if not since:
            info["captures_listed"] = len(caps)
    todo.sort(key=lambda x: (x[0], x[1]))
    stats = {"todo": len(todo), "fetched": 0, "from_raw": 0, "same_digest": 0, "no_table": 0, "failed": 0, "events": 0}
    failures_in_row = 0
    for ts, page, cap in todo:
        if stats["fetched"] >= max_fetch:
            break
        info = progress["pages"][page]
        digest = cap.get("digest")
        earliest = progress["digests"].get(digest) if digest else None
        if earliest and earliest <= ts:
            info["skipped"][ts] = "same_digest"
            stats["same_digest"] += 1
            continue
        cached = Path(raw_dir) / _slug(page) / f"{ts}.html.gz" if raw_dir is not None else None
        if cached is not None and cached.exists():
            actual, final_url, body = ts, f"{WAYBACK_URL}/{ts}id_/{cap['original']}", cached.read_bytes()
            fetched_at = _iso(datetime.fromtimestamp(cached.stat().st_mtime, timezone.utc))   # 當初抓下來的時刻
            stats["from_raw"] += 1
        else:
            try:
                actual, final_url, body = wb.fetch(ts, cap["original"])
            except WaybackError as e:
                stats["failed"] += 1
                failures_in_row += 1
                tries = info.setdefault("failures", {})
                tries[ts] = tries.get(ts, 0) + 1
                if tries[ts] >= 3:
                    info["skipped"][ts] = f"fetch_failed: {e}"
                if failures_in_row >= 3:
                    errors["fetch"] = f"連續 {failures_in_row} 次失敗（{e}），本輪停止"
                    break
                continue
            failures_in_row = 0
            stats["fetched"] += 1
            fetched_at = _iso(utc_now())
        html = _decode(body)
        if raw_dir is not None:
            dest = Path(raw_dir) / _slug(page) / f"{actual}.html.gz"
            if not dest.exists():
                dest.parent.mkdir(parents=True, exist_ok=True)
                tmp = dest.with_suffix(".tmp")
                tmp.write_bytes(gzip.compress(html.encode("utf-8")))
                tmp.replace(dest)
        tables = parse_farside(html)
        known = wayback_time(actual)
        if not tables:
            info["skipped"][ts] = "no_etf_table"
            stats["no_table"] += 1
        else:
            for table in tables:
                for row in table["rows"]:
                    if row["day"] > (known + timedelta(days=1)).date().isoformat():
                        continue    # 未來日期＝解析錯誤，不記
                    if store.add(event(source="farside", page=page, asset=table["asset"], row=row,
                                       known_at=_iso(known), basis="wayback_capture", fetched_at=fetched_at,
                                       evidence=final_url)):
                        stats["events"] += 1
            info["done"].append(ts)
            if digest and (earliest is None or ts < earliest):   # 只記有表的內容；沒表的可用 retry_no_table 重跑
                progress["digests"][digest] = ts
        if (stats["fetched"] + stats["from_raw"]) % 10 == 0:
            on_checkpoint()
            log(f"[etf_vintages] wayback 已處理 {stats['fetched']} 筆存檔，新增 {stats['events']} 筆紀錄")
    stats["remaining"] = sum(1 for ts, page, _ in todo
                             if ts not in progress["pages"][page]["skipped"] and ts not in set(progress["pages"][page]["done"]))
    if errors:
        stats["errors"] = errors
    return stats


# ---------------------------------------------------------------- CoinGlass

def coinglass_rows(body: dict) -> list[dict]:
    """CoinGlass ETF flow-history → 與 parse_farside 相同的列格式（缺 flow_usd＝「-」，pending 規則同 mark_pending）。"""
    data = body.get("data") if isinstance(body, dict) else None
    if not isinstance(data, list):
        return []
    rows, values_by_row, items = [], [], []
    for item in data:
        if not isinstance(item, dict):
            continue
        ts = number(item.get("timestamp"))
        if ts is None:
            continue
        items.append((datetime.fromtimestamp(ts / 1000 if ts >= 1e12 else ts, timezone.utc).date().isoformat(), item))
    for day, item in sorted(items, key=lambda x: x[0]):
        values = {str(f["etf_ticker"]): number(f.get("flow_usd"))
                  for f in item.get("etf_flows") or [] if isinstance(f, dict) and f.get("etf_ticker")}
        total = number(item.get("flow_usd"))
        if total is None and all(v is None for v in values.values()):
            continue
        rows.append({"day": day, "total_usd": total, "funds_usd": {tk: v for tk, v in values.items() if v is not None}})
        values_by_row.append(values)
    mark_pending(rows, values_by_row)
    return rows


def process_coinglass(store: Store, client: Any = None) -> dict:
    out = {}
    if client is None:
        from ai_layer.coinglass import CoinglassClient
        client = CoinglassClient(transient_retries=1)
    for asset, endpoint in COINGLASS_ENDPOINTS.items():
        body = client.get(endpoint, {})
        result = getattr(client, "last_result", {}) or {}
        if body is None:
            out[asset] = {"error": result.get("error") or "request_failed", "http": result.get("http")}
            continue
        fetched = result.get("observed_at") or _iso(utc_now())
        n = 0
        rows = coinglass_rows(body)
        for row in rows:
            if store.add(event(source="coinglass", page=endpoint, asset=asset, row=row, known_at=fetched,
                               basis="local_fetch", fetched_at=fetched, evidence=f"coinglass {endpoint} @ {fetched}")):
                n += 1
        out[asset] = {"rows": len(rows), "events": n, "fetched_at": fetched}
    return out


def import_macro_vintages(store: Store, progress: dict, path: Path | None = None) -> dict:
    """把 macro_vintages（10/09 起每日 01:40 UTC 的 CoinGlass BTC 總額）併進來；不知道 pending，complete_only 不採用。"""
    from ai_layer import macro_vintages

    path = Path(path) if path is not None else macro_vintages.DEFAULT_LOG
    rows = macro_vintages.load(path)
    start = int(progress.get("macro_vintages_rows", 0))
    if start > len(rows):
        start = 0
    n = 0
    for row in rows[start:]:
        if row.get("metric") != "etf_flow_usd" or row.get("kind") not in ("first_seen", "revised"):
            continue
        value, seen = number(row.get("value")), row.get("observed_at")
        if value is None or not seen:
            continue
        if store.add(event(source="coinglass", page="macro_daily", asset="BTC",
                           row={"day": row["day"], "total_usd": value, "funds_usd": {}, "pending": None},
                           known_at=_iso(pd.Timestamp(seen).to_pydatetime()), basis="local_fetch",
                           fetched_at=row.get("source_collected_at") or seen, evidence="state/macro_vintages.jsonl")):
            n += 1
    progress["macro_vintages_rows"] = len(rows)
    return {"rows": len(rows) - start, "events": n}


# ---------------------------------------------------------------- research reads

def _select(events: list[dict], *, source: str, asset: str) -> list[dict]:
    asset = asset.upper()
    return [e for e in events if e.get("asset") == asset and (source == "any" or e.get("source") == source)]


def asof(asset: str, at, *, source: str = "farside", complete_only: bool = True, field: str = "total_usd",
         path: Path | None = None, events: list[dict] | None = None) -> pd.Series:
    """at 之前已公開的各觀測日值（每天取 known_at 最晚的一筆）。

    complete_only=True：只用當時已完整（沒有未公布基金）的紀錄；部分加總不拿來用。
    source：farside／coinglass／any（any＝兩來源中 known_at 最晚的那筆）。紀錄前沒有資料的時段回傳空，不補值。
    """
    at = pd.Timestamp(at)
    if at.tzinfo is None:
        raise ValueError("at must be timezone-aware")
    limit = at.tz_convert("UTC").value
    best: dict[str, tuple[int, int, float]] = {}
    evs = events if events is not None else load(path)
    for i, ev in enumerate(_select(evs, source=source, asset=asset)):
        t = _ns(ev["known_at"])
        if t > limit or (complete_only and not _complete(ev)):
            continue
        value = ev.get(field)
        if value is None:
            continue
        cur = best.get(ev["day"])
        if cur is None or (t, i) >= cur[:2]:
            best[ev["day"]] = (t, i, float(value))
    series = pd.Series({day: v[2] for day, v in best.items()}, dtype=float)
    series.index = pd.to_datetime(series.index, utc=True)
    return series.sort_index()


def known_at(asset: str, *, source: str = "farside", path: Path | None = None) -> dict[str, pd.Timestamp]:
    """每個觀測日「最終值」第一次完整公開的時刻（與 macro_vintages.known_at 同形，給之後 macro 管線換來源用）。"""
    table = daily_table(asset, source=source, path=path)
    return {} if table.empty else {r.day: r.final_known_at for r in table.itertuples() if pd.notna(r.final_known_at)}


def nyse_close_utc(day: str) -> pd.Timestamp:
    local = datetime.strptime(day, "%Y-%m-%d").replace(hour=16, tzinfo=NY)
    return pd.Timestamp(local).tz_convert("UTC")


def daily_table(asset: str, *, source: str = "farside", path: Path | None = None,
                events: list[dict] | None = None) -> pd.DataFrame:
    """每個觀測日一列：第一次看到、第一次完整、最終值與其首次公開時刻、完整後修訂次數、距美股收盤的延遲（小時）。"""
    evs = events if events is not None else load(path)
    by_day: dict[str, list[dict]] = {}
    for ev in _select(evs, source=source, asset=asset):
        by_day.setdefault(ev["day"], []).append(ev)
    rows = []
    for day, items in sorted(by_day.items()):
        items.sort(key=lambda e: _ns(e["known_at"]))
        first = items[0]
        complete = [e for e in items if _complete(e)]
        # 依時間走一遍完整值，連續相同的值合併；值變了才算修訂
        timeline: list[tuple[int, float]] = []
        for e in complete:
            t, v = _ns(e["known_at"]), float(e["total_usd"])
            if not timeline or abs(timeline[-1][1] - v) > REVISION_TOLERANCE_USD:
                timeline.append((t, v))
        close = nyse_close_utc(day)
        first_complete = pd.Timestamp(timeline[0][0], tz="UTC") if timeline else pd.NaT
        final_known = pd.Timestamp(timeline[-1][0], tz="UTC") if timeline else pd.NaT
        rows.append({
            "day": day, "first_seen_at": pd.Timestamp(_ns(first["known_at"]), tz="UTC"),
            "first_seen_value_usd": first.get("total_usd"),
            "first_complete_at": first_complete, "complete_value_usd": timeline[0][1] if timeline else None,
            "final_value_usd": timeline[-1][1] if timeline else None, "final_known_at": final_known,
            "revisions": max(0, len(timeline) - 1),
            "max_abs_revision_usd": max((abs(v - timeline[0][1]) for _, v in timeline), default=0.0) if timeline else None,
            "complete_lag_hours": (first_complete - close).total_seconds() / 3600 if timeline else None,
            "n_events": len(items), "pages": ",".join(sorted({e["page"] for e in items})),
        })
    return pd.DataFrame(rows)


# ---------------------------------------------------------------- status / report

def status(path: Path | None = None) -> dict:
    evs = load(path)
    progress = load_progress()
    out: dict[str, Any] = {"store": str(Path(path) if path else STORE_DIR / OBS_NAME), "events": len(evs), "sources": {}}
    for source in sorted({e["source"] for e in evs}):
        for asset in sorted({e["asset"] for e in evs if e["source"] == source}):
            table = daily_table(asset, source=source, events=evs)
            done = table.dropna(subset=["first_complete_at"])
            lag = done["complete_lag_hours"]
            out["sources"].setdefault(source, {})[asset] = {
                "days": len(table), "complete_days": len(done),
                "first_day": table["day"].min() if len(table) else None, "last_day": table["day"].max() if len(table) else None,
                "median_complete_lag_hours": round(float(lag.median()), 1) if len(lag) else None,
                "days_with_revisions": int((done["revisions"] > 0).sum()) if len(done) else 0,
            }
    out["wayback"] = {page: {"done": len(info.get("done", [])), "skipped": len(info.get("skipped", {})),
                             "last_cdx": info.get("last_cdx")} for page, info in progress["pages"].items()}
    out["last_run"] = read_json(STORE_DIR / "last_run.json", {})
    return out


def _fmt_h(value) -> str:
    return "—" if value is None or pd.isna(value) else f"{value:.0f}"


def report_markdown(path: Path | None = None, now: datetime | None = None, lag_days: int = 3) -> str:
    """覆蓋率與公布延遲：每月有完整值的交易日、延遲中位數／P90、在「觀測日＋lag_days」前已完整的比例。"""
    now = now or utc_now()
    evs = load(path)
    lines = [f"# ETF 資金流 vintage 覆蓋率（{now:%Y-%m-%d %H:%M} UTC，程式產生）", "",
             "來源與口徑見 `ai_layer/etf_vintages.py` 開頭與 `docs/ETF資金流vintage.md`。延遲＝第一次看到完整值的時刻 − 美股收盤（16:00 紐約）。",
             f"「≤D+{lag_days}」＝在觀測日 UTC 零點＋{lag_days} 天之前已完整（macro.yaml 目前假設 ETF 觀測日＋{lag_days} 天可得）。",
             "存檔時刻只是公開的上限：延遲大多反映存檔頻率，不是 Farside 真的晚公布。", ""]
    header_len = len(lines)
    for source in ("farside", "coinglass"):
        for asset in ("BTC", "ETH", "SOL"):
            table = daily_table(asset, source=source, events=evs)
            if table.empty:
                continue
            table["month"] = table["day"].str[:7]
            table["cutoff"] = pd.to_datetime(table["day"], utc=True) + pd.Timedelta(days=lag_days)
            table["within"] = table["first_complete_at"].notna() & (table["first_complete_at"] <= table["cutoff"])
            lines += [f"## {source} · {asset}", "",
                      f"觀測日 {len(table)} 天（{table['day'].min()} ～ {table['day'].max()}），有完整值 {int(table['first_complete_at'].notna().sum())} 天，"
                      f"完整後有修訂 {int((table['revisions'] > 0).sum())} 天。", "",
                      f"| 月份 | 觀測日 | 有完整值 | 延遲中位數（時） | 延遲 P90（時） | ≤D+{lag_days} |", "|---|---:|---:|---:|---:|---:|"]
            for month, g in table.groupby("month"):
                lag = g["complete_lag_hours"].dropna()
                lines.append(f"| {month} | {len(g)} | {int(g['first_complete_at'].notna().sum())} | "
                             f"{_fmt_h(lag.median() if len(lag) else None)} | {_fmt_h(lag.quantile(0.9) if len(lag) else None)} | "
                             f"{g['within'].mean():.0%} |")
            lines.append("")
    if len(lines) == header_len:
        lines.append("（尚無紀錄）")
    return "\n".join(lines) + "\n"


def write_daily_tables(events: list[dict]) -> dict:
    out = {}
    for source in sorted({e["source"] for e in events}):
        for asset in sorted({e["asset"] for e in events if e["source"] == source}):
            table = daily_table(asset, source=source, events=events)
            dest = STORE_DIR / f"daily_{asset}_{source}.csv"
            tmp = dest.with_suffix(".tmp")
            table.to_csv(tmp, index=False)
            tmp.replace(dest)
            out[f"{source}:{asset}"] = len(table)
    return out


# ---------------------------------------------------------------- CLI

def run(mode: str, *, max_fetch: int, since_days: int | None, coinglass: bool, retry_no_table: bool = False,
        log=print) -> int:
    started = utc_now()
    with store_lock(blocking=(mode == "backfill")) as got:
        if not got:
            log("[etf_vintages] 另一個收集／回補正在跑，本輪略過")
            return 0
        store, progress = Store(), load_progress()

        def checkpoint():
            store.flush()
            write_json(_progress_path(), progress)

        summary: dict[str, Any] = {"mode": mode, "started_at": _iso(started)}
        errors = {}
        try:
            summary["wayback"] = process_wayback(store, progress, Wayback(), max_fetch=max_fetch, since_days=since_days,
                                                 retry_no_table=retry_no_table, on_checkpoint=checkpoint, log=log)
            if summary["wayback"].get("errors"):
                errors["wayback"] = summary["wayback"]["errors"]
        except Exception as e:  # 一個來源壞掉不影響其他來源
            errors["wayback"] = f"{type(e).__name__}: {e}"
        if coinglass:
            try:
                summary["coinglass"] = process_coinglass(store)
                bad = {a: r for a, r in summary["coinglass"].items() if "error" in r}
                if bad:
                    errors["coinglass"] = bad
            except Exception as e:
                errors["coinglass"] = f"{type(e).__name__}: {e}"
            try:
                summary["macro_vintages"] = import_macro_vintages(store, progress)
            except Exception as e:
                errors["macro_vintages"] = f"{type(e).__name__}: {e}"
        checkpoint()
        events = load()
        summary["daily_tables"] = write_daily_tables(events)
        summary["events_total"] = len(events)
        summary["finished_at"] = _iso(utc_now())
        if errors:
            summary["errors"] = errors
        write_json(STORE_DIR / "last_run.json", summary)
    wb = summary.get("wayback") or {}
    beat = {"mode": mode, "events": summary["events_total"], "wayback_fetched": wb.get("fetched"),
            "wayback_remaining": wb.get("remaining")}
    if errors and not (wb.get("fetched") or any("error" not in r for r in (summary.get("coinglass") or {}).values())):
        beat["error"] = "; ".join(errors)
    heartbeat("etf-vintage", beat)
    log(json.dumps(summary, ensure_ascii=False, default=str))
    return 1 if "error" in beat else 0


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)
    c = sub.add_parser("collect", help="每小時：近 45 天新存檔＋CoinGlass＋macro_vintages")
    c.add_argument("--max", type=int, default=30)
    c.add_argument("--no-coinglass", action="store_true")
    b = sub.add_parser("backfill", help="Wayback 全歷史回補（由舊到新，可續跑）")
    b.add_argument("--max", type=int, default=300)
    b.add_argument("--retry-no-table", action="store_true", help="先前沒解析出表的存檔重跑（讀本機原始檔）")
    sub.add_parser("status")
    r = sub.add_parser("report")
    r.add_argument("--out", help="寫到檔案（預設印出）")
    a = sub.add_parser("asof")
    a.add_argument("asset")
    a.add_argument("at")
    a.add_argument("--source", default="farside")
    a.add_argument("--partial", action="store_true", help="也用部分加總")
    args = parser.parse_args(argv)
    if args.command == "collect":
        return run("collect", max_fetch=args.max, since_days=45, coinglass=not args.no_coinglass)
    if args.command == "backfill":
        return run("backfill", max_fetch=args.max, since_days=None, coinglass=False, retry_no_table=args.retry_no_table)
    if args.command == "status":
        print(json.dumps(status(), ensure_ascii=False, indent=2, default=str))
        return 0
    if args.command == "report":
        text = report_markdown()
        if args.out:
            Path(args.out).write_text(text, encoding="utf-8")
            print(args.out)
        else:
            print(text)
        return 0
    series = asof(args.asset, args.at, source=args.source, complete_only=not args.partial)
    print(series.tail(30).to_string())
    return 0


if __name__ == "__main__":
    sys.exit(main())
