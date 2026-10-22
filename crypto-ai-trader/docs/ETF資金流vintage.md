# ETF 資金流 vintage 紀錄（2026-10-22 起；canonical）

程式 `ai_layer/etf_vintages.py`，排程 `com.cryptoai.etf-vintage`（每小時 :23），測試 `tests/test_etf_vintages.py`。
只顯示／研究，不接交易、不改 macro 管線、不碰任何 Freqtrade 實例。

## 為什麼

ETF 淨流歷史庫（`state/macro_*.parquet`）只存 CoinGlass 的最新修訂值。2026-10-21 起研究只認本機記下的時刻（`macro_vintages`），
那份紀錄 10/09 才開始，而且每天只在 01:40 UTC 抓一次，所以 2024–25 研究窗內 ETF 一律缺值，
`reports/vintage_etf_rerun_20261021/REPORT.md` 的結論只能寫「ETF 無法評估」。這裡補兩件事：

1. **往後**：每小時記下每個觀測日的數值、這個值「可證明已公開」的時刻（known_at）與本機抓取時刻（fetched_at）。
2. **往前**：從 Internet Archive（Wayback Machine）已公開的 Farside 頁面存檔回補 2024-01 起的歷史，時刻用存檔時刻。

## 來源與時刻口徑

| source | 內容 | known_at（研究用） | 起點 |
|---|---|---|---|
| `farside` | Wayback 存的 Farside 表（BTC、ETH、SOL；每檔基金＋總額，百萬美元換成美元） | 存檔時刻（`known_at_basis: wayback_capture`） | 觀測日 BTC 2024-01-11、ETH 2024-07-23、SOL 2025-10-28（最早存檔 BTC 2024-01-30，已含上市以來的表） |
| `coinglass` | 本機每小時呼叫 CoinGlass 中轉 `/api/etf/{bitcoin,ethereum}/flow-history`（逐檔＋總額） | 本機抓取時刻（`local_fetch`） | 2026-10-22 |
| `coinglass`（page `macro_daily`） | 併入 `state/macro_vintages.jsonl` 的 BTC 總額（10/09 起每日 01:40 UTC） | 該紀錄的 `observed_at` | 2026-10-09 |

- known_at 一律是「可證明已公開」的上限：只會比真正公布晚、不會早。Farside 頁面本身不寫公布時間；存檔稀疏的月份
  （2025-02～2026-06 每月只有個位數存檔）known_at 會晚好幾天，這是誠實代價，**不補值、不內插、不拿最新修訂冒充**。
- Farside 直連有 Cloudflare 驗證頁，本模組不繞過，只讀 Internet Archive 已公開的存檔（每次請求間隔 2 秒，429／5xx 退避）。
- 金十快訊搜尋只回最近約 3 天、且多是 A 股 ETF，不能回補，沒有接。
- CoinGlass 中轉金鑰 2026-11-10 到期；到期後 `coinglass` 停止新增，`farside` 不受影響（`last_run.json` 會記錯誤）。

## 目前覆蓋（2026-10-22 全量回補後）

逐月表見 `reports/etf_vintage_coverage_20261022.md`；之後用 `report` 指令重產。

- Wayback 8 個 Farside 頁共 1,462 份存檔：1,408 份解析成功，54 份沒有 ETF 表（上市前的頁、首頁、2026 年部分只有空殼的存檔），0 份失敗。
  共 3,099 筆紀錄。
- 有完整值的觀測日：BTC 696 天、ETH 564 天、SOL 81 天（SOL 2026-04 之後的存檔都沒有表，目前只到 2026-03-25）。
- 「觀測日＋3 天內可證明已完整公開」的比例（macro.yaml 目前假設 D+3）：2024–25 窗內 BTC 53%（延遲中位數 45 小時）、
  ETH 33%（101 小時）。存檔密的月份延遲短：2024-04～09 BTC 延遲中位數 12～17 小時、D+3 內 77%～100%；
  2026-07 起 9～44 小時、52%～87%。2026-03～05 幾乎沒有存檔，延遲中位數 50～100 天，這幾個月實際上不能用。
- 和 CoinGlass 最終值對帳：BTC 696 天中 618 天差距在 15 萬美元內，ETH 564 天中 553 天。其餘是兩家數字本來就不同
  （例如 BTC 2025-02-25 Farside −1,113.7 vs CoinGlass −937.9 百萬美元），不是解析錯。CoinGlass 有、Farside 沒有的日子
  都是美股休市日（CoinGlass 記 0）。

## 紀錄格式（`state/etf_vintages/observations.jsonl`，只追加、進版控當證據）

一列＝某來源頁、某觀測日的一個版本；內容和時間上前一筆相同就不記。

```json
{"source": "farside", "page": "farside.co.uk/btc/", "asset": "BTC", "day": "2026-10-20",
 "total_usd": -6700000.0, "funds_usd": {"FBTC": 3600000.0, "BITB": -10300000.0, ...}, "pending": ["IBIT", "BTCO"],
 "known_at": "2026-10-21T00:36:41+00:00", "known_at_basis": "wayback_capture",
 "fetched_at": "2026-10-22T02:40:00+00:00", "evidence": "https://web.archive.org/web/20261021003641id_/https://farside.co.uk/btc/",
 "kind": "first_seen"}
```

- 「-」＝未公布或休市，不當 0；整列基金都是「-」的休市日不記。
- `pending`：基金在同表較早日期有數字、之後到表尾都是「-」且不超過 3 個交易日 → 當日總額只是部分加總。
  尚未上市的基金、表中間的既定缺口、表尾超過 3 天的停止申報都不算。`pending: null`＝不知道（macro_daily 那批）。
- `kind` 以寫入當下為準；回補可能晚於收集寫入，研究一律以 known_at 排序重算（`daily_table` 已處理）。
- 原始存檔 HTML 存 `user_data/data_ext/etf_vintages/raw/<頁>/<存檔時刻>.html.gz`（不進版控），可重新解析核對。
- 衍生檔（不進版控）：`daily_<資產>_<來源>.csv`（每個觀測日的首次看到、首次完整、最終值與其首次公開時刻、修訂次數、
  距美股收盤延遲）、`progress.json`（已處理的存檔）、`last_run.json`。

## 研究怎麼用

```python
from ai_layer import etf_vintages as ev
ev.asof("BTC", "2025-03-01T00:00:00Z")                    # 當時已完整公開的每日總額（預設 source="farside"）
ev.asof("BTC", t, complete_only=False)                    # 連部分加總也要（當時畫面上看到的）
ev.asof("BTC", t, source="any")                           # farside／coinglass 取 known_at 最晚的一筆
ev.daily_table("BTC")                                     # 每日公布時刻與修訂
ev.known_at("BTC")                                        # 觀測日 → 最終值首次公開時刻（與 macro_vintages.known_at 同形）
```

- 紀錄前、或某天在 at 之前還沒有完整值 → 回傳裡就沒有那天，**不得補 0 或前向填補**。
- 這份資料讓 ETF 可以在 2024–25 研究窗內用「當時可得」的值重評；但重評是新的檢驗，應先預先登記，不拿已曝光的保留段。
- 目前 macro 管線（`macro_backfill.apply_known_at`）仍只認 `macro_vintages`；要改用本模組的時刻是另一個決定，需使用者同意。

## 指令

```bash
.venv/bin/python -m ai_layer.etf_vintages collect            # 排程跑的：近 45 天新存檔（每輪最多 30 筆）＋CoinGlass＋macro_vintages
.venv/bin/python -m ai_layer.etf_vintages backfill --max 300 # Wayback 全歷史，由舊到新、可續跑；和 collect 共用一把鎖
.venv/bin/python -m ai_layer.etf_vintages status             # 每來源每資產天數、完整天數、延遲中位數、各頁處理進度
.venv/bin/python -m ai_layer.etf_vintages report --out reports/etf_vintage_coverage_<日期>.md
.venv/bin/python -m ai_layer.etf_vintages asof BTC 2025-03-01T00:00:00Z
bash scripts/etf_vintage_install.sh [--status|--uninstall]   # 排程安裝／狀態／卸下
```

心跳 `etf-vintage`（`ai_layer/state/heartbeats.json`）；所有來源都失敗才帶 error。
