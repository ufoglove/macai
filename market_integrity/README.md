# market_integrity：市場資料可信度驗證樣本

目的：確認每筆行情 / OI / Funding / 儲備證明資料都帶有**時間戳、來源、市場類型、可用狀態**，
並在交易所異常或資料尚未公布時，**絕不**把缺值當成 0 或「正常」。不含任何下單功能，只用公開唯讀端點。

## 執行

```bash
# 在 Mac mini 上（可直連 Binance）：會抓 11/02–11/03 真實 Binance 資料
python3 -m market_integrity.run_sample --out out/20261103

# 只跑備援樣本 + 重播（不連網）
python3 -m market_integrity.run_sample --skip-live --out out/20261103

# 測試
python3 -m unittest discover -s tests -v
```

只依賴 Python 標準函式庫（3.9+）。

## 每筆記錄欄位

| 欄位 | 意義 |
|---|---|
| `venue` / `source` / `source_tier` | 交易所、端點或網址、一手(primary) / 代用交易所(venue_proxy) / 聚合(aggregator) / 二手報導(secondary) |
| `market_type` | spot / usdm_perpetual / perpetual / aggregate_derivatives / exchange_attestation |
| `event_time` | 數值描述的時間（K 線收盤、OI 快照、資金費結算、PoR 快照） |
| `available_at` | 即時系統最早可以知道它的時間（point-in-time 用） |
| `fetched_at` | 實際抓取時間 |
| `value` / `raw` | 解析後數值與原始字串；非 OK 狀態一律 `None` |
| `availability` | 見下表 |

| availability | 意義 | 可否進策略 |
|---|---|---|
| OK | 已公布、已解析、有時間戳 | ✅ |
| INCOMPLETE_PERIOD | K 線尚未收盤 | ❌（值僅供稽核） |
| NOT_YET_PUBLISHED | 事件時間在抓取時間之後（例：下一次資金費結算） | ❌ |
| SOURCE_UNREACHABLE | DNS/TCP/proxy/地區封鎖（403/451） | ❌ |
| EXCHANGE_ERROR | 5xx、維護、交易所錯誤碼 | ❌ |
| RATE_LIMITED | 418/429 | ❌ |
| EMPTY_RESPONSE | 200 但過去時段無資料 | ❌ |
| MISSING_IN_SERIES | 序列中間缺一筆 | ❌ |
| PARSE_ERROR | 無法解析（例：JS 動態頁面） | ❌ |
| NO_TIMESTAMP | 有數值但不知道是何時的 | ❌（值僅供稽核） |
| SUSPECT_VALUE | 未通過合理性檢查（OI=0、資金費剛好 0、量級錯誤） | ❌（值僅供稽核） |
| NOT_PROVIDED | 該來源不提供此指標 | ❌ |

## 重播

`replay.py` 以每小時 `as_of` 時鐘走過 11/02–11/03，用假的 transport 提供 Binance 格式的歷史資料，
並注入：5xx 停機、地區封鎖、200 空回應、OI 字面 0、資金費 API 維護錯誤碼。
同一份資料同時餵給「完整性管線」與「naive fillna(0) 管線」比較。
