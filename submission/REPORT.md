# K4-Track02-Day17 — Report cá nhân

Phần phân tích tối đa một trang, không tính output ở phần 5.
Định dạng tham chiếu và phạm vi tính trang: [SUBMISSION.md](../docs/SUBMISSION.md).

**Họ tên / MSSV:** Nguyễn Mạnh Hải / 2A202602988
**Repo:** https://github.com/NguyenManhHai2004/K4-Track02-Day17-NguyenManhHai-2A202602988-DataPipelineEngineering
**Commit bài nộp:** bbc847b
**AI đã dùng và phạm vi hỗ trợ (hoặc không dùng):** Claude Code (Sonnet 5.5): hỗ trợ đọc code, chẩn đoán ba lỗi, sửa `pipeline/` và soạn REPORT. Tôi đã review diff và chạy lại verify/test/rerun/dbt để kiểm chứng. Không sửa `scripts/verify.py`, `tests/`, `data/` hay cách tính checksum.
**Nguồn tham khảo khác (nếu có):** Slide Ngày 17, `docs/`, tài liệu Debezium về envelope `before`/`after`/`op`.

## 1. Ba lỗi

| | Lỗi Silver | Lỗi late data | Lỗi xoá (CDC) |
|---|---|---|---|
| **Triệu chứng** | `silver_tickets` có 24 hàng cho 12 ticket; T-91 trả về 3 phiên bản (`low/open`, `high/open`, `high/closed`); gold_doc_chunks có 22 hàng / 9 chunk. | `gold_feature_daily` lệch bản tính lại toàn bộ từ Silver (`c50b8851… != 8630e04a…`); u05 ngày 08-12 chỉ có (2, 0) thay vì (5, 1); `LOOKBACK_DAYS=0 < 3`. | T-97 không là tombstone (`is_deleted=False`, còn user/subject/body); snapshot mới nhất còn T-97 (1 hàng); RAG còn 2 chunk của T-97. |
| **Nguyên nhân gốc** | `upsert_silver_tickets` dùng `INSERT` thay vì upsert theo khoá: mỗi ngày chạy là thêm hàng mới, chạy lại một ngày thì nhân đôi, và không có bảo vệ thứ tự LSN. | `LOOKBACK_DAYS=0` dựa trên giả định "event đến trong vài giây", nhưng event offline của mobile đến muộn tới 3 ngày (P99 = 3.00). Partition cũ không bao giờ được tính lại. | Với `op='d'` thì `after` là null, nên `ticket_id` lấy từ `after` bằng NULL và dòng bị lọc bởi `WHERE ticket_id IS NOT NULL`. Delete không bao giờ tới Silver, nên Gold và RAG không biết. |
| **Cách sửa** (file, vài dòng) | `pipeline/silver.py`: `MERGE INTO silver_tickets … WHEN MATCHED AND s._lsn > t._lsn THEN UPDATE … WHEN NOT MATCHED THEN INSERT`. | `pipeline/config.py`: `LOOKBACK_DAYS = 3` (= ceil(P99) đo bằng `main.py --lateness`). | `pipeline/staging.py`: `ticket_id = coalesce(after.ticket_id, before.ticket_id)`. Delete thành dòng `is_deleted=true` với cột PII null; training snapshot và `gold_doc_chunks` vốn đã lọc `is_deleted`/`_op<>'d'`. |
| **Khái niệm trên slide** | Silver có khoá; idempotent upsert (MERGE); LSN ordering. | Event time vs ingest time; lookback / overwrite-partition; đo trước khi chọn. | CDC log-based Debezium; tombstone; truyền delete xuống Gold/RAG. |

## 2. Các con số

- P99 lateness đo từ Bronze: `3.00` ngày (43 record, P50 = 0, P95 = 2.90, max = 3) → `LOOKBACK_DAYS = 3`
- `submission/checksums.txt`: PASS — Gold checksum: `39e115c510ecdf526800eac227158a4f`
- `make parity`: PARITY (`silver_tickets` 3c15dfd43701, `gold_feature_daily` 8630e04a61d1)

## 3. Lựa chọn công cụ / kỹ thuật (mỗi dòng một câu "vì sao")

- MERGE theo khoá cho `silver_tickets`, overwrite-partition cho `gold_feature_daily`: Silver là bảng thực thể nên cần ghi đè theo khoá và so LSN để chạy lại không nhân đôi hay lùi trạng thái; Gold là aggregate theo ngày nên xoá-rồi-ghi cả partition trong lookback cho kết quả tất định.
- Tombstone thay vì xoá hẳn hàng trong Silver: giữ được dấu vết "ticket này đã bị xoá" để truyền xuống Gold/RAG và để chạy lại không làm ticket "sống lại", trong khi PII đã bị xoá khỏi hàng.
- Snapshot training dựng lại từ Bronze "as of" ngày đó, không sửa snapshot cũ: tái lập được chính xác dữ liệu một lần train, tránh rò rỉ tương lai; muốn đổi thì tạo version mới (`SnapshotImmutableError` chặn sửa lén).
- DuckDB (lite) / dbt (track dbt) cho bài toán cỡ này, chứ không phải Spark: dữ liệu chỉ vài chục dòng, một máy là đủ; Spark thêm chi phí vận hành mà không có lợi. dbt cho cùng logic dưới dạng SQL khai báo, có test và contract, và parity chứng minh hai bản cho cùng checksum.

## 4. Hai câu hỏi suy ngẫm

1. Snapshot bất biến và quyền được xoá thực chất mâu thuẫn, nên cần một ngoại lệ có kiểm soát. Tôi giữ nguyên tắc "không sửa snapshot đang dùng" cho dữ liệu hợp lệ. Với yêu cầu xoá, tôi tạo một lần xoá có ghi log (tombstone/danh sách `ticket_id` cần xoá) rồi dựng lại các snapshot còn giá trị từ Bronze đã được xoá theo yêu cầu (hoặc xoá hàng trong các snapshot đó), ghi version mới và checksum mới để vẫn kiểm toán được. Snapshot đã dùng để train mô hình sẽ được ghi nhận là bị ảnh hưởng. Lưu ý: kết quả lab chỉ xoá T-97 khỏi snapshot mới nhất và RAG chunks, còn snapshot cũ vẫn chứa văn bản T-97 nên đây chưa phải cơ chế xoá PII đầy đủ cho production. Về lâu dài nên áp dụng retention ngắn cho snapshot chứa văn bản thô, hoặc tách PII khỏi snapshot (chỉ giữ ID).
2. Đặt chốt PII ở Silver (ngay sau Bronze) làm chốt bắt buộc, vì mọi bảng Gold đều đọc từ đó. Ngoài regex, thêm bước nhận diện thực thể tên (NER) và bước kiểm tra chất lượng trước khi ghi Gold. Đo bằng một bộ mẫu có gán nhãn tay: precision/recall trên email, số điện thoại và tên, kèm kiểm tra tự động "không còn mẫu PII trong Silver/Gold" (như check verify hiện có), và theo dõi số hàng bị quarantine vì nghi PII.

## 5. Output (dán nguyên văn)

```text
PS> .\.venv\Scripts\python.exe -m scripts.verify
=== verify.py — Day 17 pipeline contracts ===
  [OK ] Bronze  every daily batch landed as Parquet (7 days x 3 sources)
  [OK ] Bronze  re-landing a batch is a no-op (append-only, no duplicate file)
  [OK ] Bronze  Bronze keeps the raw truth: Kafka tombstone + redelivered events are still there
  [OK ] Silver  silver_tickets has exactly one row per ticket_id
  [OK ] Silver  T-91 shows its latest state: high / closed / bug
  [OK ] Silver  deleted ticket T-97 is a tombstone: is_deleted and no personal data left
  [OK ] Silver  no email / phone number survives past Bronze
  [OK ] Silver  silver_events has one row per event_id (Kafka redeliveries removed)
  [OK ] Silver  2 malformed events quarantined with a reason; the run did not halt
  [OK ] Gold    gold_feature_daily reconciles with a full recompute from Silver
  [OK ] Gold    u05's offline events of 08-12 (arrived 08-15) are counted on 08-12
  [OK ] Gold    LOOKBACK_DAYS covers measured P99 lateness (p99=3.00 days)
  [OK ] Gold    training set uses point-in-time priority (T-91 created as 'low')
  [OK ] Gold    late feedback creates a NEW snapshot version; the old one is untouched
  [OK ] Gold    latest training snapshot excludes the deleted ticket T-97
  [OK ] Gold    deletes propagate to the RAG index: no chunk of T-97
  [OK ] Gold    gold_doc_chunks: one row per chunk, and a re-run embeds 0 new chunks
  [OK ] Rerun   re-run 2026-08-12 three times -> Gold checksum identical to a fresh build

RESULT: 18/18 checks — ALL PASS
re-run checksums written to submission/checksums.txt

PS> .\.venv\Scripts\python.exe -m pytest
..................................                                       [100%]
34 passed in 3.42s

PS> .\.venv\Scripts\python.exe -m scripts.rerun_check
# Lab 17 — re-run check for 2026-08-12

run                     gold_feature_daily    gold_training_set     gold_doc_chunks       gold (combined)
fresh build             8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #1 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #2 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #3 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f

RESULT: PASS — 3 re-runs, identical checksums

PS> .\.venv\Scripts\python.exe main.py --lateness
event lateness over 43 Bronze records (calendar days): p50=0.00 p95=2.90 p99=3.00 max=3
-> lookback must be >= ceil(p99) = 3 day(s); config.LOOKBACK_DAYS = 3

PS> .\.venv\Scripts\python.exe main.py --land-only; cd dbt_project; dbt build --event-time-start 2026-08-10 --event-time-end 2026-08-17
15:49:27  Running with dbt=1.12.5
15:49:27  Registered adapter: duckdb=1.11.0
15:49:28  Unable to do partial parsing because saved manifest not found. Starting full parse.
15:49:32  Found 5 models, 13 data tests, 2 sources, 502 macros, 1 unit test
15:49:32
15:49:32  Concurrency: 1 threads (target='dev')
15:49:32
15:49:35  1 of 19 START sql view model main.stg_events ................................... [RUN]
15:49:35  1 of 19 OK created sql view model main.stg_events .............................. [OK in 0.14s]
15:49:35  2 of 19 START sql view model main.stg_ticket_changes ........................... [RUN]
15:49:35  2 of 19 OK created sql view model main.stg_ticket_changes ...................... [OK in 0.06s]
15:49:35  3 of 19 START sql incremental model main.silver_events ......................... [RUN]
15:49:35  3 of 19 OK created sql incremental model main.silver_events .................... [OK in 0.15s]
15:49:35  4 of 19 START unit_test silver_tickets::silver_tickets_latest_change_wins_and_delete_is_tombstone  [RUN]
15:49:36  4 of 19 PASS silver_tickets::silver_tickets_latest_change_wins_and_delete_is_tombstone  [PASS in 0.22s]
15:49:36  8 of 19 START sql incremental model main.silver_tickets ........................ [RUN]
15:49:36  8 of 19 OK created sql incremental model main.silver_tickets ................... [OK in 0.16s]
15:49:36  5 of 19 START test not_null_silver_events_event_id ............................. [RUN]
15:49:36  5 of 19 PASS not_null_silver_events_event_id ................................... [PASS in 0.05s]
15:49:36  6 of 19 START test not_null_silver_events_user_id .............................. [RUN]
15:49:36  6 of 19 PASS not_null_silver_events_user_id .................................... [PASS in 0.03s]
15:49:36  7 of 19 START test unique_silver_events_event_id ............................... [RUN]
15:49:36  7 of 19 PASS unique_silver_events_event_id ..................................... [PASS in 0.02s]
15:49:36  9 of 19 START test accepted_values_silver_tickets_category__bug__billing__other  [RUN]
15:49:36  9 of 19 PASS accepted_values_silver_tickets_category__bug__billing__other ...... [PASS in 0.02s]
15:49:36  10 of 19 START test accepted_values_silver_tickets_priority__low__medium__high . [RUN]
15:49:36  10 of 19 PASS accepted_values_silver_tickets_priority__low__medium__high ....... [PASS in 0.02s]
15:49:36  11 of 19 START test accepted_values_silver_tickets_status__open__pending__closed  [RUN]
15:49:36  11 of 19 PASS accepted_values_silver_tickets_status__open__pending__closed ..... [PASS in 0.02s]
15:49:36  12 of 19 START test not_null_silver_tickets__lsn ............................... [RUN]
15:49:36  12 of 19 PASS not_null_silver_tickets__lsn ..................................... [PASS in 0.02s]
15:49:36  13 of 19 START test not_null_silver_tickets_is_deleted ......................... [RUN]
15:49:36  13 of 19 PASS not_null_silver_tickets_is_deleted ............................... [PASS in 0.02s]
15:49:36  14 of 19 START test not_null_silver_tickets_ticket_id .......................... [RUN]
15:49:36  14 of 19 PASS not_null_silver_tickets_ticket_id ................................ [PASS in 0.02s]
15:49:36  15 of 19 START test unique_silver_tickets_ticket_id ............................ [RUN]
15:49:36  15 of 19 PASS unique_silver_tickets_ticket_id .................................. [PASS in 0.02s]
15:49:36  16 of 19 START sql microbatch model main.gold_feature_daily .................... [RUN]
15:49:36  Batch 1 of 7 START batch 2026-08-10 of main.gold_feature_daily ....................... [RUN]
15:49:36  Batch 1 of 7 OK created batch 2026-08-10 of main.gold_feature_daily .................. [OK in 0.06s]
15:49:36  Batch 2 of 7 START batch 2026-08-11 of main.gold_feature_daily ....................... [RUN]
15:49:36  Batch 2 of 7 OK created batch 2026-08-11 of main.gold_feature_daily .................. [OK in 0.13s]
15:49:36  Batch 3 of 7 START batch 2026-08-12 of main.gold_feature_daily ....................... [RUN]
15:49:36  Batch 3 of 7 OK created batch 2026-08-12 of main.gold_feature_daily .................. [OK in 0.08s]
15:49:36  Batch 4 of 7 START batch 2026-08-13 of main.gold_feature_daily ....................... [RUN]
15:49:36  Batch 4 of 7 OK created batch 2026-08-13 of main.gold_feature_daily .................. [OK in 0.06s]
15:49:36  Batch 5 of 7 START batch 2026-08-14 of main.gold_feature_daily ....................... [RUN]
15:49:37  Batch 5 of 7 OK created batch 2026-08-14 of main.gold_feature_daily .................. [OK in 0.07s]
15:49:37  Batch 6 of 7 START batch 2026-08-15 of main.gold_feature_daily ....................... [RUN]
15:49:37  Batch 6 of 7 OK created batch 2026-08-15 of main.gold_feature_daily .................. [OK in 0.06s]
15:49:37  Batch 7 of 7 START batch 2026-08-16 of main.gold_feature_daily ....................... [RUN]
15:49:37  Batch 7 of 7 OK created batch 2026-08-16 of main.gold_feature_daily .................. [OK in 0.07s]
15:49:37  16 of 19 OK created sql microbatch model main.gold_feature_daily ............... [SUCCESS in 0.56s]
15:49:37  17 of 19 START test dbt_utils_free_unique_combination_gold_feature_daily_user_id__event_date  [RUN]
15:49:37  17 of 19 PASS dbt_utils_free_unique_combination_gold_feature_daily_user_id__event_date  [PASS in 0.03s]
15:49:37  18 of 19 START test not_null_gold_feature_daily_event_date ..................... [RUN]
15:49:37  18 of 19 PASS not_null_gold_feature_daily_event_date ........................... [PASS in 0.02s]
15:49:37  19 of 19 START test not_null_gold_feature_daily_user_id ........................ [RUN]
15:49:37  19 of 19 PASS not_null_gold_feature_daily_user_id .............................. [PASS in 0.02s]
15:49:37
15:49:37  Finished running 3 incremental models, 13 data tests, 1 unit test, 2 view models in 0 hours 0 minutes and 5.14 seconds (5.14s).
15:49:37
15:49:37  Completed successfully
15:49:37
15:49:37  Done. PASS=19 WARN=0 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=19

PS> .\.venv\Scripts\python.exe -m scripts.parity
=== parity: lite pipeline vs dbt ===
  [OK ] silver_tickets       lite 3c15dfd43701  dbt 3c15dfd43701
  [OK ] gold_feature_daily   lite 8630e04a61d1  dbt 8630e04a61d1
RESULT: PARITY — both implementations agree
```

Ghi chú: dùng lệnh PowerShell tương đương `make` theo SUBMISSION.md. Output dbt và parity được lấy từ cùng một lần chạy trên Bronze đã land.
