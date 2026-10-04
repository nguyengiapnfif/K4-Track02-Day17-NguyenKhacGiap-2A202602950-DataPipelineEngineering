# K4-Track02-Day17 — Report cá nhân

Phần phân tích tối đa một trang, không tính output ở phần 5.
Định dạng tham chiếu và phạm vi tính trang: [SUBMISSION.md](../docs/SUBMISSION.md).

**Họ tên / MSSV:** Nguyễn Khắc Giáp / 2A202602950
**Repo:** https://github.com/nguyengiapnfif/K4-Track02-Day17-NguyenKhacGiap-2A202602950-DataPipelineEngineering
**Commit bài nộp:** ``
**AI đã dùng và phạm vi hỗ trợ (hoặc không dùng):** GitHub Copilot hỗ trợ đọc code,
phân tích lỗi, đề xuất và kiểm tra các thay đổi; người học review và chịu trách nhiệm
về mã nguồn cũng như nội dung báo cáo.
**Nguồn tham khảo khác (nếu có):**

## 1. Ba lỗi

Mỗi lỗi 4 dòng. Triệu chứng = thứ bạn *thấy* đầu tiên (check nào fail, số nào lạ,
checksum nào lệch) — không phải cách sửa.

| | Lỗi Silver | Lỗi late data | Lỗi xoá (CDC) |
|---|---|---|---|
| **Triệu chứng** | `silver_tickets` có 24 dòng cho 12 ticket; T-91 có cả trạng thái cũ và mới. | Feature của `u05` ngày 08-12 chỉ có 2 events thay vì 5; P99 lateness là 3 ngày. | T-97 vẫn còn trong Silver/Gold dù nguồn phát CDC delete với `after = null`. |
| **Nguyên nhân gốc** | Code `INSERT` mọi batch, không merge theo `ticket_id` và không kiểm tra LSN. | `LOOKBACK_DAYS = 0`, nên batch ngày ingest không tính lại ngày event cũ. | Staging chỉ lấy `ticket_id` từ `after`, nhưng delete không có `after`. |
| **Cách sửa** (file, vài dòng) | `pipeline/silver.py`: dùng `MERGE`, cập nhật khi `_lsn` mới hơn. | `pipeline/config.py`: đặt `LOOKBACK_DAYS = 3`; Gold vẫn nhóm theo `event_time`. | `pipeline/staging.py`: lấy khóa bằng `coalesce(after, before, key)`. |
| **Khái niệm trên slide** | Upsert theo khóa, latest state wins, CDC ordering. | Event time, late data, lookback window. | CDC delete, Kafka tombstone, soft delete. |

## 2. Các con số

- P99 lateness đo từ Bronze: `3.00` ngày → `LOOKBACK_DAYS = 3`
- `submission/checksums.txt`: PASS — Gold checksum: `39e115c510ecdf526800eac227158a4f`
- `make parity`: PARITY

## 3. Lựa chọn công cụ / kỹ thuật (mỗi dòng một câu "vì sao")

- MERGE theo khoá cho `silver_tickets`, overwrite-partition cho `gold_feature_daily`: giữ một trạng thái hiện tại cho ticket và tính lại chính xác các ngày chịu ảnh hưởng.
- Tombstone thay vì xoá hẳn hàng trong Silver: giữ bằng chứng CDC và ngăn bản ghi cũ hồi sinh khi replay.
- Snapshot training dựng lại từ Bronze "as of" ngày đó, không sửa snapshot cũ: bảo đảm tái lập lịch sử và point-in-time correctness.
- DuckDB (lite) / dbt (track dbt) cho bài toán cỡ này, chứ không phải Spark: chạy cục bộ nhanh, đủ SQL và dễ đối chiếu hai implementation.

## 4. Hai câu hỏi suy ngẫm

1. Snapshot `v2026-08-12`..`v2026-08-14` vẫn chứa văn bản của T-97 (đã bị xoá ngày
   08-15). "Snapshot bất biến" và "quyền được xoá dữ liệu" mâu thuẫn — bạn xử lý thế nào?

   Snapshot cũ được giữ bất biến để bảo đảm khả năng tái lập và kiểm toán dữ liệu tại
   thời điểm lịch sử; vì vậy không sửa trực tiếp nội dung của các snapshot đã phát hành.
   Ngược lại, quyền được xoá phải được áp dụng cho các bảng đang phục vụ người dùng:
   CDC delete tạo tombstone ở Silver, còn training snapshot mới nhất và RAG index loại
   T-97. Trong production, cần bổ sung một quy trình privacy deletion riêng: lập danh
   mục mọi bản sao chứa PII, xoá hoặc mã hoá/thu hồi quyền truy cập theo chính sách
   retention, ghi audit log, rồi tạo lại các bảng phục vụ hiện tại. Nếu pháp lý yêu cầu
   xoá cả dữ liệu lịch sử, tính bất biến của snapshot phải có ngoại lệ được kiểm soát
   và có bằng chứng audit.

2. Regex che được email và số điện thoại, nhưng tên "Nguyễn Văn An" vẫn còn. Bạn sẽ
   đặt chốt PII nào, ở tầng nào, và đo nó ra sao?

   Regex hiện tại chỉ là lớp bảo vệ cơ bản ở ranh giới Bronze -> Silver, nên cần thêm
   một PII gate sau bước parsing/chuẩn hoá và trước khi dữ liệu đi vào Gold, LLM hoặc
   RAG. Gate này nên kết hợp regex với recognizer cho tên người, địa chỉ, mã định danh
   và danh sách từ nhạy cảm; bản ghi không đạt phải vào quarantine thay vì âm thầm
   ghi tiếp. Tôi sẽ xây một tập kiểm thử có nhãn gồm email, số điện thoại, họ tên,
   địa chỉ và các cách viết có dấu/không dấu, sau đó đo precision, recall và tỷ lệ
   PII lọt qua (false negative). Ngoài kiểm thử offline, có thể lấy mẫu dữ liệu sau
   gate để kiểm tra định kỳ, theo dõi tỷ lệ quarantine và đặt ngưỡng cảnh báo cho
   từng loại PII. Không nên ghi raw PII vào log hoặc output kiểm tra.

## 5. Output (dán nguyên văn)

```text
$ make verify
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
$ make test
..................................                                                                                                                                                                                                                             [100%]
34 passed in 0.96s
$ make rerun3
# Lab 17 — re-run check for 2026-08-12

run                     gold_feature_daily    gold_training_set     gold_doc_chunks       gold (combined)
fresh build             8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #1 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #2 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #3 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f

RESULT: PASS — 3 re-runs, identical checksums

$ make lateness
event lateness over 43 Bronze records (calendar days): p50=0.00 p95=2.90 p99=3.00 max=3
-> lookback must be >= ceil(p99) = 3 day(s); config.LOOKBACK_DAYS = 3

$ make dbt
cd dbt_project && DBT_PROFILES_DIR=. /Users/Giap/PycharmProjects/VinAI/Day17/K4-Track02-Day17-NguyenKhacGiap-2A202602950-DataPipelineEngineering/.venv/bin/dbt build --event-time-start 2026-08-10 --event-time-end 2026-08-17
15:34:44  Running with dbt=1.12.5
15:34:44  Registered adapter: duckdb=1.11.0
15:34:44  Found 5 models, 13 data tests, 2 sources, 502 macros, 1 unit test
15:34:44  
15:34:44  Concurrency: 1 threads (target='dev')
15:34:44  
15:34:44  1 of 19 START sql view model main.stg_events ................................... [RUN]
15:34:44  1 of 19 OK created sql view model main.stg_events .............................. [OK in 0.03s]
15:34:44  2 of 19 START sql view model main.stg_ticket_changes ........................... [RUN]
15:34:44  2 of 19 OK created sql view model main.stg_ticket_changes ...................... [OK in 0.01s]
15:34:44  3 of 19 START sql incremental model main.silver_events ......................... [RUN]
15:34:44  3 of 19 OK created sql incremental model main.silver_events .................... [OK in 0.05s]
15:34:44  4 of 19 START unit_test silver_tickets::silver_tickets_latest_change_wins_and_delete_is_tombstone  [RUN]
15:34:44  4 of 19 PASS silver_tickets::silver_tickets_latest_change_wins_and_delete_is_tombstone  [PASS in 0.05s]
15:34:44  8 of 19 START sql incremental model main.silver_tickets ........................ [RUN]
15:34:44  8 of 19 OK created sql incremental model main.silver_tickets ................... [OK in 0.06s]
15:34:44  5 of 19 START test not_null_silver_events_event_id ............................. [RUN]
15:34:44  5 of 19 PASS not_null_silver_events_event_id ................................... [PASS in 0.02s]
15:34:44  6 of 19 START test not_null_silver_events_user_id .............................. [RUN]
15:34:44  6 of 19 PASS not_null_silver_events_user_id .................................... [PASS in 0.00s]
15:34:44  7 of 19 START test unique_silver_events_event_id ............................... [RUN]
15:34:44  7 of 19 PASS unique_silver_events_event_id ..................................... [PASS in 0.01s]
15:34:44  9 of 19 START test accepted_values_silver_tickets_category__bug__billing__other  [RUN]
15:34:44  9 of 19 PASS accepted_values_silver_tickets_category__bug__billing__other ...... [PASS in 0.01s]
15:34:44  10 of 19 START test accepted_values_silver_tickets_priority__low__medium__high . [RUN]
15:34:44  10 of 19 PASS accepted_values_silver_tickets_priority__low__medium__high ....... [PASS in 0.01s]
15:34:44  11 of 19 START test accepted_values_silver_tickets_status__open__pending__closed  [RUN]
15:34:44  11 of 19 PASS accepted_values_silver_tickets_status__open__pending__closed ..... [PASS in 0.01s]
15:34:44  12 of 19 START test not_null_silver_tickets__lsn ............................... [RUN]
15:34:44  12 of 19 PASS not_null_silver_tickets__lsn ..................................... [PASS in 0.01s]
15:34:44  13 of 19 START test not_null_silver_tickets_is_deleted ......................... [RUN]
15:34:44  13 of 19 PASS not_null_silver_tickets_is_deleted ............................... [PASS in 0.01s]
15:34:44  14 of 19 START test not_null_silver_tickets_ticket_id .......................... [RUN]
15:34:44  14 of 19 PASS not_null_silver_tickets_ticket_id ................................ [PASS in 0.00s]
15:34:44  15 of 19 START test unique_silver_tickets_ticket_id ............................ [RUN]
15:34:44  15 of 19 PASS unique_silver_tickets_ticket_id .................................. [PASS in 0.00s]
15:34:44  16 of 19 START sql microbatch model main.gold_feature_daily .................... [RUN]
15:34:44  Batch 1 of 7 START batch 2026-08-10 of main.gold_feature_daily ....................... [RUN]
15:34:44  Batch 1 of 7 OK created batch 2026-08-10 of main.gold_feature_daily .................. [OK in 0.01s]
15:34:44  Batch 2 of 7 START batch 2026-08-11 of main.gold_feature_daily ....................... [RUN]
15:34:44  Batch 2 of 7 OK created batch 2026-08-11 of main.gold_feature_daily .................. [OK in 0.01s]
15:34:44  Batch 3 of 7 START batch 2026-08-12 of main.gold_feature_daily ....................... [RUN]
15:34:44  Batch 3 of 7 OK created batch 2026-08-12 of main.gold_feature_daily .................. [OK in 0.01s]
15:34:44  Batch 4 of 7 START batch 2026-08-13 of main.gold_feature_daily ....................... [RUN]
15:34:44  Batch 4 of 7 OK created batch 2026-08-13 of main.gold_feature_daily .................. [OK in 0.01s]
15:34:44  Batch 5 of 7 START batch 2026-08-14 of main.gold_feature_daily ....................... [RUN]
15:34:44  Batch 5 of 7 OK created batch 2026-08-14 of main.gold_feature_daily .................. [OK in 0.01s]
15:34:44  Batch 6 of 7 START batch 2026-08-15 of main.gold_feature_daily ....................... [RUN]
15:34:44  Batch 6 of 7 OK created batch 2026-08-15 of main.gold_feature_daily .................. [OK in 0.01s]
15:34:44  Batch 7 of 7 START batch 2026-08-16 of main.gold_feature_daily ....................... [RUN]
15:34:44  Batch 7 of 7 OK created batch 2026-08-16 of main.gold_feature_daily .................. [OK in 0.01s]
15:34:44  16 of 19 OK created sql microbatch model main.gold_feature_daily ............... [SUCCESS in 0.08s]
15:34:44  17 of 19 START test dbt_utils_free_unique_combination_gold_feature_daily_user_id__event_date  [RUN]
15:34:44  17 of 19 PASS dbt_utils_free_unique_combination_gold_feature_daily_user_id__event_date  [PASS in 0.01s]
15:34:44  18 of 19 START test not_null_gold_feature_daily_event_date ..................... [RUN]
15:34:44  18 of 19 PASS not_null_gold_feature_daily_event_date ........................... [PASS in 0.01s]
15:34:44  19 of 19 START test not_null_gold_feature_daily_user_id ........................ [RUN]
15:34:44  19 of 19 PASS not_null_gold_feature_daily_user_id .............................. [PASS in 0.01s]
15:34:44  
15:34:44  Finished running 3 incremental models, 13 data tests, 1 unit test, 2 view models in 0 hours 0 minutes and 0.44 seconds (0.44s).
15:34:44  
15:34:44  Completed successfully
15:34:44  
15:34:44  Done. PASS=19 WARN=0 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=19

$ make parity
=== parity: lite pipeline vs dbt ===
  [OK ] silver_tickets       lite 3c15dfd43701  dbt 3c15dfd43701
  [OK ] gold_feature_daily   lite 8630e04a61d1  dbt 8630e04a61d1
RESULT: PARITY — both implementations agree
```

Nếu dùng PowerShell, ghi lệnh tương đương và output thực tế theo [SUBMISSION.md](../docs/SUBMISSION.md).
Nếu làm bonus, thêm output B1 hoặc đường dẫn bằng chứng B2 ở cuối phần này.
