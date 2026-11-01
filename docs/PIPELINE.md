# Thiết kế pipeline: Tiki Reviews — Kafka → Spark → Lưu trữ → Phân tích

Tài liệu này mô tả từng bước của pipeline xử lý review Tiki theo thời gian thực. Ở mỗi bước có giải thích **vì sao chọn cách làm đó**, **lợi / hại**, và **trade-off** so với các phương án khác.

Hạ tầng dựa trên `docker-compose.yml` hiện có: Kafka 3.9.1 (KRaft, 1 broker), Spark 3.5.5 (`local[N]`), PostgreSQL 16.

---

## Mục lục

0. [Tổng quan kiến trúc](#0-tổng-quan-kiến-trúc)
1. [Thu thập dữ liệu (Crawler)](#1-thu-thập-dữ-liệu-crawler)
2. [Kafka — tầng đệm sự kiện](#2-kafka--tầng-đệm-sự-kiện)
3. [Spark Structured Streaming — xử lý](#3-spark-structured-streaming--xử-lý)
4. [Lưu trữ — Data lake + Serving DB](#4-lưu-trữ--data-lake--serving-db)
5. [Gán sentiment](#5-gán-sentiment)
6. [Phân tích & hiển thị](#6-phân-tích--hiển-thị)
7. [Vận hành, lỗi và phục hồi](#7-vận-hành-lỗi-và-phục-hồi)
8. [Lộ trình triển khai](#8-lộ-trình-triển-khai)
9. [Bảng tổng hợp trade-off](#9-bảng-tổng-hợp-trade-off)

---

## 0. Tổng quan kiến trúc

```
┌────────────────┐  JSON, key=product_id   ┌──────────────────────────┐
│ Tiki crawler   │ ──────────────────────▶ │ Kafka                    │
│ (poll 15–30')  │                         │  tiki.reviews.raw   (3p) │
│ watermark/sp   │ ──────────────────────▶ │  tiki.products.raw  (3p) │
└────────────────┘                         └────────────┬─────────────┘
                                                        │
        ┌───────────────────────────────────────────────┼───────────────────────┐
        │ Spark Structured Streaming (1 ứng dụng, 3 query)                       │
        │                                               │                        │
        │  Q1 BRONZE: Kafka ──────────────────────▶ lake/bronze (Parquet, thô)   │
        │                                               │                        │
        │  Q2 SILVER: Kafka → parse → clean → dedupe ─▶ lake/silver (Parquet)    │
        │                       └─ bản ghi lỗi ───────▶ lake/quarantine          │
        │                                               │                        │
        │  Q3 SERVING: readStream lake/silver → sentiment → UPSERT PostgreSQL    │
        └───────────────────────────────────────────────┼───────────────────────┘
                                                        │
               ┌────────────────────────────────────────┴──────────────┐
               ▼                                                       ▼
   Spark batch (định kỳ): silver → lake/gold         PostgreSQL: bảng + SQL views
   (tổng hợp nặng, lịch sử dài)                       ──▶ Dashboard (Metabase)
```

### Vì sao kiến trúc này (Kappa) chứ không phải Lambda?

| | **Kappa** (chọn) | Lambda |
|---|---|---|
| Ý tưởng | Một luồng streaming duy nhất. Muốn xử lý lại thì replay từ Kafka hoặc bronze | Hai nhánh song song: batch (chính xác) + speed layer (nhanh) |
| Lợi | Chỉ một bộ logic làm sạch, dễ hiểu, dễ demo | Batch layer tính lại toàn bộ được, rất chính xác |
| Hại | Muốn tính lại lịch sử dài phải replay | **Viết logic 2 lần** (batch + stream), dễ lệch nhau |
| Hợp với dự án? | Có. Dữ liệu là stream từ crawler, không có kho offline sẵn | Không. Không có nguồn batch riêng |

Vẫn có một job batch (bước 6) để tổng hợp từ silver. Nhưng job này **đọc kết quả của stream**, không xử lý lại dữ liệu thô bằng logic khác, nên vẫn là Kappa.

---

## 1. Thu thập dữ liệu (Crawler)

### 1.1. Nguồn dữ liệu

Nguồn là API nội bộ của web Tiki (không có tài liệu, không cần API key; đã thử gọi được mà không cần cookie):

| Endpoint | Dùng để |
|---|---|
| `GET /api/personalish/v1/blocks/listings?category=..&page=..&limit=40` | Phát hiện sản phẩm trong danh mục |
| `GET /api/v2/products/{id}` | Thông tin sản phẩm: giá, brand, discount |
| `GET /api/v2/reviews?product_id=..&sort=id\|desc&page=..&limit=20` | Review, sắp xếp **mới nhất trước** |

### 1.2. Luồng cào tăng dần (incremental)

```
Khởi động: nạp state {product_id: max_review_id_đã_thấy} từ file/Postgres

Mỗi chu kỳ (15–30 phút):
  1. (mỗi 24h) quét listings → thêm sản phẩm mới vào danh sách theo dõi
  2. for product in danh sách theo dõi (ThreadPool 3–5 luồng):
        a. GET products/{id} → gửi 1 snapshot vào tiki.products.raw  (kèm giá → dùng cho phân tích giá)
        b. page = 1
           loop:
             GET reviews?sort=id|desc&page=page
             for review in data:
                 if review.id <= state[product]: DỪNG     ← gặp review cũ
                 gửi tiki.reviews.raw (thêm crawled_at)
             if page >= paging.last_page: DỪNG
             page += 1
        c. cập nhật state[product] = max(review.id)
  3. producer.flush(); lưu state
```

**Tại sao dùng watermark theo `review.id` với sort `id|desc`?**
- Crawler hiện tại dùng `score|desc` (sắp theo "Hữu ích") nên thứ tự review lộn xộn, mỗi lần chạy phải quét lại toàn bộ trang. Sản phẩm lớn nhất có 3.235 review, tức 162 trang.
- Với `id|desc`, các lần chạy sau thường **chỉ tốn 1 request cho mỗi sản phẩm**.
- Dùng `id` thay vì `created_at` vì `id` tăng dần và không trùng, còn `created_at` có thể có nhiều review trùng cùng một giây.

| Trade-off | Lựa chọn | Giá phải trả |
|---|---|---|
| Polling định kỳ hay realtime thật | Polling 15–30 phút (Tiki không có webhook) | Độ trễ cuối cùng tính bằng phút, không phải giây |
| Chu kỳ ngắn hay dài | 15–30 phút | Ngắn hơn thì dữ liệu tươi hơn nhưng dễ bị chặn IP / 429 |
| Số luồng | 3–5 | Nhiều luồng thì nhanh hơn nhưng dễ bị chặn. Đã cào ổn định 20 nghìn review với 5 luồng và delay 0,5–1,2s |
| State lưu ở file hay Postgres | File JSON (đơn giản) → Postgres (khi cần) | Nếu mất file state, lần sau sẽ cào lại từ đầu. Việc này **an toàn** vì downstream có dedupe, chỉ tốn thời gian |

### 1.3. Dữ liệu gửi đi

Mỗi review là **một message thô**, có thêm 3 trường do crawler gắn vào:

```json
{
  "review_id": 19732262, "product_id": 123836523, "rating": 5,
  "title": "Cực kì hài lòng", "content": "...", "created_at": 1705502788,
  "customer_id": 123, "purchased": true, "purchased_at": 1705400000,
  "delivery_rating": [...], "comment_count": 0, "spid": 71523845,
  "crawled_at": 1793428800, "source": "tiki", "schema_version": 1
}
```

**Tại sao gửi dữ liệu thô, không làm sạch tại crawler?**
- ✅ Logic làm sạch nằm ở **một chỗ** (Spark). Khi sửa logic thì replay từ bronze, không phải cào lại.
- ✅ Crawler đơn giản, chỉ lo việc thu thập.
- ❌ Message lớn hơn một chút. Không đáng kể (~1 KB/review).

**Tại sao thêm `crawled_at`?**
- Để đo độ trễ của pipeline.
- Quan trọng hơn: lần cào đầu sẽ lấy về review từ **2013** (`created_at` rất cũ). Nếu Spark đặt watermark theo `created_at` thì những review này sẽ bị **loại như dữ liệu đến muộn**. `crawled_at` luôn gần với thời điểm hiện tại nên an toàn khi dùng làm mốc watermark.

**Quyền riêng tư:** Dữ liệu có `customer_name`, `customer_id` của người dùng thật. Nên **không gửi `customer_name`** và **hash `customer_id`** (ví dụ `sha256(id + salt)`). Vẫn đếm được số người dùng khác nhau nhưng không lộ danh tính.

---

## 2. Kafka — tầng đệm sự kiện

### 2.1. Tại sao cần Kafka? Sao không ghi thẳng crawler → Postgres?

| | Có Kafka | Không có Kafka |
|---|---|---|
| Crawler và Spark phụ thuộc nhau? | **Không.** Spark tắt thì message vẫn nằm trong Kafka, bật lại đọc tiếp | Có. DB chết thì crawler phải tự retry hoặc mất dữ liệu |
| Replay | Đọc lại từ offset bất kỳ trong thời gian retention | Không replay được |
| Nhiều consumer | Thêm job mới đọc cùng topic mà không ảnh hưởng job cũ | Phải sửa crawler |
| Chi phí | Thêm một hệ thống phải vận hành, tốn khoảng 1 GB RAM | Đơn giản hơn |

Với đồ án Big Data, Kafka là thành phần cốt lõi để chứng minh **tách rời producer và consumer**, chịu được tải dồn (backpressure) và replay.

### 2.2. Thiết kế topic

| Topic | Partition | Key | Nội dung |
|---|---|---|---|
| `tiki.reviews.raw` | 3 | `product_id` | Review thô |
| `tiki.products.raw` | 3 | `product_id` | Snapshot thông tin và giá sản phẩm mỗi lần cào |

Tạo topic:
```powershell
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:19092 `
  --create --if-not-exists --topic tiki.reviews.raw --partitions 3 --replication-factor 1 `
  --config retention.ms=1209600000   # 14 ngày
```

**Đặt tên `<nguồn>.<thực thể>.<giai đoạn>`:** nhìn tên là biết dữ liệu đến từ đâu và ở giai đoạn nào. Sau này có thể thêm `shopee.reviews.raw` mà không đụng vào topic cũ.

**Tại sao key = `product_id`?**
- ✅ Các review của cùng một sản phẩm vào cùng một partition nên **giữ được thứ tự** theo sản phẩm. Điều này có ích nếu sau này tổng hợp có trạng thái theo sản phẩm.
- ❌ **Lệch tải (skew):** sản phẩm lớn nhất chiếm 3.235 trên 20.705 review (16%), nên một partition sẽ nặng hơn các partition còn lại.
- Phương án khác: key = `review_id` thì phân tải đều nhưng mất thứ tự theo sản phẩm. Ở quy mô này skew không đáng kể, nên **ưu tiên giữ thứ tự**.

**Tại sao 3 partition?**
- Số partition là **giới hạn trên của độ song song** khi Spark đọc (mỗi partition tương ứng một task).
- Spark đang chạy `local[2]`–`local[4]`, nên 3 partition là vừa.
- ❌ Tăng partition thì dễ (nhưng làm đổi cách phân key), giảm thì không được. Chọn ít để đơn giản, đủ để demo song song.

**Tại sao `replication-factor 1`?**
- Chỉ có 1 broker nên không thể đặt cao hơn.
- ❌ Broker chết là **mất dữ liệu chưa xử lý**. Môi trường production cần tối thiểu 3 broker với `replication-factor=3` và `min.insync.replicas=2`. Đây là **trade-off chấp nhận được vì là môi trường demo**, nên ghi rõ trong báo cáo.

**Tại sao retention 14 ngày (mặc định là 7)?**
- Kafka là bộ đệm, không phải nơi lưu trữ lâu dài. Lưu trữ lâu dài là việc của bronze Parquet.
- 14 ngày đủ để sửa lỗi Spark và replay. Lâu hơn thì tốn ổ đĩa của Docker.

### 2.3. Định dạng message: JSON hay Avro/Protobuf?

| | **JSON** (chọn) | Avro + Schema Registry |
|---|---|---|
| Dễ đọc / debug | ✅ Xem trực tiếp bằng `kafka-console-consumer` | ❌ Dạng nhị phân |
| Kích thước | ❌ Lớn hơn khoảng 2–3 lần | ✅ Gọn |
| Kiểm soát schema | ❌ Không có, producer gửi sai thì consumer mới phát hiện | ✅ Schema Registry chặn ngay message sai schema |
| Hạ tầng | ✅ Không thêm gì | ❌ Thêm container Schema Registry |

Chọn JSON và **tự quản lý schema bằng trường `schema_version`**, kèm bước kiểm tra trong Spark (mục 3.3). Khi hệ thống lớn và có nhiều đội cùng gửi dữ liệu thì mới cần đến Avro.

### 2.4. Cấu hình producer

```python
KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    key_serializer=lambda k: str(k).encode(),
    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
    acks="all",              # chờ broker xác nhận đã ghi
    retries=5,
    linger_ms=50,            # gom message 50ms rồi gửi theo lô
    compression_type="gzip",
)
```

| Tham số | Lợi | Hại |
|---|---|---|
| `acks="all"` | Không mất message khi broker nhận xong | Chậm hơn `acks=1` một chút |
| `retries=5` | Tự gửi lại khi mạng chập chờn | **Có thể gửi trùng message**, nên downstream phải dedupe (mục 3.5) |
| `linger_ms=50` + gzip | Gửi theo lô, tăng throughput, tiết kiệm băng thông | Tăng độ trễ thêm 50ms, không đáng kể vì crawler poll theo phút |

Không dùng `.get(timeout=30)` sau mỗi lần `send` như `producer.py` cũ. Cách đó biến việc gửi bất đồng bộ thành đồng bộ và rất chậm. Chỉ cần `flush()` ở cuối chu kỳ.

**Ngữ nghĩa giao nhận: at-least-once.** Message có thể bị trùng nhưng không bị mất. Kafka có idempotent producer và transactions để đạt exactly-once, nhưng không giải quyết được việc **crawler cào lại cùng một review** (khi mất file state). Vì vậy dedupe ở downstream là bắt buộc, và lúc đó exactly-once phía producer trở nên thừa.

---

## 3. Spark Structured Streaming — xử lý

### 3.1. Tại sao Spark Structured Streaming?

| | **Spark SS** (chọn) | Apache Flink | Python consumer tự viết |
|---|---|---|---|
| Mô hình | Micro-batch (độ trễ khoảng giây) | Xử lý từng sự kiện (độ trễ khoảng ms) | Tùy |
| Batch + stream cùng API | ✅ Một DataFrame API cho cả hai | Có, nhưng hệ sinh thái Python yếu hơn | ❌ |
| Checkpoint / phục hồi | ✅ Có sẵn | ✅ Có sẵn | ❌ Phải tự viết |
| Đọc/ghi Parquet, JDBC | ✅ Có sẵn | Có | Phải tự viết |
| Đã có trong repo | ✅ | ❌ | — |

Nguồn dữ liệu poll theo phút, nên độ trễ cỡ giây của micro-batch là quá đủ. Ưu điểm mili-giây của Flink không đem lại giá trị gì ở đây.

### 3.2. Cấu trúc: một ứng dụng, ba query

```python
spark.streams.awaitAnyTermination()   # chạy cả 3 query trong cùng 1 spark-submit
```

| Query | Nguồn → Đích | Sink | Ngữ nghĩa |
|---|---|---|---|
| Q1 Bronze | Kafka → `lake/bronze/reviews` | Parquet file sink có sẵn | **Exactly-once** |
| Q2 Silver | Kafka → clean → `lake/silver/reviews` | Parquet file sink có sẵn | **Exactly-once** |
| Q3 Serving | `lake/silver` (readStream) → sentiment → Postgres | `foreachBatch` + UPSERT | At-least-once + idempotent nên **hiệu quả như exactly-once** |

**Tại sao tách 3 query thay vì một `foreachBatch` ghi cả ba đích như `streaming_job.py`?**

Job cũ ghi Parquet rồi ghi JDBC trong cùng `foreachBatch`. Nếu job chết **giữa hai lệnh ghi**, Spark chạy lại cả batch và Parquet sẽ có **bản ghi trùng**. README đã tự nêu vấn đề này.

- ✅ **File sink có sẵn của Spark** (`writeStream.format("parquet")`) ghi log `_spark_metadata`. Batch lỗi giữa chừng sẽ không bao giờ được tính, nên đạt exactly-once mà không phải viết thêm code.
- ✅ Q3 tách riêng nên **model sentiment chậm không làm nghẽn việc ghi dữ liệu**. Bronze và silver vẫn chạy đều.
- ✅ Postgres chết chỉ dừng Q3. Data lake vẫn tiếp tục được ghi.
- ❌ Q1 và Q2 cùng đọc Kafka hai lần, tốn thêm tài nguyên. Không đáng kể ở quy mô này.
- ❌ Q3 trễ hơn vài giây vì phải chờ Q2 ghi xong file.
- ❌ Ba query cần nhiều core hơn: nâng `local[2]` lên `local[4]` và cấp RAM Docker từ 6 GB trở lên.

### 3.3. Đọc Kafka và parse

```python
raw = (spark.readStream.format("kafka")
       .option("kafka.bootstrap.servers", "kafka:19092")
       .option("subscribe", "tiki.reviews.raw")
       .option("startingOffsets", "earliest")
       .option("maxOffsetsPerTrigger", 10000)
       .option("failOnDataLoss", "false")
       .load())

parsed = raw.select(
    col("key").cast("string").alias("kafka_key"),
    col("partition"), col("offset"), col("timestamp").alias("kafka_ts"),
    col("value").cast("string").alias("json"),
).withColumn("r", from_json("json", REVIEW_SCHEMA))
```

| Tham số | Vì sao | Trade-off |
|---|---|---|
| `startingOffsets=earliest` | Lần chạy đầu đọc toàn bộ dữ liệu đang có trong topic | Chỉ có tác dụng ở lần đầu. Các lần sau Spark đọc offset từ checkpoint |
| `maxOffsetsPerTrigger=10000` | **Chống quá tải:** backfill hàng triệu dòng sẽ được chia thành các batch 10 nghìn | Nhỏ quá thì có nhiều batch nhỏ và nhiều file nhỏ. Lớn quá thì một batch có thể làm tràn RAM |
| `failOnDataLoss=false` | Message hết hạn retention trước khi Spark kịp đọc thì vẫn chạy tiếp | ❌ **Mất dữ liệu một cách im lặng.** Phải theo dõi log cảnh báo. Đặt `true` nếu muốn dừng hẳn khi có lỗi này |
| Giữ `partition`, `offset` | Truy vết được bản ghi nào đến từ message nào | Tốn thêm vài byte |

**Bản ghi lỗi (quarantine):** `from_json` trả về `null` khi JSON hỏng hoặc sai schema. Những dòng này ghi riêng vào `lake/quarantine/reviews`, **không xóa đi**, để còn điều tra.
- ✅ Không mất dữ liệu, biết được crawler đã gửi sai cái gì.
- ❌ Thêm một query hoặc một nhánh ghi.

### 3.4. Làm sạch (chuyển từ `tiki_clean.py` sang PySpark)

Đặt các hàm thuần trong `src/streaming/transforms.py` để **viết unit test được** mà không cần Kafka.

| Bước trong `tiki_clean.py` | PySpark | Ghi chú |
|---|---|---|
| Ép kiểu số | `col(c).cast("long")` | |
| `parse_ts` (unix giây) | `to_timestamp(from_unixtime("created_at"))` | API luôn trả về unix giây nên không cần đoán định dạng như bản pandas |
| `clean_text`: HTML | `regexp_replace(c, "<[^>]+>", " ")` | |
| `clean_text`: emoji | `regexp_replace(c, "[\\x{1F000}-\\x{1FFFF}\\x{2600}-\\x{27BF}]", "")` | Spark dùng regex của Java nên cú pháp `\x{...}` khác Python |
| `clean_text`: khoảng trắng | `trim(regexp_replace(c, "\\s+", " "))` | |
| `fillna` | `fillna({"content": "", "customer_name": "Ẩn danh"})` | |
| Lọc rating 1–5 | `filter(col("rating").between(1, 5))` | |
| Lọc content < 5 ký tự | ⚠️ **Không lọc ở silver.** Chỉ thêm cờ `has_content` | Xem giải thích bên dưới |
| Thêm cột | `review_date`, `review_month`, `ingest_date`, `content_len`, `sentiment_by_rating` | |

**Tại sao không xóa review không có nội dung như `tiki_clean.py`?**
- Có tới 59% review chỉ có số sao. Nếu xóa thì **thống kê rating bị sai lệch**: rating trung bình, phân bố sao, số review theo ngày đều tính thiếu hơn một nửa.
- Thay vì xóa, thêm cờ `has_content = content_len >= 5`. Bước sentiment chỉ xử lý những dòng có `has_content = true`, còn bước phân tích rating thì dùng toàn bộ.
- Nguyên tắc chung: **silver giữ đủ dữ liệu, mỗi bài toán tự lọc theo nhu cầu.**

### 3.5. Khử trùng (dedupe)

Bản ghi trùng có thể đến từ ba nguồn: producer retry, crawler cào lại, hoặc Spark chạy lại một batch.

```python
silver = (cleaned
          .withWatermark("crawled_ts", "2 hours")
          .dropDuplicatesWithinWatermark(["review_id"]))   # Spark ≥ 3.5
```

Dedupe được làm ở **ba lớp**, mỗi lớp xử lý một loại trùng:

| Lớp | Cơ chế | Bắt được | Không bắt được |
|---|---|---|---|
| Spark (Q2) | `dropDuplicatesWithinWatermark` trong cửa sổ 2 giờ | Trùng do retry hoặc gửi lặp trong thời gian ngắn | Crawler cào lại sau vài ngày |
| Postgres (Q3) | `PRIMARY KEY (review_id)` + `ON CONFLICT` | **Mọi bản trùng** | — |
| Gold (bước 6) | `row_number() over (partition by review_id order by crawled_ts desc) = 1` | Mọi bản trùng còn sót trong silver | — |

**Tại sao watermark theo `crawled_ts` mà không theo `created_at`?** Như đã nói ở mục 1.3, backfill sẽ đưa về review từ 2013, và watermark theo `created_at` sẽ vứt bỏ chúng.

**Tại sao cửa sổ là 2 giờ?**
- Spark phải **giữ trong state store** tất cả `review_id` thuộc cửa sổ này. Cửa sổ dài thì dedupe được nhiều hơn nhưng tốn RAM.
- 2 giờ bao trùm khoảng 4–8 chu kỳ crawler, đủ để bắt các lần gửi lặp. Các trường hợp còn lại đã có Postgres và gold xử lý.
- ❌ Không dedupe được hoàn toàn ở silver. Phải chấp nhận silver có thể có một ít bản trùng.

### 3.6. Trigger và checkpoint

```python
q2 = (silver.writeStream.format("parquet")
      .option("path", "/opt/demo/lake/silver/reviews")
      .option("checkpointLocation", "/opt/demo/checkpoints/silver_reviews")
      .partitionBy("review_month")
      .trigger(processingTime="1 minute")
      .start())
```

**Tại sao trigger 1 phút thay vì 5 giây như job cũ?**
- Mỗi micro-batch có dữ liệu sinh ra **ít nhất một file Parquet cho mỗi partition được ghi**. Với trigger 5 giây, một đợt cào lớn được rải ra hàng trăm batch, mỗi batch chỉ vài chục dòng. Trường hợp xấu nhất có thể lên tới khoảng 17 nghìn batch mỗi ngày. Đây là **vấn đề file nhỏ (small files problem)**: khi đọc, Spark phải mở hàng nghìn file và rất chậm.
- Crawler poll 15–30 phút một lần, nên trigger 5 giây không làm dữ liệu tươi hơn, chỉ làm dữ liệu bị chia vụn.
- ✅ Ít file, file to hơn, đọc nhanh hơn.
- ❌ Dữ liệu xuất hiện ở silver chậm tối đa 1 phút. Chấp nhận được.

**Checkpoint** lưu offset Kafka đã đọc và state dùng để dedupe. Mỗi query **bắt buộc có một thư mục checkpoint riêng**.
- ⚠️ Khi đổi logic của query theo cách không tương thích (đổi schema hoặc đổi điều kiện dedupe), phải xóa checkpoint và replay. Đây là cái giá của việc phục hồi tự động.

---

## 4. Lưu trữ — Data lake + Serving DB

### 4.1. Tại sao lưu ở hai nơi?

| | Data lake (Parquet) | Serving DB (PostgreSQL) |
|---|---|---|
| Vai trò | Lưu **toàn bộ lịch sử**, nguồn để replay và phân tích nặng | Phục vụ **truy vấn nhanh** cho dashboard và API |
| Kiểu truy vấn | Quét nhiều dòng, ít cột (aggregate) | Lấy ít dòng theo khóa, lọc, join |
| Mở rộng | Thêm file thoải mái | Bị giới hạn bởi một máy |
| Cập nhật / xóa dòng | ❌ Khó (file bất biến) | ✅ UPSERT dễ dàng |

Mỗi hệ thống làm tốt một việc. Dùng chỉ Postgres thì không mở rộng được và mất khả năng replay. Dùng chỉ Parquet thì dashboard chậm và không upsert được.

### 4.2. Data lake theo mô hình Medallion

```
lake/
├── bronze/reviews/ingest_date=2026-10-31/part-*.parquet   ← nguyên văn từ Kafka (+partition, offset)
├── silver/reviews/review_month=2026-10/part-*.parquet     ← đã làm sạch, đúng kiểu, gần như không trùng
├── quarantine/reviews/                                     ← bản ghi lỗi parse
└── gold/
    ├── product_daily_stats/                                ← tổng hợp sẵn cho phân tích
    └── brand_monthly_stats/
```

| Tầng | Lợi | Nếu bỏ tầng này thì |
|---|---|---|
| **Bronze** | Bảo hiểm: sửa logic làm sạch thì đọc lại từ bronze, **không phải cào lại Tiki** (API có thể đã đổi hoặc bị chặn) | Sai logic là mất dữ liệu gốc |
| **Silver** | Một nguồn dữ liệu sạch duy nhất cho mọi phân tích | Mỗi phân tích phải tự làm sạch lại |
| **Gold** | Kết quả tổng hợp sẵn, dashboard đọc nhanh | Mỗi lần xem dashboard phải tính lại |

❌ Cái giá: dữ liệu được lưu khoảng 2–3 lần. Với Parquet nén, 20 nghìn review chỉ cỡ vài MB, nên chi phí không đáng kể.

### 4.3. Chiến lược phân vùng (partitionBy)

| Tầng | Partition theo | Vì sao |
|---|---|---|
| Bronze | `ingest_date` (ngày cào) | Truy vấn thường gặp ở bronze là "replay dữ liệu cào ngày X" |
| Silver | `review_month` (tháng viết review) | Phân tích thường lọc theo **thời gian viết review**. Nếu chia theo ngày thì với dữ liệu từ 2013 sẽ có khoảng 5 nghìn thư mục, mỗi thư mục chỉ vài dòng, lại thành vấn đề file nhỏ |

Không partition theo `product_id`: có hàng trăm đến hàng nghìn giá trị nên sinh quá nhiều thư mục. Lọc theo sản phẩm đã có Postgres với index đảm nhận.

### 4.4. Gom file nhỏ (compaction)

Dù trigger là 1 phút, sau vài tuần vẫn có hàng nghìn file nhỏ. Viết job `compact.py` chạy hằng ngày: đọc partition của tháng trước, `coalesce(1)`, rồi ghi đè.

**Phương án khác: Delta Lake hoặc Apache Iceberg.**
- ✅ Có lệnh `OPTIMIZE`, giao dịch ACID, `MERGE` (upsert ngay trên data lake) và time travel.
- ❌ Thêm thư viện và cấu hình, tăng độ phức tạp.
- Gợi ý: dùng Parquet thuần trước. Nếu còn thời gian, nâng cấp silver lên Delta Lake (`io.delta:delta-spark_2.12:3.x`) như một điểm cộng trong báo cáo.

### 4.5. PostgreSQL — schema và upsert

```sql
CREATE TABLE IF NOT EXISTS tiki_reviews (
    review_id           BIGINT PRIMARY KEY,
    product_id          BIGINT NOT NULL,
    rating              SMALLINT NOT NULL CHECK (rating BETWEEN 1 AND 5),
    content             TEXT,
    has_content         BOOLEAN,
    created_at          TIMESTAMP NOT NULL,
    purchased_at        TIMESTAMP,
    customer_hash       TEXT,
    sentiment_by_rating TEXT,
    predicted_sentiment TEXT,         -- NULL nếu không có content
    sentiment_score     REAL,
    is_mismatch         BOOLEAN,
    crawled_at          TIMESTAMP,
    updated_at          TIMESTAMP DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_tr_product_created ON tiki_reviews (product_id, created_at);

CREATE TABLE IF NOT EXISTS tiki_products (
    product_id BIGINT, name TEXT, brand_name TEXT,
    price BIGINT, list_price BIGINT, discount_rate INT,
    crawled_at TIMESTAMP,
    PRIMARY KEY (product_id, crawled_at)       -- giữ lịch sử giá
);
```

**Upsert trong `foreachBatch` (Q3):** JDBC writer của Spark chỉ hỗ trợ `append` và `overwrite`, không có upsert. Vì vậy ghi vào bảng tạm (staging) rồi `MERGE` sang bảng chính:

```python
def upsert_batch(df, batch_id):
    (df.write.mode("overwrite").option("truncate", "true")
       .jdbc(JDBC_URL, "tiki_reviews_staging", properties=PROPS))
    run_sql("""
        INSERT INTO tiki_reviews AS t SELECT * FROM tiki_reviews_staging
        ON CONFLICT (review_id) DO UPDATE SET
            content = EXCLUDED.content, rating = EXCLUDED.rating,
            predicted_sentiment = EXCLUDED.predicted_sentiment,
            sentiment_score = EXCLUDED.sentiment_score,
            is_mismatch = EXCLUDED.is_mismatch, updated_at = now()
    """)

def run_sql(sql):
    jvm = spark._jvm
    jvm.java.lang.Class.forName("org.postgresql.Driver")
    conn = jvm.java.sql.DriverManager.getConnection(JDBC_URL, USER, PASSWORD)
    try:
        conn.createStatement().execute(sql)
    finally:
        conn.close()
```

| Lựa chọn | Lợi | Hại |
|---|---|---|
| `DO UPDATE` (chọn) | Người dùng sửa review trên Tiki thì Postgres cập nhật theo | Ghi nhiều hơn `DO NOTHING` |
| `DO NOTHING` | Nhanh nhất | Bỏ qua mọi thay đổi sau lần ghi đầu |
| Staging + MERGE | Idempotent: Spark chạy lại batch thì kết quả **vẫn như cũ** | Ghi hai lần (staging rồi bảng chính) |
| Gọi SQL qua JVM | Không cần cài `psycopg2` vào container Spark | Code hơi khó đọc. Có thể thay bằng `psycopg2` nếu chấp nhận cài thêm |

---

## 5. Gán sentiment

Kết luận từ việc phân tích `tiki_reviews_clean.csv` (7.581 dòng: 85% positive, 9% negative, 5,5% neutral; nhãn suy từ số sao bị nhiễu): **không tự huấn luyện model, chỉ dùng base model tiếng Việt đã huấn luyện sẵn để dự đoán**.

### 5.1. Hai cột sentiment

| Cột | Cách tính | Áp dụng cho |
|---|---|---|
| `sentiment_by_rating` | 1–2 → negative, 3 → neutral, 4–5 → positive | **Mọi review**, kể cả review không có nội dung |
| `predicted_sentiment` | Base model chạy trên `content` | Chỉ review có `has_content = true` (khoảng 41%) |
| `is_mismatch` | Hai cột trên khác nhau và `sentiment_score` cao | Đánh dấu review đáng chú ý, ví dụ 5 sao nhưng nội dung chê |

### 5.2. Đặt model ở đâu? Trong Q3, không đặt trong Q2

```python
@pandas_udf("struct<label:string,score:float>")
def predict(texts: pd.Series) -> pd.DataFrame:
    model = get_model()            # load 1 lần mỗi executor (cache global), KHÔNG load mỗi batch
    return pd.DataFrame(model(texts.tolist(), batch_size=32))
```

| Lựa chọn | Lợi | Hại |
|---|---|---|
| **Trong Q3** (chọn) | Model chậm không làm nghẽn bronze và silver | Postgres có predicted_sentiment trễ hơn vài phút |
| Trong Q2 | Một bước duy nhất | Model chậm hoặc lỗi thì **dừng luôn việc ghi data lake** |
| Service Python riêng (FastAPI) | Tách hẳn khỏi Spark, mở rộng độc lập | Thêm một service, thêm network call, phức tạp hơn |

**Lưu ý về tài nguyên:**
- Cần cài `torch`, `transformers` (và tách từ `underthesea`/`pyvi` nếu model là PhoBERT) vào image Spark. Nên viết `Dockerfile` riêng kế thừa `apache/spark:3.5.5`.
- Chạy trên CPU thì chậm (ước lượng vài chục câu mỗi giây). Với khoảng 8 nghìn review có nội dung thì chạy được. Nếu khối lượng lên hàng triệu dòng, xem lại phương án chưng cất (distillation) đã bàn.
- Đặt `maxFilesPerTrigger` cho readStream của Q3 để mỗi batch không quá nặng.

### 5.3. Đánh giá

- Gán nhãn thủ công khoảng 300 review "khó" (1–3 sao và một ít 5 sao) làm tập test cố định, lưu ở `data/eval/`.
- Báo cáo **F1 macro** của base model trên tập này. Không dùng accuracy vì dữ liệu lệch 85% positive.

---

## 6. Phân tích & hiển thị

### 6.1. Phân tích ở đâu? Chia theo độ nặng

| Loại | Công cụ | Ví dụ | Vì sao |
|---|---|---|---|
| Gần realtime, nhẹ | **SQL view trong Postgres** | Review mới trong 24h, cảnh báo negative tăng | Dữ liệu đã ở Postgres, có index, view luôn mới |
| Nặng, lịch sử dài | **Spark batch** đọc silver, ghi gold | Xu hướng rating theo tháng qua 13 năm, so sánh brand | Quét toàn bộ dữ liệu, Spark xử lý tốt hơn Postgres |
| Stateful streaming | Spark `groupBy(window(...))` | Số review mỗi giờ theo sản phẩm | ⚠️ **Không khuyến nghị lúc đầu**, xem bên dưới |

**Tại sao không làm aggregate bằng streaming ngay từ đầu?**
- Aggregate có trạng thái cần watermark theo **event time** (`created_at`), nhưng backfill có review từ 2013 nên phần lớn sẽ bị loại.
- Mỗi lần đổi logic tổng hợp lại phải xóa checkpoint.
- SQL view và batch job đơn giản, đúng, và đủ nhanh ở quy mô này. Khi cần chứng minh "window aggregation" trong báo cáo thì làm thêm một query riêng, đặt watermark theo `crawled_ts`.

### 6.2. Các chỉ số nên có

**Theo sản phẩm / brand**
- Số review, rating trung bình, phân bố 1–5 sao, theo ngày/tuần/tháng.
- Tỉ lệ negative (theo cả rating và model) và xu hướng.
- Top sản phẩm bị chê nhiều nhất trong 7 ngày.

**Chất lượng review**
- Tỉ lệ review có nội dung, độ dài trung bình.
- Tỉ lệ `is_mismatch`, kèm danh sách review đáng ngờ.
- Tỉ lệ review từ người "đã mua" (`purchased`) so với chưa mua.

**Liên kết với giá** (dùng `tiki_products`)
- Rating và số review mới thay đổi thế nào trước và sau khi giảm giá.
- Kết hợp với các view `price_changes`, `price_alerts` đã có trong `app/price_analytics.sql`.

**Sức khỏe pipeline**
- Độ trễ `now() - crawled_at` và `crawled_at - created_at`.
- Số bản ghi trong quarantine mỗi ngày.

Ví dụ view:
```sql
CREATE OR REPLACE VIEW product_negative_spike AS
WITH daily AS (
  SELECT product_id, created_at::date AS d,
         COUNT(*) AS n,
         AVG((sentiment_by_rating = 'negative')::int) AS neg_ratio
  FROM tiki_reviews GROUP BY 1, 2
)
SELECT *, neg_ratio - AVG(neg_ratio) OVER (
          PARTITION BY product_id ORDER BY d ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING) AS delta_vs_7d
FROM daily WHERE n >= 5;
```

### 6.3. Dashboard

| | **Metabase** (chọn) | Apache Superset | Grafana | Tự viết (Streamlit) |
|---|---|---|---|---|
| Cài đặt | 1 container, vài phút | Nặng, nhiều bước | Nhẹ | Phải viết code |
| Phù hợp | Biểu đồ nghiệp vụ trên SQL | Như Metabase, mạnh hơn | Time-series, giám sát | Tùy biến hoàn toàn |
| Tự refresh | Có | Có | ✅ Rất tốt | Tự làm |

Metabase kết nối thẳng vào Postgres, kéo thả là có biểu đồ, nên tốn ít công nhất. Nếu muốn nhấn mạnh tính "realtime" khi demo thì Grafana với auto-refresh 10 giây trực quan hơn.

---

## 7. Vận hành, lỗi và phục hồi

| Sự cố | Điều gì xảy ra | Phục hồi |
|---|---|---|
| Crawler bị chặn (429/403) | Không có dữ liệu mới, pipeline vẫn chạy | Retry với backoff. Giảm số luồng, tăng delay |
| Crawler chết giữa chu kỳ | State chưa được lưu | Lần sau cào lại một phần, **trùng nhưng an toàn** nhờ dedupe |
| Kafka chết | Crawler gửi lỗi | Crawler retry. ⚠️ Message chưa ghi xuống đĩa có thể mất (RF=1) |
| Spark chết | Kafka giữ message | Chạy lại `spark-submit`, đọc tiếp từ checkpoint |
| Postgres chết | Q3 lỗi. Q1, Q2 vẫn chạy | Bật lại Postgres, khởi động lại Q3. Q3 đọc tiếp silver từ checkpoint |
| Sửa logic làm sạch | Silver cũ sai | Xóa `lake/silver`, xóa checkpoint Q2, **replay từ bronze** bằng batch job |
| JSON sai schema | Vào quarantine | Sửa crawler hoặc schema, xử lý lại phần quarantine |

**Giám sát tối thiểu**
- `query.lastProgress` trong log Spark: `numInputRows`, `inputRowsPerSecond`, `processedRowsPerSecond`. Nếu processed thấp hơn input trong thời gian dài thì pipeline đang bị dồn.
- Consumer lag: Spark tự quản lý offset trong checkpoint, không commit về Kafka, nên `kafka-consumer-groups.sh` sẽ không thấy lag. Phải so `endOffset` trong `lastProgress` với offset mới nhất của topic.
- Spark UI ở `localhost:4040`: cần thêm `ports: ["4040:4040"]` cho service `spark` trong `docker-compose.yml`.

---

## 8. Lộ trình triển khai

Mỗi bước đều chạy được và demo được trước khi sang bước tiếp theo.

| # | Việc | Kết quả kiểm chứng |
|---|---|---|
| 1 | Tái cấu trúc thư mục (`src/`, `sql/`, `data/`, `docs/`), sửa mount trong `docker-compose.yml` | Job cũ vẫn chạy được |
| 2 | Crawler: `KafkaSink`, sort `id\|desc`, watermark theo sản phẩm, `crawled_at`, hash `customer_id` | `kafka-console-consumer` thấy message JSON đúng định dạng |
| 3 | Q1 Bronze | Có file trong `lake/bronze/`. Tắt rồi bật Spark không bị trùng |
| 4 | Q2 Silver: parse, quarantine, clean, dedupe + unit test cho `transforms.py` | Số dòng silver ≤ bronze, schema đúng kiểu |
| 5 | Q3 Serving: upsert Postgres (chưa có model) | Gửi lại cùng dữ liệu thì `COUNT(*)` **không đổi** |
| 6 | SQL views + Metabase | Dashboard có số liệu |
| 7 | Base model sentiment trong Q3 + đánh giá trên tập gán tay | Có F1 macro, có danh sách `is_mismatch` |
| 8 | Spark batch: gold + compaction | Bảng gold, số file giảm |
| 9 | (Tùy chọn) Delta Lake, window aggregation, Grafana | Điểm cộng trong báo cáo |

---

## 9. Bảng tổng hợp trade-off

| Quyết định | Chọn | Được | Mất / Chấp nhận |
|---|---|---|---|
| Kiến trúc | Kappa | Một bộ logic, đơn giản | Tính lại lịch sử phải replay |
| Thu thập | Polling tăng dần theo `review_id` | Nhẹ, ít request | Trễ cỡ phút, API không chính thức nên có thể bị đổi hoặc chặn |
| Message | JSON thô + `schema_version` | Dễ debug, không thêm hạ tầng | Không kiểm soát schema chặt như Avro |
| Kafka key | `product_id` | Giữ thứ tự theo sản phẩm | Lệch tải partition |
| Replication | RF=1 | Chạy được trên 1 máy | Mất dữ liệu nếu broker hỏng (chỉ hợp với demo) |
| Giao nhận | At-least-once + dedupe nhiều lớp | Đơn giản, không mất dữ liệu | Phải dedupe ở downstream |
| Xử lý | Spark SS micro-batch | Batch và stream cùng một API, có checkpoint | Trễ cỡ giây (không quan trọng ở đây) |
| Trigger | 1 phút | Ít file nhỏ | Trễ thêm tối đa 1 phút |
| Tách query | Q1/Q2 file sink, Q3 upsert | Exactly-once ở lake, model không chặn ingest | Đọc Kafka 2 lần, cần nhiều core hơn |
| Watermark | `crawled_ts` | Không làm rơi dữ liệu backfill cũ | Không dùng được cho tổng hợp theo event time |
| Review không có nội dung | Giữ lại, gắn cờ `has_content` | Thống kê rating đúng | Bước sentiment phải tự lọc |
| Lưu trữ | Parquet medallion + Postgres | Replay, phân tích nặng, truy vấn nhanh | Lưu dữ liệu 2–3 lần |
| Partition silver | `review_month` | Tránh hàng nghìn thư mục nhỏ | Lọc theo ngày phải đọc cả tháng |
| Upsert | Staging + `ON CONFLICT DO UPDATE` | Idempotent, cập nhật được review bị sửa | Ghi 2 lần |
| Sentiment | Base model trong Q3 | Không cần huấn luyện, không chặn ingest | Chậm trên CPU, image Spark nặng hơn |
| Phân tích | SQL view + Spark batch | Đơn giản, đúng | Không "realtime" bằng stateful streaming |
| Dashboard | Metabase | Cài nhanh, không phải viết code | Ít tùy biến hơn tự viết |
