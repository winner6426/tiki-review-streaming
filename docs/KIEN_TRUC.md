# Lựa chọn kiến trúc: vì sao Kafka + Spark + Delta Lake

Tài liệu này giải thích vì sao đề tài dùng kiến trúc hiện tại, và so sánh nó với các kiến trúc, công nghệ khác về điểm mạnh và điểm yếu.

---

## 1. Bài toán và các ràng buộc

Đề tài cần xây một **hệ thống phân tán xử lý luồng review sản phẩm Tiki**: review đến liên tục, được làm sạch, loại trùng, gán sentiment, rồi phục vụ cho dashboard gần thời gian thực.

Các ràng buộc thực tế quyết định kiến trúc:

| Ràng buộc | Số liệu | Hệ quả |
|---|---|---|
| **Nguồn thật quá thưa** | 87 sản phẩm chỉ có khoảng 1 review mới mỗi ngày (xem `NOTE.md`) | Không thể làm streaming trên dữ liệu thật, phải **phát lại** (replay) dữ liệu lịch sử |
| **API bị giới hạn** | WAF chặn khi vượt khoảng 300 request/phút | Không thể gọi API theo thời gian thực ở tốc độ cao. Dữ liệu phải được cào trước (backfill) |
| **Khối lượng đã cào** | 2.436.108 review, khoảng 204 nghìn sản phẩm | Đủ để phát liên tục khoảng 2 giờ 15 phút ở 300 message/giây cho mỗi vòng |
| **Phần cứng** | 1 máy, 7,8 GB RAM, 8 luồng CPU | Mọi thành phần phải chạy được trong khoảng 4–5 GB RAM dành cho Docker |
| **Yêu cầu môn học** | Big data, xử lý phân tán, streaming | Phải có thành phần phân tán thật và chứng minh được khả năng mở rộng |

---

## 2. Kiến trúc đã chọn

```
           NGUỒN (giả lập hệ thống Tiki)              │                PIPELINE XỬ LÝ
                                                     │
Tiki API ─▶ backfill ─▶ raw/ ─▶ prepare ─▶ replay/ ─▶ replay producer ─▶ KAFKA ─▶ SPARK STRUCTURED STREAMING
            (1 lần)    JSON     (1 lần)   Parquet    300 msg/s          6 partition    │
                                                                                      ├─▶ bronze     (message gốc)
                                                                                      ├─▶ silver     (làm sạch, MERGE loại trùng)
                                                                                      ├─▶ quarantine (message lỗi)
                                                                                      └─▶ Postgres   (phục vụ dashboard)
                                                                                                  DELTA LAKE
```

Kiến trúc này là sự kết hợp của 3 mẫu thiết kế phổ biến:
- **Kappa**: chỉ có một đường xử lý dạng luồng. Muốn xử lý lại dữ liệu cũ thì phát lại luồng chứ không viết riêng một đường batch.
- **Medallion** (bronze / silver / gold): dữ liệu đi qua các tầng, mỗi tầng sạch hơn tầng trước.
- **Replay làm nguồn**: dữ liệu lịch sử được phát lại với tốc độ điều chỉnh được, đóng vai một hệ thống sinh sự kiện thật.

---

## 3. Lý do chọn từng thành phần

### Kafka: tầng thu nhận

- **Tách nguồn khỏi bên xử lý.** Producer không cần biết Spark có đang chạy hay không. Spark sập thì message vẫn nằm trong Kafka, khởi động lại thì đọc tiếp đúng chỗ.
- **Phân tán theo partition.** Topic có 6 partition với key là `product_id`: review của cùng một sản phẩm giữ đúng thứ tự, và 6 task Spark đọc song song được.
- **Phát lại được.** Kafka giữ message 3 ngày, nên có thể đọc lại từ đầu để xử lý lại mà không cần chạy lại producer.

### Spark Structured Streaming: tầng xử lý

- **Một engine cho cả streaming lẫn batch.** Cùng một API DataFrame dùng cho silver (streaming) và gold (batch).
- **Exactly-once khi ghi vào Delta**, nhờ checkpoint cộng với ghi giao dịch.
- **Mở rộng theo chiều ngang.** Thêm worker bằng `docker compose up --scale spark-worker=N`. Đây là cơ sở để đo benchmark throughput theo số worker.
- **Có sẵn connector** cho Kafka, Delta và JDBC/Postgres, viết bằng Python.

### Delta Lake: tầng lưu trữ

- **`MERGE`** để loại trùng theo `review_id` **xuyên các batch**. Parquet thuần không làm được việc này.
- **ACID**: Spark sập giữa chừng thì không để lại file ghi dở.
- **Change Data Feed**: tầng serving chỉ đẩy sang Postgres những dòng mới hoặc vừa sửa, không phải đọc lại cả bảng.
- **`OPTIMIZE` / `VACUUM`**: gom các file nhỏ do micro-batch sinh ra.
- **Time travel**: xem lại bảng ở một phiên bản cũ, hữu ích khi gỡ lỗi.

### Postgres: tầng phục vụ

Dashboard cần truy vấn nhanh, có chỉ mục và đồng thời nhiều người dùng. Delta tối ưu cho quét lớn, không tối ưu cho truy vấn điểm. Vì vậy dữ liệu đã sạch được đẩy sang Postgres bằng upsert (`ON CONFLICT`).

### Replay thay cho crawl thời gian thực

- Nguồn thật quá thưa và bị giới hạn request, nên không tạo được luồng đủ lớn.
- Replay cho phép **điều chỉnh tốc độ** (`--rate 300`, `1000`, `5000`) để kiểm tra giới hạn của hệ thống.
- Dữ liệu vẫn là **review thật**, chỉ có thời điểm đến pipeline là giả lập. `created_at` gốc được giữ nguyên, `crawled_at` là thời điểm message đến.

---

## 4. So sánh với các kiến trúc khác

### 4.1. Kappa (đã chọn) và Lambda

| | **Kappa** | Lambda |
|---|---|---|
| Cấu trúc | Một đường xử lý luồng | Hai đường song song: batch (chính xác) và speed (nhanh), gộp ở tầng phục vụ |
| Xử lý lại dữ liệu cũ | Phát lại luồng qua cùng một code | Chạy lại đường batch |
| Số bộ code | **1** | **2**, cùng một logic phải viết hai lần và giữ cho khớp nhau |
| Phù hợp khi | Engine streaming đủ mạnh để xử lý lại toàn bộ lịch sử | Phép tính batch quá nặng, không xử lý dạng luồng được |

Chọn Kappa vì logic làm sạch và loại trùng không quá nặng để xử lý dạng luồng, và Delta `MERGE` đảm bảo tính đúng mà không cần đường batch riêng. Gold vẫn là batch, nhưng nó đọc từ silver chứ không phải một đường song song thứ hai.

### 4.2. Pipeline batch đơn giản (cron + pandas + Postgres)

| | **Kiến trúc đã chọn** | Batch đơn giản |
|---|---|---|
| Độ trễ | Khoảng 1 phút (trigger) | Theo chu kỳ cron, thường vài giờ |
| Mở rộng | Thêm worker Spark, thêm partition Kafka | Giới hạn bởi RAM của 1 máy (pandas nạp hết vào bộ nhớ) |
| Chịu lỗi | Checkpoint, Kafka giữ dữ liệu | Job lỗi thì chạy lại từ đầu |
| Độ phức tạp | Cao: 5 dịch vụ Docker | Thấp: vài script Python |
| Đáp ứng yêu cầu big data / streaming | Có | Không |

Với 2,4 triệu review, một máy chạy pandas vẫn xử lý được. Nhưng nó không đáp ứng yêu cầu streaming và phân tán của đề tài, và không mở rộng được khi dữ liệu tăng.

### 4.3. Hadoop (HDFS + YARN + MapReduce)

Kiến trúc đã chọn không loại bỏ Hadoop mà **thay từng thành phần** của nó:

| Thành phần Hadoop | Thay bằng | Lý do |
|---|---|---|
| MapReduce | Spark | MapReduce chỉ chạy batch và ghi dữ liệu trung gian ra đĩa sau mỗi bước. Spark xử lý được cả luồng và giữ dữ liệu trong RAM |
| HDFS | Delta Lake trên volume Docker | HDFS cần thêm NameNode và DataNode (khoảng 1 GB RAM mỗi tiến trình), không có `MERGE` hay ACID, và xử lý kém khi có nhiều file nhỏ |
| YARN | Spark standalone | Đủ cho 1 cluster chỉ chạy Spark và nhẹ hơn |
| *(không có)* | Kafka | Hadoop không có tầng thu nhận dữ liệu liên tục |

Nếu cần HDFS, chỉ phải đổi biến `LAKE` trong `src/common/config.py` thành `hdfs://namenode:9000/lake` và thêm HDFS vào `docker-compose.yml`. Code xử lý giữ nguyên.

### 4.4. Engine xử lý luồng: Spark, Flink, Kafka Streams

| | **Spark Structured Streaming** | Apache Flink | Kafka Streams / ksqlDB |
|---|---|---|---|
| Mô hình | Micro-batch | Xử lý từng sự kiện | Xử lý từng sự kiện, là thư viện chạy trong ứng dụng |
| Độ trễ | Từ khoảng 1 giây tới vài phút | Mili-giây | Mili-giây |
| Batch trên cùng engine | Có | Có | Không |
| Ghi Delta Lake | Hỗ trợ đầy đủ, kể cả `MERGE` | Có connector nhưng hạn chế hơn | Không trực tiếp |
| Python | PySpark rất phổ biến | PyFlink còn ít tài liệu | Chỉ Java/Scala (ksqlDB thì dùng SQL) |
| Độ phổ biến trong học tập | Rất cao | Trung bình | Trung bình |

Chọn Spark vì đề tài không cần độ trễ mili-giây (dashboard refresh mỗi phút là đủ), cần `MERGE` vào Delta, và cần một engine cho cả streaming lẫn batch. Flink là lựa chọn tốt hơn nếu yêu cầu độ trễ dưới 1 giây, ví dụ phát hiện gian lận.

### 4.5. Định dạng bảng: Parquet thuần, Delta, Iceberg, Hudi

| | Parquet thuần | **Delta Lake** | Apache Iceberg | Apache Hudi |
|---|---|---|---|---|
| ACID, `MERGE` | Không | Có | Có | Có (tối ưu cho upsert) |
| Change Data Feed | Không | Có | Có (incremental read) | Có |
| Tích hợp với Spark | Có sẵn | Rất tốt, chỉ cần thêm package | Tốt | Tốt nhưng cấu hình phức tạp hơn |
| Đa engine (Trino, Flink, …) | Tốt | Khá | **Tốt nhất** | Khá |

Parquet thuần buộc phải tự viết logic loại trùng và gom file nhỏ. Delta giải quyết cả hai bằng `MERGE` và `OPTIMIZE`, và là định dạng dễ tích hợp nhất với Spark. Iceberg phù hợp hơn nếu nhiều engine khác nhau cùng đọc một bảng.

### 4.6. Ghi thẳng vào Postgres, không có data lake

| | **Kafka → Delta → Postgres** | Kafka → Postgres |
|---|---|---|
| Lưu dữ liệu gốc | Có (bronze) | Không, chỉ có dữ liệu đã xử lý |
| Sửa logic rồi xử lý lại | Chạy lại silver từ bronze | Phải phát lại Kafka, mà Kafka chỉ giữ 3 ngày |
| Phân tích quét toàn bộ (gold) | Spark quét Delta, chạy phân tán | Postgres quét bảng lớn, chậm và ảnh hưởng dashboard |
| Độ phức tạp | Cao hơn | Thấp hơn |

### 4.7. Dịch vụ cloud có quản lý (Kinesis, Databricks, BigQuery, …)

Không dùng vì có chi phí, cần tài khoản cloud, và che đi phần hạ tầng mà đề tài cần thể hiện. Ngược lại, kiến trúc hiện tại chuyển lên cloud được mà không phải viết lại: Kafka thành MSK hoặc Confluent Cloud, Spark và Delta thành Databricks, volume thành S3.

---

## 5. Điểm mạnh

| Điểm mạnh | Nhờ đâu |
|---|---|
| **Mở rộng theo chiều ngang** | Kafka chia partition, thêm worker Spark |
| **Chịu lỗi, không mất dữ liệu** | Kafka giữ message, checkpoint của Spark, giao dịch ACID của Delta |
| **Không trùng dữ liệu** | `MERGE` theo `review_id` ở silver, dù message bị gửi lặp hay replay nhiều vòng |
| **Xử lý lại được** | Bronze giữ message gốc. Sửa logic thì chạy lại silver từ bronze |
| **Tách biệt các tầng** | Hợp đồng dữ liệu ở `schemas.py` dùng chung cho producer và Spark. Đổi nguồn (CSV, crawler) không ảnh hưởng tầng sau |
| **Kiểm thử được tải** | Replay với `--rate` điều chỉnh được, dễ đo throughput và độ trễ |
| **Bảo vệ dữ liệu cá nhân** | Tên khách hàng bị bỏ và `customer_id` được hash ngay từ trước Kafka |
| **Gần với thực tế ngành** | Kafka, Spark và Delta là bộ công nghệ phổ biến trong các hệ thống lakehouse |

---

## 6. Điểm yếu và cách giảm thiểu

| Điểm yếu | Ảnh hưởng | Cách giảm thiểu |
|---|---|---|
| **Độ trễ micro-batch** | Dữ liệu có mặt sau khoảng 1 phút, không phải tức thì | Đủ cho dashboard. Có thể giảm `TRIGGER` xuống vài giây, đổi lại sinh nhiều file nhỏ hơn |
| **Phức tạp** | 5 dịch vụ Docker, nhiều cấu hình, khó gỡ lỗi hơn một script | Một file `docker-compose.yml`, cấu hình gom ở `config.py`, có `smoke_test.py` kiểm tra hạ tầng |
| **Tốn tài nguyên trên 1 máy** | Kafka, Spark và Postgres cần khoảng 4 GB RAM | Giới hạn heap Kafka 512 MB, 1 worker Spark 2 GB. Thêm worker khi đo benchmark |
| **Không phân tán thật** | Mọi container chạy trên cùng 1 máy vật lý, nên benchmark không phản ánh đúng một cluster nhiều máy | Ghi rõ trong báo cáo. Kiến trúc chạy được trên nhiều máy mà không phải sửa code |
| **Không có dự phòng** | Kafka chỉ có 1 broker, replication factor 1. Broker hỏng thì mất message chưa xử lý | Chấp nhận được ở môi trường phát triển. Môi trường thật cần ít nhất 3 broker |
| **Dữ liệu không thật sự "thời gian thực"** | Review là dữ liệu lịch sử được phát lại | Ghi rõ trong báo cáo. Nội dung review vẫn là dữ liệu thật |
| **Nhiều file nhỏ** | Mỗi micro-batch sinh file mới, làm chậm các lần đọc sau | `OPTIMIZE` định kỳ trong `batch/maintenance.py` |
| **Hợp đồng dữ liệu bằng JSON** | Không có kiểm tra schema tự động ở phía Kafka | `schemas.py` dùng chung cho cả hai bên, có trường `schema_version`, message sai schema vào quarantine. Schema Registry + Avro là bước nâng cấp nếu số topic tăng |
| **Dữ liệu bị nhân bản** | Cùng một review nằm ở Kafka, bronze, silver và Postgres | Kafka chỉ giữ 3 ngày, bronze có thể `VACUUM`, Postgres chỉ chứa các cột cần cho dashboard |

---

## 7. Khi nào nên chọn kiến trúc khác

| Tình huống | Nên chọn |
|---|---|
| Cần độ trễ dưới 1 giây (cảnh báo, phát hiện gian lận) | Flink hoặc Kafka Streams |
| Dữ liệu nhỏ, chỉ cần báo cáo theo ngày | Cron + pandas + Postgres |
| Nhiều engine (Trino, Flink, Spark) cùng đọc một bảng | Iceberg thay cho Delta |
| Phép tính lịch sử quá nặng để xử lý dạng luồng | Lambda |
| Không muốn tự vận hành hạ tầng | Dịch vụ cloud có quản lý (Databricks, Confluent Cloud, …) |
