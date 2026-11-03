# Hướng dẫn chạy crawler

Review mới trên Tiki rất thưa: khoảng 1 review/ngày cho 87 sản phẩm (xem [NOTE.md](NOTE.md)). Vì vậy đề tài **cào lịch sử review trước**, lưu lại thành kho nguồn, rồi **replay** vào Kafka với tốc độ tùy chỉnh. Crawler này làm phần đầu tiên: cào và lưu.

```
Tiki API ──(cào chậm)──▶ data/source/raw/ ──▶ prepare ──▶ replay producer ──▶ Kafka ──▶ ...
         └──── crawler ────┘
```

Mục lục:

1. [Chuẩn bị](#1-chuẩn-bị)
2. [Cài đặt](#2-cài-đặt)
3. [Chạy crawler: 3 bước](#3-chạy-crawler-3-bước)
4. [Tốc độ và việc bị chặn](#4-tốc-độ-và-việc-bị-chặn)
5. [Dừng, chạy tiếp, theo dõi](#5-dừng-chạy-tiếp-theo-dõi)
6. [Kết quả](#7-kết-quả)
7. [Các tùy chọn](#8-các-tùy-chọn)
8. [Lỗi thường gặp](#9-lỗi-thường-gặp)

---

## 1. Chuẩn bị

- **Python 3.12 trở lên.** Kiểm tra bằng `python --version`.
- **Git.**
- **Mạng ổn định**, và **máy không được ngủ** trong lúc cào. Trên Windows: Settings → System → Power → Sleep: *Never*.
- Khoảng **1–2 GB ổ đĩa trống** cho dữ liệu nén.

---

## 2. Cài đặt

### 2.1. Lấy code

```bash
git clone <url-repo> tiki-review-streaming
cd tiki-review-streaming
```

**Mọi lệnh bên dưới đều chạy ở thư mục gốc của repo** (thư mục có `src/` và `requirements.txt`).

### 2.2. Tạo môi trường ảo và cài thư viện

```bash
python -m venv .venv
```

Kích hoạt môi trường ảo. Lệnh khác nhau tùy terminal:

| Terminal | Lệnh |
|---|---|
| PowerShell (Windows) | `.venv\Scripts\Activate.ps1` |
| cmd (Windows) | `.venv\Scripts\activate.bat` |
| Git Bash (Windows) | `source .venv/Scripts/activate` |
| macOS / Linux | `source .venv/bin/activate` |

Kích hoạt thành công thì đầu dòng lệnh có chữ `(.venv)`. Sau đó cài thư viện:

```bash
pip install -r requirements.txt
```

**Mỗi lần mở terminal mới đều phải kích hoạt lại** môi trường ảo, trước khi chạy các lệnh ở dưới.

### 2.3. Kiểm tra

```bash
python -m src.ingestion.backfill --help
```

In ra danh sách tùy chọn là cài đặt đã xong.

---

## 3. Chạy crawler: 3 bước

Chạy **đúng thứ tự**, vì bước sau dùng kết quả của bước trước:

| # | Lệnh | Làm gì | Thời gian ước tính |
|---|---|---|---|
| 1 | `categories` | Đi cây danh mục Tiki (26 danh mục gốc, ~3.800 danh mục con), chọn ~700 danh mục để liệt kê sản phẩm | ~1 phút |
| 2 | `products` | Liệt kê sản phẩm của từng danh mục (~394 nghìn sản phẩm) | ~3–4 giờ |
| 3 | `reviews` | Cào toàn bộ review của mọi sản phẩm có review, **sản phẩm nhiều review trước** | Nhiều giờ, có thể 1–2 ngày |

### Bước 1: Cây danh mục

```bash
python -m src.ingestion.backfill categories
```

Bước này tự tạo thư mục `data/source/raw/` và file `categories.json`. Kết thúc đúng sẽ có dòng tương tự:

```
INFO    Xong: thấy ... danh mục, chọn ~700 danh mục để liệt kê, ~394000 sản phẩm theo product_count ...
```

Muốn xem toàn bộ cây danh mục tới tận lá, thêm `--full-tree`. Khi đó bước này mất khoảng 10 phút, rồi mở file `data/source/raw/categories_tree.md` để xem.

### Bước 2: Sản phẩm

```bash
python -m src.ingestion.backfill products --rate 1.2
```

**Bắt buộc dùng `--rate 1.2`**: endpoint liệt kê sản phẩm bị chặn ngay ở tốc độ mặc định (xem [mục 4](#4-tốc-độ-và-việc-bị-chặn)). Mỗi danh mục xong sẽ thành một file `data/source/raw/products/category=<id>.jsonl.gz`.

Xong bước này, xem còn bao nhiêu review cần cào và mất bao lâu:

```bash
python -m src.ingestion.backfill status
```

### Bước 3: Review

```bash
python -m src.ingestion.backfill reviews
```

Bước này lâu nhất. Mỗi sản phẩm xong sẽ thành một file `data/source/raw/reviews/<product_id>.jsonl.gz`. Vì chương trình **cào sản phẩm nhiều review trước**, vài giờ đầu thu được nhiều dữ liệu nhất (đến khoảng 150 nghìn review/giờ), về sau chậm dần.

Muốn có dữ liệu sớm để thử pipeline, có thể dừng ở một số lượng nhất định:

```bash
python -m src.ingestion.backfill reviews --target-reviews 200000
```

Sau đó muốn cào tiếp phần còn lại thì chạy lại lệnh `reviews`, không cần `--target-reviews`.

---

## 4. Tốc độ và việc bị chặn

Tiki chặn IP khi gửi quá nhanh, khoảng **300 request/phút**, endpoint liệt kê sản phẩm còn chặt hơn. Khi bị chặn, API vẫn trả HTTP 200 nhưng kèm trang HTML thay vì JSON, và chặn **cả IP khoảng 6–7 phút**.

Mức an toàn đã đo:

| Bước | `--rate` nên dùng | Ghi chú |
|---|---|---|
| `categories` | 2.5 (mặc định) | Chạy ổn ở ~86 request/phút suốt 10 phút |
| `products` | **1.2** | Ở 2.5 req/s (150 request/phút) bị chặn sau ~1,5 phút |
| `reviews` | 2.5 (mặc định) | Chạy ổn ở 3 req/s trong bài test. Nếu bị chặn liên tục, giảm xuống 1.5 |

- `--rate` là **tổng** request/giây của mọi luồng cộng lại. Tăng `--workers` **không** làm nhanh hơn.
- Khi bị chặn, chương trình **tự dừng mọi luồng 7 phút** rồi chạy tiếp. Bị chặn quá 3 lần liên tiếp thì dừng hẳn. Khi đó chờ 10–15 phút rồi chạy lại cùng lệnh với `--rate` thấp hơn.
- **Không chạy 2 crawler cùng lúc trên một mạng**, vì cùng IP thì dùng chung giới hạn.

---

## 5. Dừng, chạy tiếp, theo dõi

### Dừng và chạy tiếp

- Dừng bất cứ lúc nào bằng **Ctrl+C**.
- **Chạy lại đúng lệnh đó để tiếp tục.** Chương trình tự bỏ qua phần đã cào xong.
- Mỗi file chỉ xuất hiện khi đã cào xong trọn vẹn một danh mục hoặc một sản phẩm. Phần đang cào dở lúc dừng sẽ được cào lại từ đầu ở lần chạy sau, nên dữ liệu không bao giờ bị thiếu một nửa.
- Tắt máy, mất mạng hoặc máy ngủ cũng xử lý như vậy: chỉ cần chạy lại lệnh.

Chạy trong terminal của bạn (terminal của VS Code, PowerShell hoặc cmd) và **giữ cửa sổ mở** cho tới khi xong.

### Theo dõi tiến độ

Cứ 30 giây, chương trình in một dòng tiến độ:

```
18:02:18 INFO    Review: 120/45000 sản phẩm, 52000 review | 148 request/phút | 0 lần bị chặn
```

Nếu thấy `lần bị chặn` tăng liên tục, hãy dừng và chạy lại với `--rate` thấp hơn.

Xem tổng quan bất cứ lúc nào. Lệnh này chạy được cả khi crawler đang chạy ở terminal khác:

```bash
python -m src.ingestion.backfill status
```

```
Danh mục lá:        698/698 đã liệt kê sản phẩm
Sản phẩm (đã loại trùng): ..., có review: ...
Đã cào:             ... sản phẩm, ... review, ... MB (gzip)
Còn lại:            ... sản phẩm, ~... review, ~... request (~... giờ ở 2.5 req/s)
```

Toàn bộ log của mọi lần chạy được lưu ở `data/source/raw/backfill.log`.

---

Cào xong thì **gom thư mục `products/` và `reviews/`** của mọi người vào chung một máy. Tên file không xung đột. Nếu một sản phẩm được cào ở nhiều máy thì bước `prepare` sau này sẽ loại trùng.

Mỗi người vẫn giữ tốc độ an toàn như [mục 4](#4-tốc-độ-và-việc-bị-chặn). **Không dùng proxy hay dịch vụ đổi IP** để lách giới hạn của Tiki.

---

## 6. Kết quả

Toàn bộ nằm trong `data/source/raw/`. Thư mục này đã được `.gitignore`, **không commit lên git**. Muốn chia sẻ dữ liệu thì nén thư mục rồi gửi qua Drive.

```
data/source/raw/
├── categories.json                     danh mục sẽ được liệt kê sản phẩm
├── categories_tree.json / .md          cây danh mục (xem file .md cho dễ đọc)
├── products/category=<id>.jsonl.gz     item gốc từ API listing: id, tên, giá, brand, review_count, ...
├── reviews/<product_id>.jsonl.gz       review gốc từ API, mỗi dòng 1 review
└── backfill.log                        log của mọi lần chạy
```

Dữ liệu được giữ **nguyên như API trả về**, chỉ thêm `_crawled_at` (và `_category_id`, `_sort` cho sản phẩm). Đọc thử:

```python
import gzip, json
from pathlib import Path

f = next(Path("data/source/raw/reviews").glob("*.jsonl.gz"))
with gzip.open(f, "rt", encoding="utf-8") as fh:
    review = json.loads(fh.readline())
print(review["id"], review["rating"], review["content"], review["created_by"]["name"])
```

---

## 7. Các tùy chọn

| Tùy chọn | Mặc định | Ý nghĩa |
|---|---|---|
| `--rate` | 2.5 | Tổng request/giây |
| `--workers` | 3 | Số luồng |
| `--roots ID ...` | toàn bộ 26 danh mục gốc | Chỉ cào các danh mục gốc này, dùng để chia việc |
| `--full-tree` | tắt | Bước `categories`: lưu cây danh mục đầy đủ tới tận lá |
| `--max-listing-pages` | 50 | Số trang tối đa mỗi danh mục, mỗi kiểu sắp xếp |
| `--max-reviews-per-product` | không giới hạn | Mỗi sản phẩm chỉ lấy tối đa N review |
| `--target-reviews` | không giới hạn | Dừng khi lần chạy này đã cào đủ N review |
| `--min-reviews` | 1 | Bỏ qua sản phẩm có ít review hơn |
| `--block-wait` | 420 | Số giây chờ khi bị chặn |
| `--out` | `data/source/raw` | Thư mục lưu |
| `-v` | tắt | In chi tiết hơn |

---

## 8. Lỗi thường gặp

| Hiện tượng | Nguyên nhân | Cách xử lý |
|---|---|---|
| `No module named src` | Không đứng ở thư mục gốc repo | `cd` vào thư mục có `src/` rồi chạy lại |
| `No module named requests` | Chưa kích hoạt môi trường ảo hoặc chưa cài thư viện | Kích hoạt `.venv` ([mục 2.2](#22-tạo-môi-trường-ảo-và-cài-thư-viện)), chạy `pip install -r requirements.txt` |
| `running scripts is disabled` (PowerShell) | Windows chặn chạy script | `Set-ExecutionPolicy -Scope CurrentUser RemoteSigned` |
| `Bị WAF chặn! Dừng mọi luồng 420s` | Gửi quá nhanh | Chương trình tự chờ. Nếu lặp lại nhiều lần thì dừng và giảm `--rate` |
| `Bị chặn 4 lần liên tiếp ...` | IP đang bị chặn lâu hơn bình thường | Chờ 10–15 phút rồi chạy lại với `--rate` thấp hơn |
| `Chưa có categories.json` | Chạy `products` trước `categories` | Chạy bước 1 trước |
| `Chưa có sản phẩm` | Chạy `reviews` trước `products` | Chạy bước 2 trước |
| `Task ... lỗi, lần chạy sau sẽ thử lại` | Lỗi mạng ở một danh mục hoặc sản phẩm | Không cần làm gì. Chạy lại lệnh sau khi xong để cào nốt phần lỗi |
| Lỗi in tiếng Việt (`UnicodeEncodeError`) | Terminal không dùng UTF-8 | Chạy `chcp 65001` trước, hoặc dùng terminal của VS Code |
