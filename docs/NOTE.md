# Ghi chú: Giới hạn request của API Tiki

Kết quả kiểm tra thực tế ngày 01/11/2026, gọi không kèm cookie, từ một IP.

---

## 1. API có giới hạn request không?

**Có, nhưng API không báo trước.**

- Cả 3 endpoint đang dùng (`/api/v2/reviews`, `/api/v2/products/{id}`, `/api/personalish/v1/blocks/listings`) **không trả header giới hạn** nào (`X-RateLimit-*`, `Retry-After`).
- Tiki đặt WAF của CDN ByteDance (`Server: Byte-nginx`) phía trước API. Gửi request quá nhanh sẽ bị WAF chặn.

### Kết quả test burst trên `/api/v2/reviews`

| Số luồng | Tốc độ | Kết quả |
|---|---|---|
| 1 | 1,2 req/s | 30/30 OK |
| 5 | 5,5 req/s | 60/60 OK |
| 10 | 14,7 req/s | 100/100 OK |
| 20 | 30 req/s | **49/200 bị chặn** (sau khoảng 340 request trong khoảng 50 giây) |
| 40 | 92 req/s | 298/300 bị chặn |

Bài test trên chỉ kéo dài khoảng 50 giây nên **chưa phản ánh đúng giới hạn**. Xem bài test giữ tốc độ đều ngay dưới đây.

### Kết quả test giữ tốc độ đều (trộn 50% `products/{id}` + 50% `reviews`, giống crawler thật)

| Tốc độ | Thời gian | Kết quả |
|---|---|---|
| 3 req/s (180 req/phút) | 120 giây | 360/360 OK |
| 5 req/s (300 req/phút) | 62 giây | **Bị chặn sau 308 request** |

→ Giới hạn thật nhiều khả năng là **khoảng 300 request trong mỗi 60 giây (cửa sổ trượt)**, tức trung bình khoảng 5 req/s:
- Ở bài test này, lúc bị chặn thì 60 giây gần nhất vừa đủ khoảng 300 request.
- Ở bài burst phía trên, IP cũng bị chặn ngay khi vượt khoảng 300–340 request trong chưa tới 1 phút.

Nghĩa là gửi nhanh trong vài giây thì không sao, nhưng nếu **tổng request trong 1 phút vượt khoảng 300** thì bị chặn.

### Khi bị chặn thì API trả về gì?

- **Không trả 429.** API vẫn trả **HTTP 200**, nhưng body là một trang HTML (`Content-Type: text/html`) thay vì JSON.
- Trang này là một JS challenge của WAF: chứa `_wafchallengeid` và dòng chữ *"the system is inspecting your current network environment…"*.
- Toàn bộ IP bị chặn trong **khoảng 5–6 phút**, sau đó tự mở lại.

---

## 2. Lỗi trong crawler hiện tại

Hàm `_get` trong `tiki_reviews_crawler.py` chỉ nhận diện bị chặn qua status `429/503`. Khi WAF chặn (status 200 + HTML):

1. `r.json()` ném lỗi, crawler in "Request lỗi", đợi 2 giây, retry 3 lần rồi trả về `None`.
2. `get_reviews` dừng giữa chừng, nhưng các review **đã lấy được vẫn bị ghi vào CSV**.
3. Lần chạy resume sau sẽ **bỏ qua sản phẩm đó** vì `product_id` đã có trong file.

→ **Dữ liệu bị thiếu mà không có cảnh báo.**

**Cách sửa:**
- Kiểm tra `Content-Type` có phải `application/json` không, và body có chứa `_wafchallengeid` không. Nếu có, coi như bị chặn.
- Khi bị chặn: dừng **tất cả** luồng, đợi **ít nhất 6 phút** rồi mới thử lại. Retry 2 giây là vô ích vì cả IP đang bị chặn.
- Không ghi review của một sản phẩm vào CSV nếu chưa cào xong sản phẩm đó.

---

## 3. Streaming: poll mỗi vài giây có được không?

**Không nên.** Poll mỗi vài giây vừa dễ bị chặn, vừa không có thêm dữ liệu mới.

### 3.1. Thứ quyết định bị chặn là tổng tốc độ request

Với luồng cào tăng dần (xem `PIPELINE.md` mục 1.2), mỗi chu kỳ tốn khoảng **2 request/sản phẩm** (`products/{id}` + trang 1 của reviews). Với 87 sản phẩm:

| Chu kỳ poll | Request/chu kỳ | Request/phút | Kết quả |
|---|---|---|---|
| 5 giây | ~174 | ~2.100 | **Bị chặn** |
| 30 giây | ~174 | ~350 | **Bị chặn** (vượt ~300/phút) |
| 1 phút | ~174 | ~174 | Ổn, nhưng chỉ còn ~40% biên an toàn |
| 5 phút | ~174 | ~35 | An toàn |
| 15–30 phút (theo thiết kế) | ~174 | < 12 | Rất an toàn |

Công thức: `request/phút ≈ 2 × số_sản_phẩm × 60 / chu_kỳ_poll (giây)`, phải giữ **≤ 150 request/phút**. Theo dõi càng nhiều sản phẩm thì chu kỳ càng phải dài.

### 3.2. Review mới đến rất chậm

Theo `data/tiki_reviews_dataset.csv` (87 sản phẩm, 20.705 review):

| Khoảng thời gian | Review mới | Trung bình |
|---|---|---|
| 30 ngày gần nhất | 23 | ~0,8 review/ngày cho cả 87 sản phẩm |
| 90 ngày gần nhất | 95 | ~1,1 review/ngày |
| 365 ngày gần nhất | 411 | ~1,1 review/ngày |

→ Poll mỗi 5 giây tốn khoảng **3 triệu request/ngày để bắt được khoảng 1 review mới**.

---

## 4. Cấu hình khuyến nghị

| Thông số | Giá trị | Lý do |
|---|---|---|
| Tổng tốc độ request | **≤ 2,5 req/s (≤ 150 request/phút)** | Bằng khoảng 50% giới hạn ~300 request/phút. Tốt nhất dùng **một rate limiter chung** cho mọi luồng, vì `sleep` hiện tại là riêng cho từng luồng |
| Chu kỳ poll | **5–15 phút** | Chu kỳ dưới 1 phút không làm dữ liệu tươi hơn |
| Số luồng + sleep | Xem bảng ngay dưới | |
| Khi bị chặn | Dừng hết, đợi **≥ 6 phút** | Block kéo dài khoảng 5–6 phút cho cả IP |
| Nhận diện bị chặn | `Content-Type != application/json` hoặc body có `_wafchallengeid` | WAF trả 200, không trả 429 |

### Sleep cho mỗi luồng

Mỗi request mất khoảng 0,4–0,8 giây (p50 khoảng 0,45 giây). Để tổng tốc độ ≤ 2,5 req/s thì mỗi luồng phải mất ít nhất `số_luồng / 2,5` giây cho một vòng (request + sleep):

| Số luồng | Mỗi vòng ít nhất | `DELAY_MIN` – `DELAY_MAX` gợi ý | Tốc độ ước tính |
|---|---|---|---|
| 1 | 0,4 giây | 0 – 0,3 | ~1,5–2 req/s |
| 3 | 1,2 giây | **0,8 – 1,2** | ~2–2,3 req/s |
| 5 | 2,0 giây | **1,6 – 2,4** | ~2–2,3 req/s |

Cấu hình hiện tại (5 luồng, sleep 0,5–1,2 giây) chạy khoảng **3,5–4 req/s (210–240 request/phút)**, chỉ thấp hơn giới hạn khoảng 20–30%. Nếu mạng nhanh hơn lúc test thì dễ vượt ngưỡng. Nên tăng sleep lên **1,6–2,4 giây**, hoặc giảm còn 3 luồng với sleep 0,8–1,2 giây.

Thêm luồng **không làm crawler nhanh hơn**: trần là ~150 request/phút dù chạy bao nhiêu luồng.

---

## 5. Gợi ý cho demo "realtime"

Vì nguồn dữ liệu thật quá thưa, nên tách làm hai luồng:

1. **Crawler thật**: chạy chu kỳ chậm (5–15 phút), gửi vào `tiki.reviews.raw`.
2. **Replay producer**: đọc `data/tiki_reviews_dataset.csv` và đẩy vào cùng topic với tốc độ khoảng 5–10 message/giây.

Kafka → Spark → dashboard vẫn chạy streaming liên tục khi demo mà không cần gọi API Tiki, nên không có nguy cơ bị chặn.
