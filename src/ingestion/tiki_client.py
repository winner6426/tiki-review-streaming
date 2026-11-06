"""Client gọi API Tiki: giới hạn tốc độ chung cho mọi luồng + nhận diện bị WAF chặn.

Giới hạn đo thực tế (xem docs/NOTE.md):
- Khoảng 300 request / 60 giây (cửa sổ trượt) cho mỗi IP.
- Khi vượt ngưỡng, WAF trả HTTP 200 kèm trang HTML challenge (không phải 429),
  và chặn cả IP khoảng 5–6 phút.
"""
import logging
import threading
import time

import requests

log = logging.getLogger(__name__)

BASE = "https://tiki.vn"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/148.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.6,en;q=0.5",
}
REVIEW_PAGE_SIZE = 20     # API trả 400 nếu limit > 20
LISTING_PAGE_SIZE = 40
LISTING_MAX_PAGES = 50    # API chỉ cho xem tối đa 50 trang (2.000 sản phẩm) mỗi danh mục, mỗi kiểu sort
# Mỗi kiểu sort cho ra một tập 2.000 sản phẩm khác nhau (đã kiểm tra: trang 50 của 5 kiểu không trùng nhau),
# nên gộp lại thì danh mục lớn lấy được tới ~10.000 sản phẩm
LISTING_SORTS = [None, "top_seller", "newest", "price,asc", "price,desc"]
ROOT_CATEGORY_ID = 2      # cha của mọi danh mục gốc


class Stopped(Exception):
    """Người dùng yêu cầu dừng (Ctrl+C)."""


class Blocked(Exception):
    """Bị WAF chặn liên tục quá số lần cho phép."""


class RateLimiter:
    """Giới hạn tốc độ chung cho mọi luồng: các request cách nhau ít nhất 1/rate giây.

    Khi bị chặn, `pause()` dừng toàn bộ luồng đến khi hết thời gian chờ.
    burst: cứ sau `burst` request thì tự nghỉ `cooldown` giây (API listing chặn theo tổng số request
    chứ không theo tốc độ, xem docs/NOTE.md). None: không nghỉ.
    """

    def __init__(self, rate: float, stop: threading.Event,
                 burst: int | None = None, cooldown: float = 600):
        self.interval = 1.0 / rate
        self.stop = stop
        self.burst = burst
        self.cooldown = cooldown
        self._lock = threading.Lock()
        self._next = 0.0
        self._sent = 0   # số request từ lần nghỉ / bị chặn gần nhất

    def wait(self):
        with self._lock:
            now = time.monotonic()
            slot = max(now, self._next)
            self._next = slot + self.interval
            self._sent += 1
            if self.burst and self._sent >= self.burst:
                self._sent = 0
                self._next = slot + self.cooldown
                log.info("Đã gửi %d request, tự nghỉ %ds để tránh bị chặn", self.burst, self.cooldown)
        if self.stop.wait(max(0.0, slot - time.monotonic())):
            raise Stopped()

    def pause(self, seconds: float):
        with self._lock:
            self._next = max(self._next, time.monotonic() + seconds)
            self._sent = 0


class TikiClient:
    def __init__(self, rate: float = 2.5, block_wait: float = 420,
                 max_blocks: int = 3, stop: threading.Event | None = None,
                 burst: int | None = None, cooldown: float = 600):
        self.stop = stop or threading.Event()
        self.limiter = RateLimiter(rate, self.stop, burst, cooldown)
        self.block_wait = block_wait
        self.max_blocks = max_blocks
        self._local = threading.local()
        self._count_lock = threading.Lock()
        self.n_requests = 0
        self.n_blocks = 0

    def _session(self) -> requests.Session:
        if not hasattr(self._local, "s"):
            self._local.s = requests.Session()
            self._local.s.headers.update(HEADERS)
        return self._local.s

    def get(self, path: str, params: dict | None = None, retries: int = 3) -> dict | None:
        """GET JSON. Trả về None nếu API báo lỗi 4xx (dữ liệu không tồn tại / tham số sai).

        Lỗi mạng hoặc 5xx: thử lại `retries` lần, hết lượt thì ném exception.
        Bị WAF chặn: dừng mọi luồng `block_wait` giây rồi thử lại, không tính vào `retries`.
        """
        attempt = blocks = 0
        while True:
            self.limiter.wait()
            with self._count_lock:
                self.n_requests += 1
            try:
                r = self._session().get(BASE + path, params=params, timeout=30)
            except requests.RequestException as e:
                attempt += 1
                if attempt >= retries:
                    raise
                log.warning("Lỗi mạng %s (lần %d): %s", path, attempt, e)
                self.limiter.pause(2 ** attempt)
                continue

            if r.status_code >= 500 or r.status_code == 429:
                attempt += 1
                if attempt >= retries:
                    r.raise_for_status()
                log.warning("HTTP %d %s (lần %d)", r.status_code, path, attempt)
                self.limiter.pause(5 * attempt)
                continue
            if r.status_code >= 400:
                log.warning("HTTP %d %s %s, bỏ qua", r.status_code, path, params)
                return None

            if self._is_blocked(r):
                blocks += 1
                with self._count_lock:
                    self.n_blocks += 1
                if blocks > self.max_blocks:
                    raise Blocked(f"Bị chặn {blocks} lần liên tiếp tại {path}")
                log.error("Bị WAF chặn! Dừng mọi luồng %ds (lần %d/%d)",
                          self.block_wait, blocks, self.max_blocks)
                self.limiter.pause(self.block_wait)
                continue
            return r.json()

    @staticmethod
    def _is_blocked(r: requests.Response) -> bool:
        ctype = r.headers.get("Content-Type", "")
        return not ctype.startswith("application/json") or b"_wafchallengeid" in r.content[:4096]

    # ---------- endpoint ----------

    def child_categories(self, parent_id: int) -> list[dict]:
        js = self.get("/api/v2/categories", {"parent_id": parent_id})
        return (js or {}).get("data", [])

    def listing(self, category_id: int, page: int, sort: str | None = None) -> dict | None:
        params = {"limit": LISTING_PAGE_SIZE, "page": page, "category": category_id}
        if sort:
            params["sort"] = sort
        return self.get("/api/personalish/v1/blocks/listings", params)

    def reviews(self, product_id: int, page: int) -> dict | None:
        return self.get("/api/v2/reviews", {
            "product_id": product_id,
            "sort": "id|desc",
            "page": page,
            "limit": REVIEW_PAGE_SIZE,
        })
