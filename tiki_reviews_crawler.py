"""Crawl đánh giá sản phẩm Tiki.

Thu thập: nội dung đánh giá, số sao, tên người dùng, thời gian đánh giá.
"""
import csv
import os
import random
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import requests
from tqdm import tqdm

# =====================================================
# CONFIG
# =====================================================

CATEGORY_ID  = 1815
MAX_PRODUCTS = 100
OUTPUT_FILE  = "tiki_reviews_dataset.csv"

# Số luồng song song khi crawl product info + reviews
PRODUCT_WORKERS = 5   # lấy thông tin sản phẩm song song
REVIEW_WORKERS  = 3   # lấy reviews song song theo từng sản phẩm

# Delay giữa các request (giây) – giảm xuống để nhanh hơn
DELAY_MIN = 0.5
DELAY_MAX = 1.2

# =====================================================
# COOKIE / HEADER
# =====================================================

# Cookie copy từ trình duyệt, truyền qua biến môi trường TIKI_COOKIE
raw_cookie_string = os.environ.get("TIKI_COOKIE", "")

cookies = {}
for part in raw_cookie_string.split("; "):
    if "=" in part:
        k, v = part.split("=", 1)
        cookies[k] = v

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/148.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "vi-VN,vi;q=0.9,fr-FR;q=0.8,fr;q=0.7,en-US;q=0.6,en;q=0.5",
    "x-guest-token": "DAmcJPYXo67ug8KynMQTLqEUawtC25vp",
    "Connection": "keep-alive",
}

# =====================================================
# CSV – KHỞI TẠO VÀ GHI INCREMENTAL
# =====================================================

CSV_COLUMNS = [
    "product_id", "product_name", "brand_id", "brand_name",
    "price", "list_price", "discount", "discount_rate",
    "review_count", "order_count",
    "review_id", "rating", "title", "content", "thank_count",
    "created_at", "customer_id", "customer_name", "purchased_at",
]


def init_csv(filepath: str) -> bool:
    """Tạo file CSV với header nếu chưa tồn tại.
    Trả về True nếu file mới, False nếu đã có (resume mode)."""
    if os.path.exists(filepath):
        print(f"[RESUME] File '{filepath}' đã tồn tại – tiếp tục ghi thêm.")
        return False
    with open(filepath, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        writer.writeheader()
    return True


def append_rows(filepath: str, rows: list[dict]):
    """Ghi ngay một batch rows vào CSV (append)."""
    if not rows:
        return
    with open(filepath, "a", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS, extrasaction="ignore")
        writer.writerows(rows)


def load_done_product_ids(filepath: str) -> set:
    """Đọc các product_id đã có trong file để bỏ qua khi resume."""
    if not os.path.exists(filepath):
        return set()
    try:
        df = pd.read_csv(filepath, usecols=["product_id"], dtype=str)
        return set(df["product_id"].dropna().unique())
    except Exception:
        return set()

# =====================================================
# HTTP HELPER
# =====================================================

def _get(url, params=None, retries=3):
    """GET với retry tự động."""
    for attempt in range(retries):
        try:
            r = requests.get(
                url,
                headers=HEADERS,
                cookies=cookies,
                params=params,
                timeout=30,
            )
            if r.status_code == 200:
                return r.json()
            if r.status_code in (429, 503):          # rate-limit
                wait = (attempt + 1) * 3
                print(f"  Rate-limit {r.status_code}, chờ {wait}s …")
                time.sleep(wait)
        except Exception as e:
            print(f"  Request lỗi (attempt {attempt+1}): {e}")
            time.sleep(2)
    return None

# =====================================================
# LẤY DANH SÁCH SẢN PHẨM
# =====================================================

def get_products_from_category(category_id: int, max_products: int) -> list[dict]:
    products, page = [], 1
    while len(products) < max_products:
        data = _get(
            "https://tiki.vn/api/personalish/v1/blocks/listings",
            params={"limit": 40, "page": page, "category": category_id},
        )
        if not data:
            break
        items = data.get("data", [])
        if not items:
            break
        for item in items:
            products.append({"id": item.get("id"), "name": item.get("name")})
            if len(products) >= max_products:
                break
        page += 1
        time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))
    return products

# =====================================================
# THÔNG TIN SẢN PHẨM
# =====================================================

def get_product_info(product_id) -> dict | None:
    js = _get(f"https://tiki.vn/api/v2/products/{product_id}")
    if not js:
        return None
    return {
        "product_id":    js.get("id"),
        "product_name":  js.get("name") or js.get("meta_title"),
        "brand_name":    js.get("brand", {}).get("name"),
        "brand_id":      js.get("brand", {}).get("id"),
        "price":         js.get("price"),
        "list_price":    js.get("list_price"),
        "discount":      js.get("discount"),
        "discount_rate": js.get("discount_rate"),
        "review_count":  js.get("review_count"),
        "order_count":   js.get("order_count"),
    }

# =====================================================
# REVIEWS – LẤY TẤT CẢ TRANG
# =====================================================

def get_reviews(product_info: dict) -> list[dict]:
    rows, page = [], 1
    while True:
        data = _get(
            "https://tiki.vn/api/v2/reviews",
            params={
                "product_id": product_info["product_id"],
                "sort": "score|desc,id|desc,stars|all",
                "page": page,
                "limit": 20,           # tăng từ 10 → 20 để ít request hơn
                "include": "comments,contribute_info,attribute_vote_summary",
            },
        )
        if not data:
            break
        reviews = data.get("data", [])
        if not reviews:
            break
        for rv in reviews:
            cb = rv.get("created_by", {})
            rows.append({
                **product_info,
                "review_id":     rv.get("id"),
                "rating":        rv.get("rating"),
                "title":         rv.get("title"),
                "content":       rv.get("content"),
                "thank_count":   rv.get("thank_count"),
                "created_at":    rv.get("created_at"),
                "customer_id":   rv.get("customer_id"),
                "customer_name": cb.get("name"),
                "purchased_at":  cb.get("purchased_at"),
            })
        page += 1
        time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))
    return rows

# =====================================================
# XỬ LÝ MỘT SẢN PHẨM (dùng cho thread pool)
# =====================================================

def process_product(product: dict, output_file: str) -> int:
    """Crawl 1 sản phẩm, lưu reviews ngay vào CSV, trả về số review đã lưu."""
    pid = product["id"]
    info = get_product_info(pid)
    if not info:
        return 0
    reviews = get_reviews(info)
    if reviews:
        append_rows(output_file, reviews)   # ← ghi ngay, không chờ
    return len(reviews)

# =====================================================
# MAIN
# =====================================================

def main():
    print("=" * 55)
    print("TIKI REVIEW CRAWLER  –  incremental save + fast mode")
    print("=" * 55)

    # ── 1. Khởi tạo CSV ──────────────────────────────────────
    init_csv(OUTPUT_FILE)
    done_ids = load_done_product_ids(OUTPUT_FILE)
    if done_ids:
        print(f"[RESUME] Đã có {len(done_ids)} product_id trong file, sẽ bỏ qua.")

    # ── 2. Lấy danh sách sản phẩm ────────────────────────────
    print(f"\n[1/2] Lấy tối đa {MAX_PRODUCTS} sản phẩm từ danh mục {CATEGORY_ID} …")
    all_products = get_products_from_category(CATEGORY_ID, MAX_PRODUCTS)

    # Lọc bỏ những sản phẩm đã crawl (resume)
    products = [p for p in all_products if str(p["id"]) not in done_ids]
    print(f"      → {len(all_products)} tìm thấy, {len(products)} chưa crawl.")

    # ── 3. Crawl song song ───────────────────────────────────
    print(f"\n[2/2] Crawl reviews với {PRODUCT_WORKERS} luồng song song …\n")
    total_reviews = 0

    with ThreadPoolExecutor(max_workers=PRODUCT_WORKERS) as pool:
        futures = {
            pool.submit(process_product, p, OUTPUT_FILE): p
            for p in products
        }
        for future in tqdm(as_completed(futures), total=len(futures), unit="sản phẩm"):
            p = futures[future]
            try:
                n = future.result()
                total_reviews += n
                tqdm.write(f"  ✓ {p['id']} ({p.get('name','')[:40]}) → {n} reviews")
            except Exception as e:
                tqdm.write(f"  ✗ {p['id']} lỗi: {e}")

    # ── 4. Tổng kết ──────────────────────────────────────────
    print("\n" + "=" * 55)
    with open(OUTPUT_FILE, encoding="utf-8-sig") as f:
        total_in_file = sum(1 for _ in f) - 1
    print(f"Hoàn thành!  {total_reviews} reviews mới  |  {total_in_file} dòng tổng cộng")
    print(f"File: {OUTPUT_FILE}")
    print("=" * 55)


if __name__ == "__main__":
    main()
