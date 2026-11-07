import argparse
import glob 
import gzip 
import hashlib
import json
import logging
import os 
import shutil

import pandas as pd

from src.common import config
from src.common.schema import PRODUCT_COLUMNS, REVIEW_COLUMNS

log = logging.getLogger(__name__)

# schema type -> pandas type 
PANDAS_TYPES = {
    "BIGINT": "Int64",
    "INT": "Int32", 
    "DOUBLE": "Float64",
    "BOOLEAN": "boolean", 
    "STRING": "string"
    }

def hash_customer(customer_id) -> str | None: 
    if pd.isna(customer_id):
        return None 
    return hashlib.sha256(f"{config.HASH_SALT}: {int(customer_id)}".encode()).hexdigest()

def review_from_raw(d: dict) -> dict: 
    by = d.get("created_by") or {}

    return {
        "review_id": d.get("id"),
        "product_id": d.get("product_id"),
        "rating": d.get("rating"),
        "title": d.get("title"),
        "content": d.get("content"),
        "thank_count": d.get("thank_count"),
        "comment_count": d.get("comment_count"),
        "created_at": d.get("created_at"),
        "purchased": by.get("purchased"),
        "purchased_at": by.get("purchased_at"),
        "customer_hash": hash_customer(d.get("customer_id") or by.get("id")),
    }

def product_from_raw(d: dict) -> dict: 
    sold = d.get("quantity_sold") or {}
    
    return {
        "product_id": d.get("id"),
        "name": d.get("name"),
        "brand_name": d.get("brand_name"),
        "price": d.get("price"),
        "list_price": d.get("list_price"),
        "discount_rate": d.get("discount_rate"),
        "rating_average": d.get("rating_average"),
        "review_count": d.get("review_count"),
        "quantity_sold": sold.get("value"),
        "category_id": d.get("_category_id"),
    }

def to_contract(df: pd.DataFrame, columns: list[tuple[str, str]]) -> pd.DataFrame:
    """
    Giữ đúng các columns, đúng thứ tự, đúng kiểu
    """
    return (df.reindex(columns = [name for name, _ in columns])
            .astype({name: PANDAS_TYPES[type_] for name, type_ in columns}))

def read_jsonl_gz(path: str):
    with gzip.open(path, "rt", encoding = "utf-8") as f:
        for line in f:
            if line.strip():
                yield json.loads(line)

def load_raw(raw_dir: str, limit: int | None = None) -> tuple[pd.DataFrame, pd.DataFrame]:
    review_files = sorted(glob.glob(os.path.join(raw_dir, "reviews", "*.jsonl.gz")))[:limit] 
    product_files = sorted(glob.glob(os.path.join(raw_dir, "products", "*.jsonl.gz")))
    log.info("Đọc %d file review, %d file sản phẩm", len(review_files), len(product_files))
    reviews = pd.DataFrame([review_from_raw(d) for p in review_files for d in read_jsonl_gz(p)])
    products = pd.DataFrame([product_from_raw(d) for p in product_files for d in read_jsonl_gz(p)])

    return reviews, products

def load_csv(path: str) -> tuple[pd.DataFrame, pd.DataFrame] | None:
    pass

def write_reviews(df: pd.DataFrame, out_dir: str): 
    n_in = len(df)

    # lọc trùng review_id & sort theo ngày
    df = (to_contract(df, REVIEW_COLUMNS)
          .dropna(subset = ["review_id", "product_id", "created_at"])
          .drop_duplicates("review_id")
          .sort_values(["created_at", "review_id"]))

    path = os.path.join(out_dir, "reviews")
    shutil.rmtree(path, ignore_errors=True)  # mỗi lần chạy ghi lại toàn bộ reviews từ đầu
    years = pd.to_datetime(df["created_at"], unit="s").dt.year
    
    for year, part in df.groupby(years): 
        os.makedirs(os.path.join(path, f"year={year}"))
        part.to_parquet(os.path.join(path, f"year={year}", "part-0.parquet"), index = False)
    log.info("Review: đọc %d, ghi %d (bỏ %d dòng trùng / thiếu), %d năm -> %s", 
            n_in, len(df), n_in - len(df), years.nunique(), path)

def write_products(df: pd.DataFrame, out_dir: str): 
    n_in = len(df)

    df = (to_contract(df, PRODUCT_COLUMNS)
          .dropna(subset=["product_id"])
          .drop_duplicates("product_id")) # 1 sản phẩm có thể nằm trong nhiều danh mục

    os.makedirs(out_dir, exist_ok = True)
    path = os.path.join(out_dir, "products.parquet")
    df.to_parquet(path, index = False)
    log.info("Sản phẩm: đọc %d, ghi %d -> %s", n_in, len(df), path)

def main(): 
    ap = argparse.ArgumentParser(description = "Chuyển data từ src -> replay")
    ap.add_argument("--limit", type = int, help = "Chạy limit file review để test thử")
    ap.add_argument("--out", default = config.SOURCE_REPLAY)
    args = ap.parse_args()
    logging.basicConfig(level = logging.INFO, 
                        format = "%(asctime)s %(levelname)-7s %(message)s",
                        datefmt = "%H:%M:%S")

    reviews, products = load_raw(config.SOURCE_RAW, args.limit)
    write_reviews(reviews, args.out)
    write_products(products, args.out)

if __name__ == "__main__":
    main()