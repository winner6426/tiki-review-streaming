"""
Hợp đồng dữ liệu: message trong Kafka gồm những trường nào, kiểu gì. 
"""

SCHEMA_VERSION = 1 

REVIEW_COLUMNS = [
    ("review_id", "BIGINT"),
    ("product_id", "BIGINT"),
    ("rating", "INT"),
    ("title", "STRING"),
    ("content", "STRING"),
    ("thank_count", "INT"),
    ("comment_count", "INT"),
    ("created_at", "BIGINT"),
    ("purchased", "BOOLEAN"),
    ("purchased_at", "BIGINT"),
    ("customer_hash", "STRING"),
]

REVIEW_META = [
    ("crawled_at", "BIGINT"),
    ("source", "STRING"),
    ("replay_round", "INT"),
    ("schema_version", "INT"),
]

PRODUCT_COLUMNS = [
    ("product_id", "BIGINT"),
    ("name", "STRING"),
    ("brand_name", "STRING"),
    ("price", "BIGINT"),          # currency: VND
    ("list_price", "BIGINT"),     # API tiki hay trả về 0 
    ("discount_rate", "INT"),     
    ("rating_average", "DOUBLE"),
    ("review_count", "INT"),
    ("quantity_sold", "BIGINT"),
    ("category_id", "BIGINT"),
]

PRODUCT_META = [
    ("crawled_at", "BIGINT"),
    ("source", "STRING"),
    ("schema_version", "INT"),
]

def ddl(columns: list[tuple[str, str]]) -> str: 
    """ 
    Data definition language
    Chuyển về dạng chuỗi cho from_json của spark dùng
    VD: [("a", "INT"), ("b", "STRING")] -> "a INT, b STRING"
    """
    return ", ".join(f"{name} {type_}" for name, type_ in columns)

# For cleaning data
REVIEW_FIELDS = [name for name, type_ in REVIEW_COLUMNS]
PRODUCT_FIELDS = [name for name, type_ in PRODUCT_COLUMNS]

# For Spark
REVIEW_DDL = ddl(REVIEW_COLUMNS + REVIEW_META)
PRODUCT_DDL = ddl(PRODUCT_COLUMNS + PRODUCT_META)