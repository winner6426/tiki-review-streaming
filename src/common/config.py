import os

# Kafka
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")    # địa chỉ Kafka
TOPIC_REVIEWS = os.getenv("TOPIC_REVIEWS", "tiki.reviews.raw")      # topic chứa review
TOPIC_PRODUCTS = os.getenv("TOPIC_PRODUCTS", "tiki.products.raw")   # topic chứa sản phẩm

# Data
SOURCE_RAW = os.getenv("SOURCE_RAW", "data/source/raw")             # JSON gốc 
SOURCE_REPLAY = os.getenv("SOURCE_REPLAY", "data/source/replay")    # Parquet location
HASH_SALT = os.getenv("HASH_SALT", "dev-salt-change-me")            # chuỗi hash

# Delta
LAKE = os.getenv("LAKE", "/data/lake")                  
BRONZE_REVIEWS = f"{LAKE}/bronze/reviews"               # message gốc từ Kafka
BRONZE_PRODUCTS = f"{LAKE}/bronze/products"
SILVER_REVIEWS = f"{LAKE}/silver/reviews"               # merge(sạch, k trùng)
SILVER_PRODUCTS = f"{LAKE}/silver/products"
QUARANTINE_REVIEWS = f"{LAKE}/quarantine/reviews"       # message lỗi
GOLD = f"{LAKE}/gold"                                   # chưa nghĩ ra
CHECKPOINTS = f"{LAKE}/_checkpoints"                    # Spark lưu offset

# Streaming
TRIGGER = os.getenv("TRIGGER", "1 minute")                                      # mỗi micro-batch cách nhau bao lâu
MAX_OFFSETS_PER_TRIGGER = int(os.getenv("MAX_OFFSETS_PER_TRIGGER", "50000"))    # số message tối đa mỗi batch

# Postgres
PG_HOST = os.getenv("PG_HOST", "postgres")     
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_DB = os.getenv("PG_DB", "tiki")            
PG_USER = os.getenv("PG_USER", "tiki")
PG_PASSWORD = os.getenv("PG_PASSWORD", "tiki")
PG_URL = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"   
