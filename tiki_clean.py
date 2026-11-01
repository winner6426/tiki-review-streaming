"""Tiki Reviews – Data Cleaning Pipeline.

| Bước | Nội dung |
|------|----------|
| 0 | Config |
| 1 | Load file CSV |
| 2 | Xóa duplicate |
| 3 | Chuẩn hóa kiểu dữ liệu |
| 4 | Làm sạch text |
| 5 | Xử lý missing values |
| 6 | Lọc outlier / bất thường |
| 7 | Feature engineering |
| 8 | Loại bỏ cột không cần thiết |
| 9 | Xuất file sạch |
"""
import os
import re

import pandas as pd

# ── Bước 0 · CONFIG ─────────────────────────────────
INPUT_FILE      = 'tiki_reviews_dataset.csv'
OUTPUT_CLEAN    = 'tiki_reviews_clean.csv'

MIN_CONTENT_LEN = 5
MAX_CONTENT_LEN = 5000
VALID_RATINGS   = {1, 2, 3, 4, 5}


def show(obj):
    """Thay cho display() của Jupyter: in DataFrame/Series ra terminal."""
    if isinstance(obj, (pd.DataFrame, pd.Series)):
        print(obj.to_string())
    else:
        print(obj)


def step_header(title):
    print('\n' + '=' * 60)
    print(title)
    print('=' * 60)


# ── Bước 1 · Load file CSV ──────────────────────────
def load_csv(path):
    step_header('Bước 1 · Load file CSV')
    df = pd.read_csv(path, dtype=str, encoding='utf-8-sig')

    print(f'Dòng   : {len(df):,}')
    print(f'Cột    : {df.shape[1]}  →  {list(df.columns)}')

    # ── Missing ban đầu
    miss = df.isna().sum()
    miss = miss[miss > 0].rename('missing_count').to_frame()
    miss['%'] = (miss['missing_count'] / len(df) * 100).round(1)
    print('\nMissing value ban đầu:')
    show(miss)

    # ── Preview
    print('\nMẫu 5 dòng đầu:')
    show(df.head())
    return df


# ── Bước 2 · Xóa Duplicate ──────────────────────────
def drop_duplicates(df):
    step_header('Bước 2 · Xóa Duplicate')
    print(f'Trước: {len(df):,} dòng')

    # 2a. Trùng hoàn toàn
    b = len(df)
    df = df.drop_duplicates()
    print(f'  ✂ Trùng hoàn toàn     : -{b - len(df):,} dòng')

    # 2b. Trùng review_id
    dup = df[df.duplicated('review_id', keep=False)]
    if not dup.empty:
        print(f'\n  Ví dụ review_id trùng ({len(dup):,} dòng):')
        show(dup[['review_id', 'product_id', 'rating', 'content']].head(6))

    b = len(df)
    df = df.drop_duplicates(subset=['review_id'], keep='first')
    print(f'  ✂ Trùng review_id     : -{b - len(df):,} dòng  (giữ lần đầu)')

    df = df.reset_index(drop=True)
    print(f'\nSau : {len(df):,} dòng')
    return df


# ── Bước 3 · Chuẩn hóa kiểu dữ liệu ─────────────────
def parse_ts(series):
    numeric  = pd.to_numeric(series, errors='coerce')
    from_ts  = pd.to_datetime(numeric, unit='s', errors='coerce', utc=True)
    from_str = pd.to_datetime(series,  errors='coerce', utc=True)
    result   = from_ts if from_ts.notna().sum() >= from_str.notna().sum() else from_str
    return result.dt.tz_localize(None)


def cast_types(df):
    step_header('Bước 3 · Chuẩn hóa kiểu dữ liệu')
    int_cols   = ['product_id', 'brand_id', 'review_id', 'customer_id',
                  'rating', 'thank_count', 'review_count', 'order_count',
                  'discount', 'discount_rate']
    float_cols = ['price', 'list_price']
    dt_cols    = ['created_at', 'purchased_at']

    def first_value(s):
        return s.dropna().iloc[0] if s.notna().any() else None

    # ── dtype trước
    all_cast_cols = [c for c in int_cols + float_cols + dt_cols if c in df.columns]
    dtype_before  = df[all_cast_cols].dtypes.rename('dtype_TRƯỚC')
    sample_before = df[all_cast_cols].apply(first_value).rename('mẫu_TRƯỚC')

    # ── cast
    for col in int_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')

    for col in float_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    for col in dt_cols:
        if col in df.columns:
            df[col] = parse_ts(df[col])

    # ── so sánh trước / sau
    dtype_after  = df[all_cast_cols].dtypes.rename('dtype_SAU')
    sample_after = df[all_cast_cols].apply(first_value).rename('mẫu_SAU')
    compare = pd.concat([dtype_before, dtype_after, sample_before, sample_after], axis=1)
    show(compare)
    return df


# ── Bước 4 · Làm sạch Text ──────────────────────────
_EMOJI_RE    = re.compile(
    r'[\U00010000-\U0010ffff\U0001F600-\U0001F64F'
    r'\U0001F300-\U0001F5FF\U0001F680-\U0001F6FF'
    r'\U0001F1E0-\U0001F1FF☀-⛿✀-➿]+',
    flags=re.UNICODE)
_MULTI_SPACE = re.compile(r'\s+')
_HTML_TAG    = re.compile(r'<[^>]+>')


def clean_text(text):
    if pd.isna(text) or str(text).strip() in ('', 'nan', 'None'):
        return None
    t = str(text)
    t = _HTML_TAG.sub(' ', t)
    t = _EMOJI_RE.sub('', t)
    t = t.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ')
    t = _MULTI_SPACE.sub(' ', t)
    return t.strip() or None


def clean_text_columns(df):
    step_header('Bước 4 · Làm sạch Text')
    text_cols = ['product_name', 'brand_name', 'title', 'content', 'customer_name']

    # ── snapshot trước
    snap_before = {}
    for col in text_cols:
        if col in df.columns:
            sample = df[col].dropna()
            dirty  = sample[sample.str.contains(r'\s{2,}|<', regex=True, na=False)]
            snap_before[col] = (dirty.iloc[0] if not dirty.empty
                                else sample.iloc[0] if not sample.empty else '')

    # ── clean
    for col in text_cols:
        if col in df.columns:
            df[col] = df[col].apply(clean_text)

    # ── so sánh trước / sau
    rows = []
    for col in text_cols:
        if col in df.columns:
            sample    = df[col].dropna()
            after_val = sample.iloc[0] if not sample.empty else ''
            rows.append({
                'cột': col,
                'TRƯỚC (mẫu)': str(snap_before.get(col, ''))[:70],
                'SAU (mẫu)':   str(after_val)[:70],
                'null sau':    df[col].isna().sum(),
            })
    show(pd.DataFrame(rows).set_index('cột'))
    return df


# ── Bước 5 · Xử lý Missing Values ───────────────────
def handle_missing(df):
    step_header('Bước 5 · Xử lý Missing Values')

    # ── missing TRƯỚC
    miss_before = df.isna().sum().rename('TRƯỚC')

    # ── fillna
    fills = {
        'title':         '',
        'content':       '',
        'thank_count':   0,
        'customer_name': 'Ẩn danh',
    }
    for col, val in fills.items():
        if col in df.columns:
            df[col] = df[col].fillna(val)

    # ── xóa dòng thiếu bắt buộc
    required = ['review_id', 'product_id', 'rating']
    b = len(df)
    df = df.dropna(subset=required).reset_index(drop=True)
    print(f'✂ Xóa dòng thiếu {required}: -{b - len(df):,} dòng  →  còn {len(df):,}')

    # ── missing SAU
    miss_after = df.isna().sum().rename('SAU')
    compare = pd.concat([miss_before, miss_after], axis=1)
    compare = compare[compare.sum(axis=1) > 0]
    compare['thay đổi'] = compare['SAU'] - compare['TRƯỚC']
    print('\nSo sánh missing trước / sau:')
    show(compare if not compare.empty else 'Không còn missing')
    return df


# ── Bước 6 · Lọc Outlier / Bất thường ───────────────
def filter_outliers(df):
    step_header('Bước 6 · Lọc Outlier / Bất thường')
    print(f'Trước: {len(df):,} dòng')

    # ── rating
    bad = df[~df['rating'].isin(VALID_RATINGS)]
    if not bad.empty:
        print(f'  Rating không hợp lệ: {bad["rating"].value_counts().to_dict()}')
        show(bad[['review_id', 'rating', 'content']].head(3))
    b = len(df)
    df = df[df['rating'].isin(VALID_RATINGS)]
    print(f'  ✂ Rating ngoài {VALID_RATINGS}: -{b - len(df):,}')

    # ── content length
    df = df.assign(_clen=df['content'].str.len())
    print('\nPhân phối độ dài content (ký tự):')
    show(df['_clen'].describe().round(0).to_frame().T)

    short = df[df['_clen'] < MIN_CONTENT_LEN][['content', 'rating']]
    if not short.empty:
        print(f'\nMẫu content quá ngắn (< {MIN_CONTENT_LEN} ký tự):')
        show(short.head(5))

    b = len(df)
    df = df[df['_clen'] >= MIN_CONTENT_LEN].drop(columns=['_clen'])
    print(f'  ✂ Content < {MIN_CONTENT_LEN} ký tự : -{b - len(df):,}')

    print(f'\nSau : {len(df):,} dòng')
    return df.reset_index(drop=True)


# ── Bước 7 · Feature Engineering ────────────────────
def add_features(df):
    step_header('Bước 7 · Feature Engineering')
    cols_before = list(df.columns)

    # sentiment
    mapping = {1: 'very_negative', 2: 'negative', 3: 'neutral',
               4: 'positive', 5: 'very_positive'}
    df['sentiment'] = df['rating'].map(mapping)

    new_cols = [c for c in df.columns if c not in cols_before]
    print(f'Cột mới: {new_cols}')
    print(f'Tổng cột: {len(cols_before)} → {df.shape[1]}')

    # ── preview feature mới
    print('\nPhân bố sentiment:')
    show(df['sentiment'].value_counts().to_frame())
    return df


# ── Bước 8 · Loại bỏ cột không cần thiết ────────────
def drop_unused_columns(df):
    step_header('Bước 8 · Loại bỏ cột không cần thiết')
    cols_to_drop = ['thank_count', 'review_count', 'order_count']

    # Lọc ra các cột thực sự tồn tại trong DataFrame
    existing_cols_to_drop = [col for col in cols_to_drop if col in df.columns]

    print(f'Trước: {df.shape[1]} cột')
    df = df.drop(columns=existing_cols_to_drop)
    print(f'Sau: {df.shape[1]} cột')
    print(f'Các cột đã loại bỏ: {existing_cols_to_drop}')
    return df


# ── Bước 9 · Xuất file sạch ─────────────────────────
def export(df, path):
    step_header('Bước 9 · Xuất file sạch')
    df.to_csv(path, index=False, encoding='utf-8-sig')
    size_kb = os.path.getsize(path) // 1024
    print(f'Đã lưu: {path}')
    print(f'   {len(df):,} dòng  |  {df.shape[1]} cột  |  ~{size_kb:,} KB')

    # ── tổng kết
    print('\n── Tổng kết ──')
    print(f'Sản phẩm unique : {df["product_id"].nunique():,}')
    if 'brand_name' in df.columns:
        print(f'Brand unique    : {df["brand_name"].nunique():,}')
    print(f'Rating trung bình: {df["rating"].mean():.2f} / 5')

    print('\nPhân bố rating:')
    show(df['rating'].value_counts().sort_index().rename('count').to_frame())

    print('\nPreview 5 dòng cuối của data sạch:')
    show(df.tail())


def main():
    print(f'Input : {INPUT_FILE}')
    print(f'Output: {OUTPUT_CLEAN}')

    df = load_csv(INPUT_FILE)
    df = drop_duplicates(df)
    df = cast_types(df)
    df = clean_text_columns(df)
    df = handle_missing(df)
    df = filter_outliers(df)
    df = add_features(df)
    df = drop_unused_columns(df)
    export(df, OUTPUT_CLEAN)


if __name__ == '__main__':
    main()
