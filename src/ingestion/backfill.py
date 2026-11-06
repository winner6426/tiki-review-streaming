"""Cào offline review Tiki để làm kho nguồn cho replay.

Chạy từ thư mục gốc của repo, theo 3 bước (mỗi bước chạy lại được, tự bỏ qua phần đã xong):

    python -m src.ingestion.backfill categories   # đi cây danh mục → danh sách danh mục lá
    python -m src.ingestion.backfill products     # liệt kê sản phẩm của từng danh mục lá
    python -m src.ingestion.backfill reviews      # cào toàn bộ review, sản phẩm nhiều review trước
    python -m src.ingestion.backfill status       # xem tiến độ

    python -m src.ingestion.backfill all          # chạy lần lượt cả 3 bước

Kết quả (mặc định trong data/source/raw/, đã gitignore):

    categories.json                          danh mục sẽ được liệt kê sản phẩm
    categories_tree.json / .md               cây danh mục đã duyệt (--full-tree để lưu tới tận lá)
    products/category=<id>.jsonl.gz          item gốc từ API listing (có review_count, giá, brand)
    reviews/<product_id>.jsonl.gz            review gốc từ API, mỗi dòng 1 review
    backfill.log

"""
import argparse
import gzip
import json
import logging
import os
import sys
import threading
import time
from collections import deque
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from pathlib import Path

from .tiki_client import (LISTING_MAX_PAGES, LISTING_PAGE_SIZE, LISTING_SORTS, REVIEW_PAGE_SIZE,
                          ROOT_CATEGORY_ID, Blocked, Stopped, TikiClient)

log = logging.getLogger("backfill")

LISTING_CAP = LISTING_PAGE_SIZE * LISTING_MAX_PAGES   


def write_jsonl_gz(path: Path, records: list[dict]):
    tmp = path.parent / (path.name + ".tmp")
    with gzip.open(tmp, "wt", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
    os.replace(tmp, path)


def read_jsonl_gz(path: Path):
    with gzip.open(path, "rt", encoding="utf-8") as f:
        for line in f:
            yield json.loads(line)


def clean_tmp(out: Path):
    for p in out.rglob("*.tmp"):
        p.unlink()



def run_parallel(tasks, fn, workers: int, stop: threading.Event,
                 on_done=None, progress=None, every: float = 30):
    """Chạy fn(task) trên `workers` luồng, nộp task dần dần để dừng được bất kỳ lúc nào.

    on_done(task, result) chạy ở luồng chính; trả về False để ngừng nộp task mới
    (task đang chạy vẫn được làm xong). Hàm trả về False nếu bị dừng sớm.
    """
    tasks = iter(tasks)
    pending = {}
    last = time.monotonic()
    more = True
    with ThreadPoolExecutor(workers) as ex:
        try:
            while True:
                while more and not stop.is_set() and len(pending) < workers * 2:
                    task = next(tasks, None)
                    if task is None:
                        break
                    pending[ex.submit(fn, task)] = task
                if not pending:
                    return not stop.is_set()
                # timeout ngắn để Ctrl+C trên Windows vẫn ngắt được
                done, _ = wait(pending, timeout=1.0, return_when=FIRST_COMPLETED)
                for fut in done:
                    task = pending.pop(fut)
                    try:
                        result = fut.result()
                    except Stopped:
                        continue
                    except Blocked as e:
                        log.error("%s. Dừng hẳn, chạy lại sau ít nhất 10 phút.", e)
                        stop.set()
                        continue
                    except Exception as e:
                        log.warning("Task %s lỗi, lần chạy sau sẽ thử lại: %s", task, e)
                        continue
                    if on_done and on_done(task, result) is False:
                        more = False
                if progress and time.monotonic() - last >= every:
                    progress()
                    last = time.monotonic()
        except KeyboardInterrupt:
            log.warning("Ctrl+C: đang dừng, phần đang cào dở sẽ bị bỏ (chạy lại để tiếp tục)")
            stop.set()
            for fut in pending:
                fut.cancel()
            return False


# =====================================================
# BƯỚC 1: CÂY DANH MỤC
# =====================================================

def step_categories(client: TikiClient, out: Path, roots: list[int] | None, full_tree: bool = False):
    """Đi cây danh mục và chọn các danh mục sẽ liệt kê sản phẩm.

    Danh mục ≤ 2.000 sản phẩm (hoặc là lá) thì liệt kê trực tiếp, lớn hơn thì đi tiếp xuống con,
    để vượt giới hạn 50 trang của API listing.
    roots=None: lấy toàn bộ danh mục gốc của Tiki.
    full_tree=True: vẫn đi tiếp xuống dưới danh mục đã chọn để lưu cây đầy đủ (tốn thêm request,
    không đổi danh sách danh mục được liệt kê).
    """
    root_list = {c["id"]: c for c in client.child_categories(ROOT_CATEGORY_ID)}
    if not roots:
        roots = list(root_list)
        log.info("Lấy toàn bộ %d danh mục gốc: %s", len(roots), roots)

    def make(c, parent_id, root, depth):
        return {"id": c["id"], "name": c.get("name"), "parent_id": parent_id, "root": root,
                "depth": depth, "product_count": c.get("product_count") or 0,
                "is_leaf": bool(c.get("is_leaf")), "listed": False}

    nodes, leaves, queue = [], [], deque()
    for r in roots:
        n = make(root_list.get(r, {"id": r}), ROOT_CATEGORY_ID, r, 0)
        nodes.append(n)
        queue.append((n, False))
    seen = 0
    while queue:
        if client.stop.is_set():
            raise Stopped()
        node, covered = queue.popleft()   # covered: đã có tổ tiên được chọn để liệt kê
        if not covered and (node["is_leaf"] or 0 < node["product_count"] <= LISTING_CAP):
            node["listed"] = covered = True
            leaves.append(node)
        if node["is_leaf"] or (covered and not full_tree):
            continue
        children = client.child_categories(node["id"])
        seen += 1
        if not children:   # không có con → tự nó là lá (có thể bị giới hạn 2.000 sản phẩm)
            if not covered:
                node["listed"] = True
                leaves.append(node)
            continue
        for c in children:
            n = make(c, node["id"], node["root"], node["depth"] + 1)
            nodes.append(n)
            queue.append((n, covered))
        if seen % 50 == 0:
            log.info("Đã duyệt %d danh mục, thấy %d, còn %d chờ duyệt", seen, len(nodes), len(queue))

    (out / "categories.json").write_text(json.dumps(leaves, ensure_ascii=False, indent=1), encoding="utf-8")
    (out / "categories_tree.json").write_text(json.dumps(nodes, ensure_ascii=False, indent=1), encoding="utf-8")
    write_tree_md(out / "categories_tree.md", nodes, full_tree)
    big = sum(l["product_count"] > LISTING_CAP for l in leaves)
    log.info("Xong: thấy %d danh mục, chọn %d danh mục để liệt kê, ~%d sản phẩm theo product_count "
             "(trùng lặp giữa danh mục chưa loại). %d danh mục vượt 2.000 sản phẩm sẽ được liệt kê theo %d kiểu sort. "
             "Xem cây ở %s",
             len(nodes), len(leaves), sum(l["product_count"] for l in leaves), big, len(LISTING_SORTS),
             out / "categories_tree.md")


def write_tree_md(path: Path, nodes: list[dict], full_tree: bool):
    children = {}
    for n in nodes:
        children.setdefault(n["parent_id"], []).append(n)
    lines = ["# Cây danh mục Tiki", "",
             f"Lấy lúc {time.strftime('%Y-%m-%d %H:%M')}. "
             f"{'Cây đầy đủ.' if full_tree else 'Chỉ đi xuống tới danh mục được liệt kê (chạy với --full-tree để xem hết).'}",
             "",
             "- `[liệt kê]`: danh mục được dùng để liệt kê sản phẩm (con của nó đã nằm trong đó).",
             "- `product_count` do API trả về, tối đa hiển thị 10.000.", ""]

    def walk(n):
        mark = " **[liệt kê]**" if n["listed"] else ""
        lines.append(f"{'  ' * n['depth']}- {n['name']} (`{n['id']}`): {n['product_count']:,} SP{mark}")
        for c in children.get(n["id"], []):
            walk(c)

    for r in children.get(ROOT_CATEGORY_ID, []):
        walk(r)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


# =====================================================
# BƯỚC 2: SẢN PHẨM
# =====================================================

def step_products(client: TikiClient, out: Path, workers: int, max_pages: int):
    cat_file = out / "categories.json"
    if not cat_file.exists():
        raise SystemExit("Chưa có categories.json. Chạy bước `categories` trước.")
    leaves = json.loads(cat_file.read_text(encoding="utf-8"))
    pdir = out / "products"
    pdir.mkdir(parents=True, exist_ok=True)
    todo = [l for l in leaves if not (pdir / f"category={l['id']}.jsonl.gz").exists()]
    log.info("Sản phẩm: %d/%d danh mục lá chưa liệt kê", len(todo), len(leaves))

    def fetch(leaf):
        # Danh mục lớn hơn 2.000 sản phẩm: liệt kê theo nhiều kiểu sort rồi gộp, để lấy được nhiều hơn 2.000
        sorts = LISTING_SORTS if (leaf["product_count"] or 0) > LISTING_CAP else [None]
        items = {}
        for sort in sorts:
            page, last = 1, max_pages
            while page <= last:
                js = client.listing(leaf["id"], page, sort)
                data = (js or {}).get("data") or []
                if not data:
                    break
                now = int(time.time())
                for it in data:
                    if it.get("id") and it["id"] not in items:
                        it["_category_id"] = leaf["id"]
                        it["_sort"] = sort
                        it["_crawled_at"] = now
                        items[it["id"]] = it
                last = min(max_pages, (js.get("paging") or {}).get("last_page") or page)
                page += 1
        write_jsonl_gz(pdir / f"category={leaf['id']}.jsonl.gz", list(items.values()))
        return len(items)

    stats = {"cats": 0, "items": 0}

    def on_done(leaf, n):
        stats["cats"] += 1
        stats["items"] += n
        log.debug("Danh mục %s (%s): %d sản phẩm", leaf["id"], leaf["name"], n)

    def progress():
        log.info("Sản phẩm: %d/%d danh mục, %d item, %d request, %d lần bị chặn",
                 stats["cats"], len(todo), stats["items"], client.n_requests, client.n_blocks)

    ok = run_parallel(todo, fetch, workers, client.stop, on_done, progress)
    progress()
    return ok


def load_products(out: Path) -> dict[int, dict]:
    """Gộp sản phẩm từ mọi danh mục, loại trùng theo id."""
    products = {}
    for f in sorted((out / "products").glob("category=*.jsonl.gz")):
        for it in read_jsonl_gz(f):
            pid = it.get("id")
            if pid and pid not in products:
                products[pid] = {"id": pid, "name": it.get("name"),
                                 "review_count": it.get("review_count") or 0}
    return products


# =====================================================
# BƯỚC 3: REVIEW
# =====================================================

def step_reviews(client: TikiClient, out: Path, workers: int,
                 max_reviews_per_product: int | None, target_reviews: int | None,
                 min_reviews: int):
    rdir = out / "reviews"
    rdir.mkdir(parents=True, exist_ok=True)
    products = load_products(out)
    if not products:
        raise SystemExit("Chưa có sản phẩm. Chạy bước `products` trước.")
    done = {int(p.name.split(".")[0]) for p in rdir.glob("*.jsonl.gz")}

    # Sản phẩm nhiều review trước: mỗi request lấy đủ 20 review, ít request phí nhất
    todo = sorted((p for p in products.values()
                   if p["review_count"] >= min_reviews and p["id"] not in done),
                  key=lambda p: p["review_count"], reverse=True)
    cap = max_reviews_per_product or float("inf")
    est_reviews = sum(min(p["review_count"], cap) for p in todo)
    est_requests = sum(-(-min(p["review_count"], cap) // REVIEW_PAGE_SIZE) for p in todo)
    log.info("Review: %d sản phẩm cần cào (đã xong %d), ước tính ~%d review, ~%d request (~%.1f giờ)",
             len(todo), len(done), est_reviews, est_requests,
             est_requests * client.limiter.interval / 3600)

    max_pages = -(-cap // REVIEW_PAGE_SIZE) if max_reviews_per_product else None

    def fetch(product):
        rows, page, last = [], 1, max_pages or 1
        while page <= last:
            js = client.reviews(product["id"], page)
            data = (js or {}).get("data") or []
            if not data:
                break
            now = int(time.time())
            for rv in data:
                rv["_crawled_at"] = now
            rows.extend(data)
            api_last = (js.get("paging") or {}).get("last_page") or page
            last = min(max_pages, api_last) if max_pages else api_last
            page += 1
        write_jsonl_gz(rdir / f"{product['id']}.jsonl.gz", rows)
        return len(rows)

    stats = {"products": 0, "reviews": 0}
    t0, req0 = time.monotonic(), client.n_requests

    def on_done(product, n):
        stats["products"] += 1
        stats["reviews"] += n
        if target_reviews and stats["reviews"] >= target_reviews:
            if not stats.get("target_hit"):
                stats["target_hit"] = True
                log.info("Đã đạt mục tiêu %d review, chờ các sản phẩm đang cào xong rồi dừng", target_reviews)
            return False

    def progress():
        mins = (time.monotonic() - t0) / 60
        rpm = (client.n_requests - req0) / mins if mins else 0
        log.info("Review: %d/%d sản phẩm, %d review | %.0f request/phút | %d lần bị chặn",
                 stats["products"], len(todo), stats["reviews"], rpm, client.n_blocks)

    ok = run_parallel(todo, fetch, workers, client.stop, on_done, progress)
    progress()
    return ok


# =====================================================
# STATUS
# =====================================================

def step_status(out: Path, rate: float):
    cat_file = out / "categories.json"
    print(f"Thư mục: {out.resolve()}")
    if cat_file.exists():
        leaves = json.loads(cat_file.read_text(encoding="utf-8"))
        n_listed = len(list((out / "products").glob("category=*.jsonl.gz")))
        print(f"[products] Danh mục lá:    {n_listed}/{len(leaves)} đã liệt kê sản phẩm")
    else:
        print("[products] Danh mục lá:    chưa chạy bước `categories`")
        return
    products = load_products(out)
    with_reviews = [p for p in products.values() if p["review_count"] > 0]
    files = list((out / "reviews").glob("*.jsonl.gz"))
    done = {int(p.name.split(".")[0]) for p in files}
    n_reviews = sum(sum(1 for _ in read_jsonl_gz(f)) for f in files)
    size_mb = sum(f.stat().st_size for f in files) / 1e6
    remaining = [p for p in with_reviews if p["id"] not in done]
    rem_req = sum(-(-p["review_count"] // REVIEW_PAGE_SIZE) for p in remaining)
    print(f"[products] Sản phẩm đã liệt kê (loại trùng): {len(products)}, có review: {len(with_reviews)}")
    print(f"[products] Tổng review của các sản phẩm này: ~{sum(p['review_count'] for p in with_reviews)}")
    print(f"[reviews]  Đã cào review:  {len(done)} sản phẩm, {n_reviews} review, {size_mb:.1f} MB (gzip)")
    print(f"[reviews]  Còn phải cào:   {len(remaining)} sản phẩm, ~{sum(p['review_count'] for p in remaining)} review, "
          f"~{rem_req} request (~{rem_req / rate / 3600:.1f} giờ ở {rate} req/s)")


# =====================================================
# MAIN
# =====================================================

def main():
    ap = argparse.ArgumentParser(description="Cào offline review Tiki cho kho nguồn replay")
    ap.add_argument("step", choices=["categories", "products", "reviews", "all", "status"])
    ap.add_argument("--out", type=Path, default=Path("data/source/raw"), help="thư mục lưu (mặc định data/source/raw)")
    ap.add_argument("--rate", type=float, default=2.5,
                    help="tổng request/giây cho mọi luồng (mặc định 2.5 ≈ 150/phút, giới hạn của Tiki ~300/phút)")
    ap.add_argument("--workers", type=int, default=3, help="số luồng (mặc định 3)")
    ap.add_argument("--roots", type=int, nargs="+", default=None,
                    help="id danh mục gốc (mặc định: toàn bộ danh mục gốc của Tiki). "
                         "Dùng để chia việc: mỗi người cào một nhóm danh mục")
    ap.add_argument("--full-tree", action="store_true",
                    help="bước categories: lưu cây danh mục đầy đủ tới tận lá (tốn thêm request)")
    ap.add_argument("--max-listing-pages", type=int, default=LISTING_MAX_PAGES,
                    help=f"số trang listing tối đa mỗi danh mục, mỗi kiểu sort "
                         f"(mặc định {LISTING_MAX_PAGES} = tối đa của API)")
    ap.add_argument("--max-reviews-per-product", type=int, default=None,
                    help="giới hạn số review mỗi sản phẩm (mặc định: lấy hết)")
    ap.add_argument("--target-reviews", type=int, default=None,
                    help="dừng khi lần chạy này đã cào đủ số review này")
    ap.add_argument("--min-reviews", type=int, default=1, help="bỏ qua sản phẩm có ít review hơn (mặc định 1)")
    ap.add_argument("--block-wait", type=float, default=420, help="số giây chờ khi bị WAF chặn (mặc định 420)")
    ap.add_argument("--burst", type=int, default=None,
                    help="cứ sau ngần này request thì tự nghỉ --cooldown giây (mặc định: không nghỉ). "
                         "Bước products nên dùng ~150 vì API listing chặn sau ~200 request bất kể tốc độ")
    ap.add_argument("--cooldown", type=float, default=600, help="số giây tự nghỉ sau mỗi --burst request (mặc định 600)")
    ap.add_argument("-v", "--verbose", action="store_true")
    args = ap.parse_args()

    args.out.mkdir(parents=True, exist_ok=True)
    for stream in (sys.stdout, sys.stderr):   # tránh lỗi in tiếng Việt khi chuyển hướng output trên Windows
        stream.reconfigure(encoding="utf-8", errors="replace")
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-7s %(message)s", datefmt="%H:%M:%S",
        handlers=[logging.StreamHandler(),
                  logging.FileHandler(args.out / "backfill.log", encoding="utf-8")],
    )
    logging.getLogger("urllib3").setLevel(logging.WARNING)

    if args.step == "status":
        step_status(args.out, args.rate)
        return

    clean_tmp(args.out)
    if sys.platform == "win32":   # không cho Windows tự sleep khi đang cào (tự hết hiệu lực khi thoát)
        import ctypes
        ctypes.windll.kernel32.SetThreadExecutionState(0x80000000 | 0x00000001)   # ES_CONTINUOUS | ES_SYSTEM_REQUIRED
    client = TikiClient(rate=args.rate, block_wait=args.block_wait, burst=args.burst, cooldown=args.cooldown)
    log.info("Bắt đầu `%s`: %.1f req/s, %d luồng, lưu vào %s", args.step, args.rate, args.workers, args.out)
    def run() -> bool:
        if args.step in ("categories", "all"):
            step_categories(client, args.out, args.roots, args.full_tree)
        if args.step in ("products", "all"):
            if not step_products(client, args.out, args.workers, args.max_listing_pages):
                return False
        if args.step in ("reviews", "all"):
            if not step_reviews(client, args.out, args.workers, args.max_reviews_per_product,
                                args.target_reviews, args.min_reviews):
                return False
        return True

    ok = False
    try:
        ok = run()
    except (KeyboardInterrupt, Stopped):
        log.warning("Đã dừng. Chạy lại cùng lệnh để tiếp tục.")
    except Blocked as e:
        log.error("%s. Chạy lại sau ít nhất 10 phút.", e)
    finally:
        log.info("Tổng %d request, %d lần bị chặn", client.n_requests, client.n_blocks)
    if not ok:   # exit code 1 để lệnh nối bằng && không chạy bước sau trên dữ liệu thiếu
        sys.exit(1)


if __name__ == "__main__":
    main()
