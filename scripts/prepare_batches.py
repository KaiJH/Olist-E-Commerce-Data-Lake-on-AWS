import argparse
import csv
import hashlib
import json
import shutil
from pathlib import Path


CUTOFF = "2018-01-01"

TABLES = {
    "orders": "olist_orders_dataset.csv",
    "order_items": "olist_order_items_dataset.csv",
    "order_payments": "olist_order_payments_dataset.csv",
    "order_reviews": "olist_order_reviews_dataset.csv",
    "customers": "olist_customers_dataset.csv",
    "products": "olist_products_dataset.csv",
    "sellers": "olist_sellers_dataset.csv",
    "geolocation": "olist_geolocation_dataset.csv",
    "category_translation": "product_category_name_translation.csv"
}

ORDER_CHILDREN = {
    "order_items": "order_id",
    "order_payments": "order_id",
    "order_reviews": "order_id"
}

def read_row(path):
    with open(path, newline="", encoding="utf-8") as f:
        rows = list(csv.reader(f))
    return rows[0], rows[1:]

def write_rows(path, header, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, quoting=csv.QUOTE_ALL, lineterminator="\n")
        w.writerow(header)
        w.writerows(rows)
        
def file_stats(path):
    data = path.read_bytes()
    with open(path, newline="", encoding="utf-8") as f:
        n = sum(1 for _ in csv.reader(f)) - 1
    return {
        "rows": n,
        "bytes": len(data),
        "md5": hashlib.md5(data).hexdigest()
    }

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--src", required=True, type=Path)
    ap.add_argument("--out", default="staging", type=Path)
    args = ap.parse_args()
    
    header, orders = read_row(args.src/TABLES["orders"])
    ts_idx = header.index("order_purchase_timestamp")
    b1_ids = {r[0] for r in orders if r[ts_idx] < CUTOFF}
    batches = {
        "batch1": {},
        "batch2": {}
    }
    
    write_rows(args.out/"batch1"/"orders"/TABLES["orders"], header, [r for r in orders if r[0] in b1_ids])
    write_rows(args.out/"batch2"/"orders"/TABLES["orders"], header, [r for r in orders if r[0] not in b1_ids])
    
    for table, key in ORDER_CHILDREN.items():
        h, rows = read_row(args.src/TABLES[table])
        k = h.index(key)
        write_rows(args.out/"batch1"/table/TABLES[table], h, [r for r in rows if r[k] in b1_ids])
        write_rows(args.out/"batch2"/table/TABLES[table], h, [r for r in rows if r[k] not in b1_ids])
    
    for table in ("customers", "products", "sellers", "geolocation", "category_translation"):
        dst = args.out/"batch1"/table/TABLES[table]
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(args.src/TABLES[table], dst)
        
    for batch in batches:
        for f in sorted((args.out/batch).glob("*/*.csv")):
            batches[batch][f.parent.name] = {
                "file": f.name,
                **file_stats(f)
            }
        manifest = args.out/batch/"manifest.json"
        manifest.write_text(json.dumps(batches[batch], indent=2))
        total = sum(v["rows"] for v in batches[batch].values())
        print(f"{batch}: {len(batches[batch])} tables, {total:,} rows -> {manifest}")
        for t, v in batches[batch].items():
            print(f"  {t:<22}{v['rows']:>10,}")


if __name__ == "__main__":
    main()