"""Create a deliberately broken order_reviews batch for the failure drill.

12 rows: 6 valid, 4 with review_score = 9, 2 with a missing review_id.
Expected result in Glue: 6 rows quarantined, the Data Quality gate fails,
the job fails, and EventBridge -> SNS sends an alert email.

Usage:
    python scripts/make_bad_batch.py --out staging/drill
"""
import argparse
import csv
import hashlib
import json
from pathlib import Path

HEADER = ["review_id", "order_id", "review_score", "review_comment_title",
          "review_comment_message", "review_creation_date", "review_answer_timestamp"]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="staging/drill", type=Path)
    args = ap.parse_args()
    rows = []
    for i in range(12):
        review_id = "" if i >= 10 else f"drill_review_{i:02d}"
        score = "9" if 6 <= i < 10 else str(1 + i % 5)
        rows.append([review_id, f"drill_order_{i:02d}", score, "", "failure drill row",
                     "2026-10-03 00:00:00", "2026-10-03 12:00:00"])
    path = args.out / "order_reviews" / "drill_bad_reviews.csv"
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, quoting=csv.QUOTE_ALL, lineterminator="\n")
        w.writerow(HEADER)
        w.writerows(rows)
    data = path.read_bytes()
    manifest = {"order_reviews": {"file": path.name, "rows": len(rows), "bytes": len(data),
                                  "md5": hashlib.md5(data).hexdigest()}}
    (args.out / "manifest.json").write_text(json.dumps(manifest, indent=2))
    print(f"wrote {path} ({len(rows)} rows) and {args.out / 'manifest.json'}")


if __name__ == "__main__":
    main()