import argparse
import json
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

def exists(s3, bucket, key):
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] in ("404", "NoSuchKey", "NotFound"):
            return False
        raise
    
    
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--batch-dir", required=True, type=Path)
    ap.add_argument("--bucket", required=True)
    ap.add_argument("--ingest-date", required=True, help="YYYY-MM-DD")
    ap.add_argument("--profile", default=None)
    ap.add_argument("--region", default="us-east-2")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    
    manifest = json.loads((args.batch_dir/"manifest.json").read_text())
    s3 = None if args.dry_run else boto3.Session(profile_name=args.profile, region_name=args.region).client("s3")
    
    uploads = [
        (args.batch_dir/table/meta["file"], f"raw/olist/{table}/ingest_date={args.ingest_date}/{meta['file']}")
        for table, meta in manifest.items()
    ]
    
    uploads.append((args.batch_dir/"manifest.json", f"manifests/ingest_date={args.ingest_date}/manifest.json"))
    
    for path, key in uploads:
        if args.dry_run:
            print(f"[dry-run] {path} -> s3://{args.bucket}/{key}")
            continue
        
        if exists(s3, args.bucket, key):
            raise SystemExit(f"STOP: s3://{args.bucket}/P{key} already exists (raw zone is append-only)")
        s3.upload_file(str(path), args.bucket, key)
        print(f"uploaded s3://{args.bucket}/{key}")

if __name__ == "__main__":
    main()