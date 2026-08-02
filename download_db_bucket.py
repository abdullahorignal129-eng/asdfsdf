"""
Download github_pipeline.db from a Hugging Face Storage Bucket.

Usage:
  python download_db_bucket.py --bucket PERDYPTO/db_snapshot --file github_pipeline.db

Requires:
  pip install huggingface_hub
  HF_TOKEN env var (or --token) with read access to the bucket.
"""
import argparse
import os
import sys

from huggingface_hub import login, list_bucket_tree, download_bucket_files


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bucket", required=True, help="namespace/bucket-name, e.g. PERDYPTO/db_snapshot")
    ap.add_argument("--file", required=True, help="local path to save the file to")
    ap.add_argument("--remote-name", default=None, help="filename inside the bucket (default: local basename)")
    ap.add_argument("--token", default=os.environ.get("HF_TOKEN"))
    args = ap.parse_args()

    if not args.token:
        sys.exit("Set HF_TOKEN env var or pass --token")

    login(token=args.token, add_to_git_credential=False)

    remote_name = args.remote_name or os.path.basename(args.file)

    try:
        existing = {item.path for item in list_bucket_tree(args.bucket) if item.type == "file"}
    except Exception as e:
        print(f"Bucket '{args.bucket}' not accessible yet ({e}) -- starting fresh, nothing to download.")
        return

    if remote_name not in existing:
        print(f"No '{remote_name}' found in bucket '{args.bucket}' yet -- starting fresh.")
        return

    print(f"Downloading hf://buckets/{args.bucket}/{remote_name} -> {args.file}")
    download_bucket_files(args.bucket, files=[(remote_name, args.file)])
    print(f"Downloaded -> {args.file}")


if __name__ == "__main__":
    main()
