"""
Upload github_pipeline.db to a Hugging Face Storage Bucket.

Buckets are mutable, non-versioned object storage (not git-based repos),
backed by Xet chunk-level dedup -- much better fit than GitHub Releases
for a file that gets overwritten every checkpoint.

Usage:
  python upload_db_bucket.py --bucket PERDYPTO/db_snapshot --file github_pipeline.db

Requires:
  pip install huggingface_hub
  HF_TOKEN env var (or --token) with write access to the bucket's namespace.
"""
import argparse
import os
import sys

from huggingface_hub import login, create_bucket, batch_bucket_files


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bucket", required=True, help="namespace/bucket-name, e.g. PERDYPTO/db_snapshot")
    ap.add_argument("--file", required=True, help="local file to upload")
    ap.add_argument("--remote-name", default=None, help="filename inside the bucket (default: local basename)")
    ap.add_argument("--token", default=os.environ.get("HF_TOKEN"))
    args = ap.parse_args()

    if not args.token:
        sys.exit("Set HF_TOKEN env var or pass --token")
    if not os.path.exists(args.file):
        sys.exit(f"File not found: {args.file}")

    login(token=args.token, add_to_git_credential=False)

    remote_name = args.remote_name or os.path.basename(args.file)
    size_mb = os.path.getsize(args.file) / 1e6
    print(f"Uploading {args.file} ({size_mb:.1f} MB) -> hf://buckets/{args.bucket}/{remote_name}")

    # exist_ok=True: safe to call every run, no-op if the bucket already exists
    create_bucket(args.bucket, exist_ok=True)

    batch_bucket_files(args.bucket, add=[(args.file, remote_name)])
    print(f"Done. hf://buckets/{args.bucket}/{remote_name}")


if __name__ == "__main__":
    main()
