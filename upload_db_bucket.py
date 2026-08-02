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

_logged_in_token = None  # avoid re-calling login() on every checkpoint upload


def upload_file_to_bucket(bucket: str, file: str, remote_name: str | None = None, token: str | None = None) -> str:
    """
    Upload `file` to the given HF bucket (namespace/name) under `remote_name`
    (defaults to the local file's basename). Creates the bucket if it
    doesn't exist yet. Returns the hf:// URI of the uploaded file.

    Importable so other scripts (e.g. the pipeline's own checkpointing)
    can call this directly instead of shelling out.
    """
    global _logged_in_token
    token = token or os.environ.get("HF_TOKEN")
    if not token:
        raise RuntimeError("Set HF_TOKEN env var or pass token= explicitly")
    if not os.path.exists(file):
        raise FileNotFoundError(f"File not found: {file}")

    if _logged_in_token != token:
        login(token=token, add_to_git_credential=False)
        _logged_in_token = token

    remote_name = remote_name or os.path.basename(file)
    size_mb = os.path.getsize(file) / 1e6
    print(f"Uploading {file} ({size_mb:.1f} MB) -> hf://buckets/{bucket}/{remote_name}")

    create_bucket(bucket, exist_ok=True)
    batch_bucket_files(bucket, add=[(file, remote_name)])

    uri = f"hf://buckets/{bucket}/{remote_name}"
    print(f"Done. {uri}")
    return uri


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bucket", required=True, help="namespace/bucket-name, e.g. PERDYPTO/db_snapshot")
    ap.add_argument("--file", required=True, help="local file to upload")
    ap.add_argument("--remote-name", default=None, help="filename inside the bucket (default: local basename)")
    ap.add_argument("--token", default=os.environ.get("HF_TOKEN"))
    args = ap.parse_args()

    upload_file_to_bucket(args.bucket, args.file, remote_name=args.remote_name, token=args.token)


if __name__ == "__main__":
    main()
