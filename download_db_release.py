"""
Download the latest github_pipeline.db from a GitHub Release asset.

Usage:
  python download_db_release.py --repo USERNAME/REPO --file github_pipeline.db --tag db-snapshot
"""
import argparse
import os
import sys
import requests

API = "https://api.github.com"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--repo", required=True)
    ap.add_argument("--file", required=True)
    ap.add_argument("--tag", default="db-snapshot")
    ap.add_argument("--token", default=os.environ.get("GITHUB_TOKEN"))
    args = ap.parse_args()

    if not args.token:
        sys.exit("Set GITHUB_TOKEN env var or pass --token")

    headers = {"Authorization": f"Bearer {args.token}", "Accept": "application/vnd.github+json"}
    r = requests.get(f"{API}/repos/{args.repo}/releases/tags/{args.tag}", headers=headers)

    if r.status_code == 404:
        print(f"No release '{args.tag}' found yet — starting fresh, nothing to download.")
        return

    r.raise_for_status()
    release = r.json()
    filename = os.path.basename(args.file)
    asset = next((a for a in release.get("assets", []) if a["name"] == filename), None)

    if not asset:
        print(f"No asset named '{filename}' in release '{args.tag}' — starting fresh.")
        return

    dl_headers = {"Authorization": f"Bearer {args.token}", "Accept": "application/octet-stream"}
    print(f"Downloading {filename} ({asset['size'] / 1e6:.1f} MB)...")
    with requests.get(asset["url"], headers=dl_headers, stream=True) as resp:
        resp.raise_for_status()
        with open(args.file, "wb") as f:
            for chunk in resp.iter_content(chunk_size=1 << 20):
                f.write(chunk)
    print(f"Downloaded -> {args.file}")


if __name__ == "__main__":
    main()
