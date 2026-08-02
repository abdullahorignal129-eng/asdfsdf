"""
Upload a large file (e.g. github_pipeline.db) to a GitHub repo as a
Release asset. Works up to 2GB per file, no git/LFS needed.

Usage:
  python upload_db_release.py --repo USERNAME/REPO --file github_pipeline.db --tag db-snapshot

Requires:
  GITHUB_TOKEN env var (or --token) with 'repo' scope.
"""
import argparse
import os
import sys
import requests

API = "https://api.github.com"
UPLOAD_API = "https://uploads.github.com"


def get_or_create_release(repo, tag, token):
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"}
    r = requests.get(f"{API}/repos/{repo}/releases/tags/{tag}", headers=headers)
    if r.status_code == 200:
        return r.json()

    r = requests.post(
        f"{API}/repos/{repo}/releases",
        headers=headers,
        json={"tag_name": tag, "name": tag, "body": "DB snapshot upload", "draft": False},
    )
    r.raise_for_status()
    return r.json()


def delete_existing_asset(repo, release, filename, token):
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"}
    for asset in release.get("assets", []):
        if asset["name"] == filename:
            requests.delete(f"{API}/repos/{repo}/releases/assets/{asset['id']}", headers=headers)


def upload_asset(repo, release, filepath, token):
    filename = os.path.basename(filepath)
    delete_existing_asset(repo, release, filename, token)

    upload_url = release["upload_url"].split("{")[0]
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/octet-stream",
    }
    size = os.path.getsize(filepath)
    print(f"Uploading {filepath} ({size / 1e6:.1f} MB) -> {repo} release '{release['tag_name']}'")

    with open(filepath, "rb") as f:
        r = requests.post(
            upload_url,
            headers=headers,
            params={"name": filename},
            data=f,
        )
    r.raise_for_status()
    asset = r.json()
    print(f"Done. Download URL: {asset['browser_download_url']}")
    return asset["browser_download_url"]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--repo", required=True, help="username/repo")
    ap.add_argument("--file", required=True)
    ap.add_argument("--tag", default="db-snapshot")
    ap.add_argument("--token", default=os.environ.get("GITHUB_TOKEN"))
    args = ap.parse_args()

    if not args.token:
        sys.exit("Set GITHUB_TOKEN env var or pass --token")
    if not os.path.exists(args.file):
        sys.exit(f"File not found: {args.file}")

    release = get_or_create_release(args.repo, args.tag, args.token)
    upload_asset(args.repo, release, args.file, args.token)


if __name__ == "__main__":
    main()
