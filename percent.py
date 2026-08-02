# scripts/github_pipeline_sqlite.py
"""
Fast GitHub metadata pipeline (async edition):

1) Scan parquet (one part) -> SQLite repo_ids
2) N tokens run concurrently, each with up to 3 in-flight requests
3) Forks -> resolve source/parent -> store CANONICAL only in metadata
4) All durable state in SQLite3 (WAL + write lock)

Same schema / same DB file as before -> fully resumable. Nothing about
your existing progress (pending/done/error/in_progress rows, metadata,
forks) is touched except that any stuck 'in_progress' rows from a
previous crashed/killed run get put back to 'pending' on startup, same
as before.

Tables:
  repo_ids   - queue from parquet (pending/done/error)
  metadata   - canonical non-fork repos only
  forks      - fork ids + resolution

Env:
  GITHUB_TOKENS=tok1,tok2,tok3
  or GITHUB_TOKEN=tok1

Usage:
  # 1) ingest parquet headers / counts into DB
  python scripts/github_pipeline_sqlite.py ingest --part 0

  # 2) process pending ids, all tokens in parallel, 3 concurrent req/token
  python scripts/github_pipeline_sqlite.py run --min-files 2 --max-files 80

  # resume anytime, safe to Ctrl+C and rerun
  python scripts/github_pipeline_sqlite.py run

  # stats
  python scripts/github_pipeline_sqlite.py status
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sqlite3
import threading
import time
from pathlib import Path

import aiohttp

try:
    import pyarrow.parquet as pq
except ImportError:
    pq = None

API = "https://api.github.com"
DEFAULT_DB = Path("github_pipeline.db")
REPO = "PERDYPTO/Python_data_set"
PART_FMT = (
    "language=Python/"
    "part-{part:05d}-147a722f-59fd-4e19-9d9a-9a37ea61b943-c000.snappy.parquet"
)

# Concurrent in-flight requests per token. GitHub's abuse-detection cares
# about concurrency + burst rate, not just points/hour, so 3 is a safe
# number that keeps you well inside limits without risking a token being
# flagged/revoked.
PER_TOKEN_CONCURRENCY = 3

# How many pending rows a worker claims from SQLite in one locked
# transaction before going back to the DB (reduces lock contention a lot
# at high concurrency).
CLAIM_BATCH_SIZE = 25

try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).resolve().parent / ".env")  # scripts/.env
    load_dotenv(Path.cwd() / ".env")  # project root .env
except ImportError:
    pass

# ---------------------------------------------------------------------------
# Tokens
# ---------------------------------------------------------------------------

def load_tokens() -> list[str]:
    multi = os.environ.get("GITHUB_TOKENS", "").strip()
    if multi:
        return [t.strip() for t in multi.split(",") if t.strip()]
    one = (os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN") or "").strip()
    return [one] if one else []


def auth_headers(token: str) -> dict:
    h = {
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
        "User-Agent": "python-dataset-pipeline",
    }
    if token:
        h["Authorization"] = f"Bearer {token}"
    return h


# ---------------------------------------------------------------------------
# SQLite (unchanged schema; all access serialized through one lock + used
# from async code via asyncio.to_thread so the event loop never blocks)
# ---------------------------------------------------------------------------

SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;

CREATE TABLE IF NOT EXISTS repo_ids (
    repo_id           INTEGER PRIMARY KEY,
    file_count_part   INTEGER DEFAULT 0,
    bytes_part        INTEGER DEFAULT 0,
    status            TEXT NOT NULL DEFAULT 'pending',
    -- pending | in_progress | done | error | skipped_filter
    last_error        TEXT,
    updated_at        TEXT
);

CREATE TABLE IF NOT EXISTS metadata (
    repo_id              INTEGER PRIMARY KEY,
    full_name            TEXT,
    html_url             TEXT,
    description          TEXT,
    default_branch       TEXT,
    size_kb              INTEGER,
    stars                INTEGER,
    forks_count          INTEGER,
    open_issues          INTEGER,
    license               TEXT,
    archived             INTEGER,
    created_at           TEXT,
    updated_at           TEXT,
    pushed_at            TEXT,
    languages_json       TEXT,
    python_bytes         INTEGER,
    total_lang_bytes     INTEGER,
    python_pct           REAL,
    file_count_part      INTEGER,
    bytes_part           INTEGER,
    resolved_from_fork_id INTEGER,
    fetched_at           TEXT
);

CREATE TABLE IF NOT EXISTS forks (
    fork_repo_id           INTEGER PRIMARY KEY,
    fork_full_name         TEXT,
    fork_html_url          TEXT,
    parent_id              INTEGER,
    parent_full_name       TEXT,
    source_id              INTEGER,
    source_full_name       TEXT,
    resolved_canonical_id  INTEGER,
    resolution             TEXT,
    updated_at             TEXT
);

CREATE INDEX IF NOT EXISTS idx_repo_ids_status ON repo_ids(status);
CREATE INDEX IF NOT EXISTS idx_metadata_python_pct ON metadata(python_pct DESC);
"""


class DB:
    """Sync sqlite wrapper. Call from async code via asyncio.to_thread()."""

    def __init__(self, path: Path):
        self.path = path
        self._lock = threading.Lock()
        self.conn = sqlite3.connect(str(path), check_same_thread=False, timeout=60)
        self.conn.row_factory = sqlite3.Row
        with self._lock:
            self.conn.executescript(SCHEMA)
            self.conn.commit()

    def execute(self, sql: str, params=()):
        with self._lock:
            cur = self.conn.execute(sql, params)
            self.conn.commit()
            return cur

    def executemany(self, sql: str, seq):
        with self._lock:
            self.conn.executemany(sql, seq)
            self.conn.commit()

    def query(self, sql: str, params=()):
        with self._lock:
            return self.conn.execute(sql, params).fetchall()

    def query_one(self, sql: str, params=()):
        with self._lock:
            return self.conn.execute(sql, params).fetchone()

    def claim_batch(self, n: int, min_files: int, max_files: int | None) -> list[int]:
        """Atomically claim up to n pending rows, mark them in_progress,
        return their ids. Single locked transaction -> low contention
        even with many concurrent workers."""
        with self._lock:
            cur = self.conn.execute(
                """
                SELECT repo_id FROM repo_ids
                WHERE status = 'pending'
                  AND file_count_part >= ?
                  AND (? IS NULL OR file_count_part <= ?)
                ORDER BY file_count_part DESC
                LIMIT ?
                """,
                (min_files, max_files, max_files, n),
            )
            ids = [int(r["repo_id"]) for r in cur.fetchall()]
            if ids:
                ts = now()
                self.conn.executemany(
                    "UPDATE repo_ids SET status='in_progress', updated_at=? WHERE repo_id=?",
                    [(ts, rid) for rid in ids],
                )
                self.conn.commit()
            return ids


def now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


# ---------------------------------------------------------------------------
# Ingest parquet -> repo_ids  (unchanged, still sync/one-shot)
# ---------------------------------------------------------------------------

def cmd_ingest(args):
    if pq is None:
        raise SystemExit("pip install pyarrow")

    db = DB(args.db)
    rel = args.file or PART_FMT.format(part=args.part)
    uri = f"hf://buckets/{REPO}/{rel}"
    print(f"Scanning {uri}")

    pf = pq.ParquetFile(uri)
    counts: dict[int, list[int]] = {}  # rid -> [file_count, bytes]

    total_rows = 0
    for rg in range(pf.metadata.num_row_groups):
        table = pf.read_row_group(rg, columns=["size_bytes", "repo_ids"])
        for row in table.to_pylist():
            total_rows += 1
            size = int(row.get("size_bytes") or 0)
            for rid in row.get("repo_ids") or []:
                rid = int(rid)
                if rid not in counts:
                    counts[rid] = [0, 0]
                counts[rid][0] += 1
                counts[rid][1] += size
        print(f"  row_group {rg + 1}/{pf.metadata.num_row_groups}  rows~{total_rows:,}  ids~{len(counts):,}")

    rows = [(rid, fc, b, "pending", None, now()) for rid, (fc, b) in counts.items()]
    db.executemany(
        """
        INSERT OR IGNORE INTO repo_ids
        (repo_id, file_count_part, bytes_part, status, last_error, updated_at)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    db.executemany(
        """
        UPDATE repo_ids
        SET file_count_part = ?, bytes_part = ?, updated_at = ?
        WHERE repo_id = ?
        """,
        [(fc, b, now(), rid) for rid, (fc, b) in counts.items()],
    )
    print(f"Ingested/updated {len(counts):,} distinct repo_ids  (rows scanned {total_rows:,})")
    cmd_status(args)


# ---------------------------------------------------------------------------
# GitHub fetch helpers (async)
# ---------------------------------------------------------------------------

class RateLimiter:
    """Per-token adaptive throttle. No blanket sleeps: we only pause when
    we're actually told to (403 rate-limit, secondary rate-limit, or
    remaining-quota headers getting low)."""

    def __init__(self):
        self._reset_at: float = 0.0
        self._lock = asyncio.Lock()

    async def wait_if_needed(self):
        async with self._lock:
            wait = self._reset_at - time.time()
        if wait > 0:
            await asyncio.sleep(min(wait, 120) + 1)

    def note_response(self, status: int, headers) -> bool:
        """Returns True if this response indicates we should back off and
        retry (rate limited / transient server error)."""
        remaining = headers.get("X-RateLimit-Remaining")
        reset = headers.get("X-RateLimit-Reset")
        if status == 403 and remaining == "0" and reset:
            self._reset_at = max(self._reset_at, float(reset))
            return True
        if status == 403 and "retry-after" in {k.lower() for k in headers.keys()}:
            retry_after = float(headers.get("Retry-After", 5))
            self._reset_at = max(self._reset_at, time.time() + retry_after)
            return True
        if status in (502, 503, 504):
            # transient - short backoff, not a full rate-limit wait
            self._reset_at = max(self._reset_at, time.time() + 3)
            return True
        # Proactively ease off before actually hitting zero, to reduce
        # abuse-detection risk, but don't sleep on every call.
        if remaining is not None:
            try:
                rem = int(remaining)
                if rem <= 3 and reset:
                    self._reset_at = max(self._reset_at, float(reset))
            except ValueError:
                pass
        return False

    @property
    def reset_at(self) -> float:
        return self._reset_at

    def is_exhausted(self) -> bool:
        """True if this token is currently waiting out a rate limit."""
        return self._reset_at > time.time()


async def http_get(
    session: aiohttp.ClientSession,
    limiter: RateLimiter,
    sem: asyncio.Semaphore,
    url: str,
    max_retries: int = 8,
):
    for attempt in range(max_retries):
        await limiter.wait_if_needed()
        try:
            async with sem:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=60)) as r:
                    body = await r.read()
                    should_retry = limiter.note_response(r.status, r.headers)
                    if should_retry:
                        continue
                    return r.status, body
        except (aiohttp.ClientError, asyncio.TimeoutError):
            await asyncio.sleep(2 ** min(attempt, 5))
            continue
    return None, None


def python_pct(langs: dict) -> float:
    if not langs:
        return 0.0
    t = sum(langs.values())
    return (100.0 * langs.get("Python", 0) / t) if t else 0.0


async def fetch_repo(session, limiter, sem, rid: int):
    status, body = await http_get(session, limiter, sem, f"{API}/repositories/{rid}")
    if status is None:
        return "error", None
    if status in (404, 451):
        return "not_found", None
    if status != 200:
        return "error", None
    try:
        return "ok", json.loads(body)
    except json.JSONDecodeError:
        return "error", None


async def fetch_langs(session, limiter, sem, rid: int) -> dict:
    status, body = await http_get(session, limiter, sem, f"{API}/repositories/{rid}/languages")
    if status != 200 or body is None:
        return {}
    try:
        return json.loads(body) or {}
    except json.JSONDecodeError:
        return {}


def usable_canonical(j: dict, allow_archived: bool) -> tuple[bool, str]:
    if j.get("fork"):
        return False, "still_fork"
    if j.get("disabled"):
        return False, "disabled"
    if not allow_archived and j.get("archived"):
        return False, "archived"
    if not j.get("full_name"):
        return False, "no_name"
    return True, "ok"


def upstream_ids(fork_j: dict) -> list[int]:
    out = []
    for key in ("source", "parent"):
        block = fork_j.get(key) or {}
        uid = block.get("id")
        if uid is not None:
            uid = int(uid)
            if uid not in out:
                out.append(uid)
    return out


# ---------------------------------------------------------------------------
# DB write helpers (sync, called via asyncio.to_thread)
# ---------------------------------------------------------------------------

def save_metadata(db: DB, rid: int, j: dict, langs: dict, part: dict, from_fork: int | None):
    total = sum(langs.values()) if langs else 0
    py = langs.get("Python", 0) if langs else 0
    db.execute(
        """
        INSERT OR REPLACE INTO metadata (
            repo_id, full_name, html_url, description, default_branch,
            size_kb, stars, forks_count, open_issues, license, archived,
            created_at, updated_at, pushed_at,
            languages_json, python_bytes, total_lang_bytes, python_pct,
            file_count_part, bytes_part, resolved_from_fork_id, fetched_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            rid,
            j.get("full_name"),
            j.get("html_url"),
            j.get("description"),
            j.get("default_branch"),
            j.get("size"),
            j.get("stargazers_count"),
            j.get("forks_count"),
            j.get("open_issues_count"),
            (j.get("license") or {}).get("spdx_id"),
            1 if j.get("archived") else 0,
            j.get("created_at"),
            j.get("updated_at"),
            j.get("pushed_at"),
            json.dumps(langs),
            py,
            total,
            round(python_pct(langs), 2),
            part.get("file_count_part"),
            part.get("bytes_part"),
            from_fork,
            now(),
        ),
    )


def save_fork(db: DB, rec: dict):
    db.execute(
        """
        INSERT OR REPLACE INTO forks (
            fork_repo_id, fork_full_name, fork_html_url,
            parent_id, parent_full_name, source_id, source_full_name,
            resolved_canonical_id, resolution, updated_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?)
        """,
        (
            rec["fork_repo_id"],
            rec.get("fork_full_name"),
            rec.get("fork_html_url"),
            rec.get("parent_id"),
            rec.get("parent_full_name"),
            rec.get("source_id"),
            rec.get("source_full_name"),
            rec.get("resolved_canonical_id"),
            rec.get("resolution"),
            now(),
        ),
    )


def mark_repo(db: DB, rid: int, status: str, err: str | None = None):
    db.execute(
        "UPDATE repo_ids SET status=?, last_error=?, updated_at=? WHERE repo_id=?",
        (status, err, now(), rid),
    )


def has_metadata(db: DB, rid: int) -> bool:
    row = db.query_one("SELECT 1 FROM metadata WHERE repo_id=?", (rid,))
    return row is not None


def get_part(db: DB, rid: int) -> dict:
    row = db.query_one(
        "SELECT file_count_part, bytes_part FROM repo_ids WHERE repo_id=?", (rid,)
    )
    return {
        "file_count_part": row["file_count_part"] if row else 0,
        "bytes_part": row["bytes_part"] if row else 0,
    }


# ---------------------------------------------------------------------------
# Checkpointing (upload db to an HF bucket on a timer, or immediately if
# several tokens go rate-limited at once)
# ---------------------------------------------------------------------------

def snapshot_db(db_path: Path) -> Path:
    """
    Create a consistent point-in-time copy of the (possibly actively being
    written) SQLite db using SQLite's own backup API, so a checkpoint
    upload never reads a half-written file. Safe to call while workers
    are still writing thanks to WAL mode.
    """
    tmp_path = db_path.with_suffix(db_path.suffix + ".checkpoint")
    if tmp_path.exists():
        tmp_path.unlink()
    src = sqlite3.connect(str(db_path))
    dst = sqlite3.connect(str(tmp_path))
    try:
        src.backup(dst)
    finally:
        dst.close()
        src.close()
    return tmp_path


def do_checkpoint_upload(db_path: Path, bucket: str):
    """Blocking: snapshot the db and upload it to the HF bucket under the
    real db's filename. Import is local so --bucket is fully optional and
    nobody without huggingface_hub installed is forced to have it."""
    from upload_db_bucket import upload_file_to_bucket

    snap = snapshot_db(db_path)
    try:
        upload_file_to_bucket(bucket, str(snap), remote_name=db_path.name)
    finally:
        try:
            snap.unlink()
        except OSError:
            pass


async def checkpoint_scheduler(
    db_path: Path,
    bucket: str,
    limiters: dict,
    interval_s: int,
    exhausted_threshold: int,
    stop_event: asyncio.Event,
    poll_s: int = 15,
    exhaustion_cooldown_s: int = 60,
):
    """
    Background task: uploads a checkpoint of the db when EITHER
      - `interval_s` seconds have passed since the last checkpoint, OR
      - at least `exhausted_threshold` tokens are currently rate-limited
        at the same time (checked every `poll_s` seconds), subject to a
        cooldown so a sustained exhaustion doesn't spam re-uploads.
    whichever happens first.
    """
    last_checkpoint = time.time()
    last_exhaustion_trigger = 0.0

    while not stop_event.is_set():
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=poll_s)
            break  # stop_event was set -> exit loop, caller does final checkpoint
        except asyncio.TimeoutError:
            pass

        now_t = time.time()
        exhausted = sum(1 for lim in limiters.values() if lim.is_exhausted())
        due_time = (now_t - last_checkpoint) >= interval_s
        due_exhaustion = (
            exhausted >= exhausted_threshold
            and (now_t - last_exhaustion_trigger) >= exhaustion_cooldown_s
        )

        if due_time or due_exhaustion:
            reason = (
                f"{exhausted}/{len(limiters)} tokens rate-limited"
                if due_exhaustion and not due_time
                else "10-min interval"
            )
            print(f"[checkpoint] uploading db ({reason})")
            try:
                await asyncio.to_thread(do_checkpoint_upload, db_path, bucket)
                last_checkpoint = time.time()
                print("[checkpoint] upload complete")
            except Exception as e:
                print(f"[checkpoint] upload FAILED: {e}")
            if due_exhaustion:
                last_exhaustion_trigger = time.time()


async def process_repo_id(rid, session, limiter, sem, db: DB, allow_archived: bool) -> str:
    part = await asyncio.to_thread(get_part, db, rid)

    st, j = await fetch_repo(session, limiter, sem, rid)
    if st != "ok" or not j:
        await asyncio.to_thread(mark_repo, db, rid, "error", st)
        return f"{rid} error:{st}"

    # Non-fork
    if not j.get("fork"):
        ok, reason = usable_canonical(j, allow_archived)
        if not ok:
            await asyncio.to_thread(mark_repo, db, rid, "done", reason)
            return f"{rid} skip:{reason}"
        has_md = await asyncio.to_thread(has_metadata, db, rid)
        if not has_md:
            langs = await fetch_langs(session, limiter, sem, rid)
            await asyncio.to_thread(save_metadata, db, rid, j, langs, part, None)
        await asyncio.to_thread(mark_repo, db, rid, "done")
        return f"{rid} CANONICAL {j.get('full_name')}"

    # Fork
    rec = {
        "fork_repo_id": rid,
        "fork_full_name": j.get("full_name"),
        "fork_html_url": j.get("html_url"),
        "parent_id": (j.get("parent") or {}).get("id"),
        "parent_full_name": (j.get("parent") or {}).get("full_name"),
        "source_id": (j.get("source") or {}).get("id"),
        "source_full_name": (j.get("source") or {}).get("full_name"),
        "resolved_canonical_id": None,
        "resolution": "pending",
    }
    uids = upstream_ids(j)
    if not uids:
        rec["resolution"] = "no_parent_or_source"
        await asyncio.to_thread(save_fork, db, rec)
        await asyncio.to_thread(mark_repo, db, rid, "done", rec["resolution"])
        return f"{rid} FORK orphan"

    for uid in uids:
        has_md = await asyncio.to_thread(has_metadata, db, uid)
        if has_md:
            rec["resolved_canonical_id"] = uid
            rec["resolution"] = "already_in_metadata"
            await asyncio.to_thread(save_fork, db, rec)
            await asyncio.to_thread(mark_repo, db, rid, "done")
            return f"{rid} FORK -> existing {uid}"

        ust, uj = await fetch_repo(session, limiter, sem, uid)
        if ust != "ok" or not uj:
            continue
        ok, reason = usable_canonical(uj, allow_archived)
        if not ok:
            rec["resolution"] = f"upstream_{uid}_{reason}"
            continue
        langs = await fetch_langs(session, limiter, sem, uid)
        await asyncio.to_thread(save_metadata, db, uid, uj, langs, part, rid)
        rec["resolved_canonical_id"] = uid
        rec["resolution"] = "saved_upstream"
        await asyncio.to_thread(save_fork, db, rec)
        await asyncio.to_thread(mark_repo, db, rid, "done")
        return f"{rid} FORK -> {uj.get('full_name')} ({uid})"

    if rec["resolution"] == "pending":
        rec["resolution"] = "upstream_unavailable"
    await asyncio.to_thread(save_fork, db, rec)
    await asyncio.to_thread(mark_repo, db, rid, "done", rec["resolution"])
    return f"{rid} FORK upstream fail"


async def token_worker(
    token: str,
    db: DB,
    allow_archived: bool,
    min_files: int,
    max_files: int | None,
    worker_id: int,
    limiters: dict | None = None,
):
    label = "anon" if not token else f"{token[:4]}...{token[-4:]}"
    limiter = RateLimiter()
    if limiters is not None:
        limiters[worker_id] = limiter
    sem = asyncio.Semaphore(PER_TOKEN_CONCURRENCY)
    connector = aiohttp.TCPConnector(limit=PER_TOKEN_CONCURRENCY)
    processed = 0

    async with aiohttp.ClientSession(headers=auth_headers(token), connector=connector) as session:
        while True:
            ids = await asyncio.to_thread(db.claim_batch, CLAIM_BATCH_SIZE, min_files, max_files)
            if not ids:
                break

            async def run_one(rid):
                nonlocal processed
                try:
                    msg = await process_repo_id(rid, session, limiter, sem, db, allow_archived)
                    processed += 1
                    print(f"[w{worker_id}:{label}] {msg}")
                except Exception as e:
                    await asyncio.to_thread(mark_repo, db, rid, "error", str(e)[:500])
                    print(f"[w{worker_id}:{label}] {rid} EXC {e}")

            # up to PER_TOKEN_CONCURRENCY in flight at once for this token
            await asyncio.gather(*(run_one(rid) for rid in ids))

    print(f"[w{worker_id}:{label}] finished, processed={processed}")


async def run_async(args, tokens: list[str]):
    db = DB(args.db)

    if args.max_files is not None:
        db.execute(
            """
            UPDATE repo_ids SET status='skipped_filter', updated_at=?
            WHERE status='pending' AND file_count_part > ?
            """,
            (now(), args.max_files),
        )
    db.execute(
        """
        UPDATE repo_ids SET status='skipped_filter', updated_at=?
        WHERE status='pending' AND file_count_part < ?
        """,
        (now(), args.min_files),
    )
    # Recover stuck in_progress rows from a previous crashed/interrupted run.
    # Nothing else changes -> your current 'done'/'error'/'metadata'/'forks'
    # rows are untouched and the run resumes from where it stopped.
    db.execute("UPDATE repo_ids SET status='pending' WHERE status='in_progress'")

    limiters: dict = {}
    stop_event = asyncio.Event()
    checkpoint_task = None
    if args.bucket:
        checkpoint_task = asyncio.create_task(
            checkpoint_scheduler(
                args.db,
                args.bucket,
                limiters,
                args.checkpoint_interval,
                args.checkpoint_exhausted,
                stop_event,
            )
        )

    try:
        await asyncio.gather(
            *(
                token_worker(
                    tokens[i], db, args.allow_archived, args.min_files, args.max_files, i, limiters
                )
                for i in range(len(tokens))
            )
        )
    finally:
        if checkpoint_task:
            stop_event.set()
            await checkpoint_task
            # Always do one final checkpoint at the very end, regardless of
            # timer/exhaustion state, so a run that finishes early (queue
            # drained) or gets cut off still uploads its last state.
            print("[checkpoint] final upload before exit")
            try:
                await asyncio.to_thread(do_checkpoint_upload, args.db, args.bucket)
                print("[checkpoint] final upload complete")
            except Exception as e:
                print(f"[checkpoint] final upload FAILED: {e}")


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------

def cmd_run(args):
    tokens = load_tokens()
    if not tokens:
        print("WARNING: no tokens -- 1 anonymous worker (60 req/hr)")
        tokens = [""]
    else:
        print(f"Tokens = {len(tokens)}, {PER_TOKEN_CONCURRENCY} concurrent req/token "
              f"-> up to {len(tokens) * PER_TOKEN_CONCURRENCY} concurrent requests total")

    asyncio.run(run_async(args, tokens))
    cmd_status(args)


def cmd_status(args):
    db = DB(args.db)
    print("=== repo_ids ===")
    for row in db.query(
        "SELECT status, COUNT(*) AS c FROM repo_ids GROUP BY status ORDER BY c DESC"
    ):
        print(f"  {row['status']}: {row['c']}")
    m = db.query_one("SELECT COUNT(*) AS c FROM metadata")
    f = db.query_one("SELECT COUNT(*) AS c FROM forks")
    print(f"=== metadata (canonical): {m['c']} ===")
    print(f"=== forks logged: {f['c']} ===")
    print("Top python_pct:")
    for row in db.query(
        "SELECT repo_id, full_name, python_pct, stars, size_kb FROM metadata "
        "ORDER BY python_pct DESC, stars DESC LIMIT 15"
    ):
        print(
            f"  {row['python_pct']:6.2f}%  stars={row['stars']}  "
            f"size_kb={row['size_kb']}  {row['full_name']}"
        )


def cmd_export(args):
    """Optional: dump metadata to JSON for the rest of the pipeline."""
    db = DB(args.db)
    rows = db.query("SELECT * FROM metadata ORDER BY python_pct DESC")
    out = []
    for r in rows:
        d = dict(r)
        d["languages"] = json.loads(d.pop("languages_json") or "{}")
        d["fork"] = False
        out.append(d)
    path = Path(args.out)
    path.write_text(json.dumps({"count": len(out), "repos": out}, indent=2), encoding="utf-8")
    print(f"Wrote {len(out)} rows -> {path}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", type=Path, default=DEFAULT_DB)
    sub = ap.add_subparsers(dest="cmd", required=True)

    p_ing = sub.add_parser("ingest", help="Parquet -> repo_ids table")
    p_ing.add_argument("--part", type=int, default=0)
    p_ing.add_argument("--file", type=str, default=None)

    p_run = sub.add_parser("run", help="Async concurrent GitHub fetch")
    p_run.add_argument("--min-files", type=int, default=2)
    p_run.add_argument("--max-files", type=int, default=80)
    p_run.add_argument("--allow-archived", action="store_true")
    p_run.add_argument(
        "--bucket", type=str, default=None,
        help="HF bucket (namespace/name) to auto-checkpoint the db to, e.g. PERDYPTO/db_snapshot. "
             "If omitted, no automatic checkpointing happens."
    )
    p_run.add_argument(
        "--checkpoint-interval", type=int, default=600,
        help="Max seconds between automatic checkpoint uploads (default 600 = 10 min)."
    )
    p_run.add_argument(
        "--checkpoint-exhausted", type=int, default=3,
        help="Trigger an immediate checkpoint upload as soon as this many tokens are "
             "simultaneously rate-limited (default 3)."
    )

    sub.add_parser("status")

    p_exp = sub.add_parser("export")
    p_exp.add_argument("--out", default="repo_metadata.json")

    args = ap.parse_args()
    if args.cmd == "ingest":
        cmd_ingest(args)
    elif args.cmd == "run":
        cmd_run(args)
    elif args.cmd == "status":
        cmd_status(args)
    elif args.cmd == "export":
        cmd_export(args)


if __name__ == "__main__":
    main()
