import sqlite3

DB = "github_pipeline.db"

conn = sqlite3.connect(DB)
cur = conn.cursor()

print("=" * 80)
print("DATABASE OVERVIEW")
print("=" * 80)

tables = cur.execute("""
SELECT name
FROM sqlite_master
WHERE type='table'
ORDER BY name
""").fetchall()

for (table,) in tables:

    print("\n")
    print("=" * 80)
    print(table)
    print("=" * 80)

    rows = cur.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    print(f"Rows: {rows:,}")

    columns = cur.execute(f"PRAGMA table_info({table})").fetchall()

    for cid, name, ctype, notnull, default, pk in columns:

        print(f"\n{name} ({ctype})")

        nulls = cur.execute(
            f"SELECT COUNT(*) FROM {table} WHERE {name} IS NULL"
        ).fetchone()[0]

        distinct = cur.execute(
            f"SELECT COUNT(DISTINCT {name}) FROM {table}"
        ).fetchone()[0]

        print(f"  NULLs      : {nulls:,}")
        print(f"  Distinct   : {distinct:,}")

        if ctype.upper() in ("INTEGER","REAL"):

            mn, mx, avg = cur.execute(
                f"""
                SELECT
                    MIN({name}),
                    MAX({name}),
                    AVG({name})
                FROM {table}
                """
            ).fetchone()

            print(f"  Min        : {mn}")
            print(f"  Max        : {mx}")
            print(f"  Avg        : {avg}")

        else:

            longest = cur.execute(
                f"""
                SELECT MAX(LENGTH({name}))
                FROM {table}
                """
            ).fetchone()[0]

            print(f"  Longest len: {longest}")

        print("  Top values:")

        try:
            vals = cur.execute(f"""
                SELECT {name}, COUNT(*)
                FROM {table}
                GROUP BY {name}
                ORDER BY COUNT(*) DESC
                LIMIT 10
            """).fetchall()

            for value, count in vals:
                print(f"    {repr(value)} : {count:,}")

        except sqlite3.OperationalError:
            print("    (cannot group this column)")
