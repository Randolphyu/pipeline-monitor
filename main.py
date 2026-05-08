"""
DSCI 551: Data Pipeline Monitoring & Lineage Tracking System

Usage:
  python main.py lineage  --table <table_name>
  python main.py lineage  --pipeline-id <id>
  python main.py inspect  --pipeline-id <id> [--days <n>]
  python main.py overview [--days <n>]
"""

import argparse
import threading
import time
from datetime import datetime, timedelta

import psycopg2
from psycopg2.extras import RealDictCursor

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────
DB_CONFIG = {
    "dbname": "dsci551",
    "user":   "randolphyu",
    "password": "",
    "host":   "localhost",
    "port":   5432,
}

# ─────────────────────────────────────────────
# DISPLAY HELPERS
# ─────────────────────────────────────────────
BOLD   = "\033[1m"
CYAN   = "\033[96m"
GREEN  = "\033[92m"
YELLOW = "\033[93m"
RED    = "\033[91m"
RESET  = "\033[0m"

def header(title):
    bar = "=" * 60
    print(f"\n{BOLD}{CYAN}{bar}{RESET}")
    print(f"{BOLD}{CYAN}  {title}{RESET}")
    print(f"{BOLD}{CYAN}{bar}{RESET}\n")

def section(title):
    print(f"\n{BOLD}{YELLOW}── {title} {'─' * (50 - len(title))}{RESET}")

def internals_box(label, text):
    print(f"\n{BOLD}{GREEN}[DB INTERNAL — {label}]{RESET}")
    for line in text.strip().split("\n"):
        print(f"  {line}")

def get_conn():
    return psycopg2.connect(**DB_CONFIG)


# ─────────────────────────────────────────────
# FEATURE 1 — LINEAGE TRAVERSAL
# DB Internal: Recursive CTE work table
# ─────────────────────────────────────────────
def feature_lineage(table_name: str = None, pipeline_id: int = None):
    header("FEATURE 1 — Lineage Traversal")

    with get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:

            # ── If pipeline_id given, resolve to tables first ──
            if pipeline_id:
                cur.execute("""
                    SELECT DISTINCT upstream_table, downstream_table
                    FROM lineage_edges
                    WHERE pipeline_id = %(pid)s
                    LIMIT 1;
                """, {"pid": pipeline_id})
                row = cur.fetchone()
                if not row:
                    print(f"  {RED}No lineage edges found for pipeline_id={pipeline_id}.{RESET}")
                    return
                table_name = row["downstream_table"]
                print(f"Pipeline {pipeline_id} → resolved to table: {BOLD}{table_name}{RESET}\n")
            else:
                print(f"Target table : {BOLD}{table_name}{RESET}\n")

            # ── Upstream CTE ───────────────────────────────────
            upstream_query = """
                WITH RECURSIVE upstream AS (
                    SELECT upstream_table, downstream_table, 1 AS depth,
                           ARRAY[%(table)s]::varchar[] AS visited
                    FROM   lineage_edges
                    WHERE  downstream_table = %(table)s
                    UNION ALL
                    SELECT e.upstream_table, e.downstream_table, u.depth + 1,
                           u.visited || e.upstream_table
                    FROM   lineage_edges e
                    JOIN   upstream u ON e.downstream_table = u.upstream_table
                    WHERE  u.depth < 10
                      AND  e.upstream_table != ALL(u.visited)
                )
                SELECT DISTINCT upstream_table, downstream_table, depth
                FROM   upstream
                ORDER  BY depth, upstream_table;
            """

            # ── Downstream CTE ─────────────────────────────────
            downstream_query = """
                WITH RECURSIVE downstream AS (
                    SELECT upstream_table, downstream_table, 1 AS depth,
                           ARRAY[%(table)s]::varchar[] AS visited
                    FROM   lineage_edges
                    WHERE  upstream_table = %(table)s
                    UNION ALL
                    SELECT e.upstream_table, e.downstream_table, d.depth + 1,
                           d.visited || e.downstream_table
                    FROM   lineage_edges e
                    JOIN   downstream d ON e.upstream_table = d.downstream_table
                    WHERE  d.depth < 10
                      AND  e.downstream_table != ALL(d.visited)
                )
                SELECT DISTINCT upstream_table, downstream_table, depth
                FROM   downstream
                ORDER  BY depth, downstream_table;
            """

            # ── Schema versions for related tables ────────────
            schema_query = """
                SELECT dataset_name, schema_version, source_system
                FROM   datasets
                WHERE  dataset_name = ANY(%(tables)s)
                ORDER  BY schema_version DESC;
            """

            cur.execute(upstream_query, {"table": table_name})
            upstream_rows = cur.fetchall()

            cur.execute(downstream_query, {"table": table_name})
            downstream_rows = cur.fetchall()

            # ── Collect all related table names ────────────────
            all_tables = {table_name}
            for r in upstream_rows + downstream_rows:
                all_tables.add(r["upstream_table"])
                all_tables.add(r["downstream_table"])

            cur.execute(schema_query, {"tables": list(all_tables)})
            schema_map = {r["dataset_name"]: r for r in cur.fetchall()}

            # ── Print full lineage chain ───────────────────────
            section("Full Lineage Chain")

            # upstream (print reversed so source is at top)
            if upstream_rows:
                max_depth = max(r["depth"] for r in upstream_rows)
                printed = set()
                for depth in range(max_depth, 0, -1):
                    for r in upstream_rows:
                        if r["depth"] == depth and r["upstream_table"] not in printed:
                            indent = "  " * (max_depth - depth)
                            sv = schema_map.get(r["upstream_table"])
                            ver = f"v{sv['schema_version']}" if sv else ""
                            ver_color = RED if sv and sv["schema_version"] >= 4 else YELLOW
                            print(f"  {indent}{r['upstream_table']} "
                                  f"{ver_color}{ver}{RESET}")
                            printed.add(r["upstream_table"])

            # target table
            sv = schema_map.get(table_name)
            ver = f"v{sv['schema_version']}" if sv else ""
            print(f"  {BOLD}{CYAN}▶ {table_name} {ver} ← YOU ARE HERE{RESET}")

            # downstream
            if downstream_rows:
                printed = set()
                for depth in range(1, max(r["depth"] for r in downstream_rows) + 1):
                    for r in downstream_rows:
                        if r["depth"] == depth and r["downstream_table"] not in printed:
                            indent = "  " * depth
                            sv = schema_map.get(r["downstream_table"])
                            ver = f"v{sv['schema_version']}" if sv else ""
                            ver_color = RED if sv and sv["schema_version"] >= 4 else YELLOW
                            print(f"  {indent}{r['downstream_table']} "
                                  f"{ver_color}{ver}{RESET}")
                            printed.add(r["downstream_table"])

            if not upstream_rows and not downstream_rows:
                print(f"  {RED}No lineage edges found for '{table_name}'.{RESET}")
                return

            print(f"\n  Upstream edges   : {len(upstream_rows)}")
            print(f"  Downstream edges : {len(downstream_rows)}")

            # ── Schema version warnings ────────────────────────
            section("Schema Version Check")
            flagged = [(name, info) for name, info in schema_map.items()
                       if info["schema_version"] >= 4]
            if not flagged:
                print(f"  {GREEN}All related tables have stable schema versions.{RESET}")
            else:
                print(f"  {RED}⚠ {len(flagged)} table(s) with high schema version (>=4):{RESET}\n")
                for name, info in flagged:
                    print(f"  {RED}{name:<35}{RESET} "
                          f"v{info['schema_version']}  source={info['source_system']}")

            # ── EXPLAIN ANALYZE ────────────────────────────────
            section("EXPLAIN ANALYZE — Recursive CTE (upstream traversal)")
            cur.execute(
                "EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT) " + upstream_query,
                {"table": table_name}
            )
            print("\n".join(r["QUERY PLAN"] for r in cur.fetchall()))

            internals_box("Recursive CTE", """
What PostgreSQL does internally:
  1. ANCHOR MEMBER executes first → seed rows written into a 'work table'
  2. RECURSIVE MEMBER reads the work table, joins lineage_edges → new rows
  3. New rows appended to an 'intermediate table' (accumulates all iterations)
  4. Work table is replaced by this iteration's output → next round begins
  5. Process stops when no new rows produced, or depth / visited blocks it
  6. Final result = CTE Scan over the intermediate table

Cycle prevention (two layers):
  • visited array: each row carries nodes already seen on its path
    → upstream_table != ALL(visited) stops cycles at iteration level
  • depth < 10: hard safety cap, not the primary cycle guard

Note on index usage:
  • lineage_edges has only 200 rows → planner correctly chooses Seq Scan
  • At production scale, a B-tree index on (downstream_table, upstream_table)
    would replace this with Index Scan per iteration
  • MVCC ensures concurrent edge inserts never block this read query
            """)




# ─────────────────────────────────────────────
# FEATURE 2 — PIPELINE INSPECT
# DB Internal: B-tree index (pipeline_id) + heap storage
# ─────────────────────────────────────────────
def feature_inspect(pipeline_id: int, days: int = 7):
    header("FEATURE 2 — Pipeline Inspect")
    print(f"Pipeline ID  : {BOLD}{pipeline_id}{RESET}")
    print(f"Time window  : last {days} day(s)\n")

    since = datetime.now() - timedelta(days=days)

    stats_query = """
        SELECT
            p.pipeline_name,
            p.owner,
            p.schedule,
            COUNT(*)                                          AS total_runs,
            SUM(CASE WHEN e.status = 'success' THEN 1 END)   AS successes,
            SUM(CASE WHEN e.status = 'failed'  THEN 1 END)   AS failures,
            ROUND(AVG(e.duration_secs))                       AS avg_duration_secs,
            MAX(e.started_at)                                 AS last_run
        FROM   pipeline_executions e
        JOIN   pipelines p USING (pipeline_id)
        WHERE  e.pipeline_id = %(pid)s
          AND  e.started_at >= %(since)s
        GROUP  BY p.pipeline_name, p.owner, p.schedule;
    """

    recent_query = """
        SELECT status, started_at, duration_secs, rows_processed
        FROM   pipeline_executions
        WHERE  pipeline_id = %(pid)s
          AND  started_at >= %(since)s
        ORDER  BY started_at DESC
        LIMIT  10;
    """

    explain_btree = """
        EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
        SELECT COUNT(*) FROM pipeline_executions
        WHERE pipeline_id = %(pid)s;
    """

    with get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:

            # ── Pipeline summary ──────────────────────────────
            section("Pipeline Summary")
            cur.execute(stats_query, {"pid": pipeline_id, "since": since})
            row = cur.fetchone()
            if not row:
                print(f"  {RED}No executions found for pipeline_id={pipeline_id} "
                      f"in last {days} days.{RESET}")
                print("  Tip: try --days 365 for a wider window.")
                return

            failure_rate = (row["failures"] or 0) / row["total_runs"] * 100
            status_color = RED if failure_rate > 10 else GREEN

            print(f"  Pipeline     : {row['pipeline_name']}")
            print(f"  Owner        : {row['owner']}")
            print(f"  Schedule     : {row['schedule']}")
            print(f"  Total runs   : {row['total_runs']:,}")
            print(f"  Successes    : {GREEN}{row['successes']:,}{RESET}")
            print(f"  Failures     : {status_color}{row['failures'] or 0:,}{RESET}"
                  f"  ({failure_rate:.1f}%)")
            print(f"  Avg duration : {row['avg_duration_secs']} s")
            print(f"  Last run     : {row['last_run']}")

            # ── Recent runs ───────────────────────────────────
            section("Last 10 Runs")
            cur.execute(recent_query, {"pid": pipeline_id, "since": since})
            runs = cur.fetchall()
            print(f"  {'Status':<10} {'Started At':<28} {'Duration(s)':<14} {'Rows Processed'}")
            print(f"  {'-'*9} {'-'*27} {'-'*13} {'-'*15}")
            for r in runs:
                sc = GREEN if r["status"] == "success" else RED
                rows_proc = f"{r['rows_processed']:,}" if r["rows_processed"] else "N/A"
                dur = r["duration_secs"] if r["duration_secs"] else "—"
                print(f"  {sc}{r['status']:<10}{RESET} {str(r['started_at']):<28} "
                      f"{str(dur):<14} {rows_proc}")

            # ── EXPLAIN ANALYZE ───────────────────────────────
            section("EXPLAIN ANALYZE — B-tree Index on pipeline_id")
            cur.execute(explain_btree, {"pid": pipeline_id})
            print("\n".join(r["QUERY PLAN"] for r in cur.fetchall()))

            internals_box("B-tree Index + Heap Storage", """
What PostgreSQL does internally:
  • Table data (heap) is stored in 8KB pages on disk
  • B-tree index on pipeline_id stores (pipeline_id, tid) pairs in a
    balanced tree — tid is a pointer to the exact heap page + row offset
  • For pipeline_id = X: O(log n) traversal down the B-tree to the leaf,
    then follow tid pointers to fetch matching heap pages directly
  • After VACUUM updates the visibility map → Index Only Scan:
    PostgreSQL reads execution_ids directly from the index,
    skipping heap page fetches entirely (Heap Fetches ≈ 0)

Why B-tree here (not BRIN):
  • pipeline_id values are scattered across the heap in random order
    (each INSERT picks a random pipeline) — no physical ordering to exploit
  • BRIN would fail: every block range contains all pipeline_ids →
    no blocks can be skipped → equivalent to a full seq scan
  • B-tree works regardless of physical ordering → correct choice here
            """)

            # ── Tip ───────────────────────────────────────────
            print(f"\n  {BOLD}Tip: trace which tables this pipeline affects:{RESET}")
            print(f"  python main.py lineage --pipeline-id {pipeline_id}\n")


# ─────────────────────────────────────────────
# FEATURE 3 — PIPELINE HEALTH OVERVIEW
# DB Internal: BRIN index (started_at) + MVCC
# ─────────────────────────────────────────────
SCHEDULE_INTERVAL = {
    "@hourly":      3600,
    "@daily":       86400,
    "@weekly":      604800,
    "0 2 * * *":    86400,
    "0 */6 * * *":  21600,
    "*/30 * * * *": 1800,
}

def feature_overview(days: int = 1):
    header("FEATURE 3 — Pipeline Health Overview")
    print(f"Time window  : last {days} day(s)\n")

    since = datetime.now() - timedelta(days=days)

    # Uses BRIN index on started_at — time-range scan across ALL pipelines
    overview_query = """
        SELECT
            p.pipeline_id,
            p.pipeline_name,
            p.owner,
            p.schedule,
            COUNT(*)                                                AS total_runs,
            SUM(CASE WHEN e.status = 'failed'  THEN 1 ELSE 0 END)  AS failures,
            ROUND(AVG(e.duration_secs))                             AS avg_duration_secs,
            MAX(e.started_at)                                       AS last_run,
            MAX(e.started_at) FILTER (WHERE e.status = 'success')   AS last_success
        FROM   pipeline_executions e
        JOIN   pipelines p USING (pipeline_id)
        WHERE  e.started_at >= %(since)s
        GROUP  BY p.pipeline_id, p.pipeline_name, p.owner, p.schedule
        ORDER  BY failures DESC, last_run ASC;
    """

    explain_brin = """
        EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
        SELECT COUNT(*) FROM pipeline_executions
        WHERE started_at >= %(since)s;
    """

    # MVCC: show xmin/xmax on most recently inserted rows
    mvcc_query = """
        SELECT execution_id, pipeline_id, status, started_at, xmin, xmax
        FROM   pipeline_executions
        ORDER  BY execution_id DESC
        LIMIT  5;
    """

    with get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:

            cur.execute(overview_query, {"since": since})
            rows = cur.fetchall()

            if not rows:
                print(f"  {RED}No executions found in last {days} days.{RESET}")
                print(f"  Tip: try --days 30 for a wider window.")
                return

            failed = [r for r in rows
                if (r["failures"] or 0) / r["total_runs"] > 0.10]

            healthy_candidates = [r for r in rows
                if (r["failures"] or 0) / r["total_runs"] <= 0.10]
            stale   = []
            healthy = []

            for r in healthy_candidates:
                 expected = SCHEDULE_INTERVAL.get(r["schedule"])
                 if expected and r["last_success"]:
                    overdue = (datetime.now() - r["last_success"]).total_seconds()
                    if overdue > expected * 1.5:
                        stale.append((r, overdue))
                    else:
                        healthy.append(r)
                 else:
                    healthy.append(r)

            # ── Failed ────────────────────────────────────────
            section(f"FAILED — {len(failed)} pipeline(s)")
            if not failed:
                print(f"  {GREEN}No failures detected.{RESET}")
            else:
                print(f"  {'ID':<5} {'Pipeline':<35} {'Owner':<8} "
                      f"{'Failures':<10} {'Last Run'}")
                print(f"  {'-'*4} {'-'*34} {'-'*7} {'-'*9} {'-'*22}")
                for r in failed:
                    print(f"  {RED}{r['pipeline_id']:<5}{RESET} "
                          f"{r['pipeline_name']:<35} {r['owner']:<8} "
                          f"{RED}{r['failures']:<10}{RESET} {r['last_run']}")

            # ── Stale ─────────────────────────────────────────
            section(f"STALE — {len(stale)} pipeline(s) overdue")
            if not stale:
                print(f"  {GREEN}All pipelines running on schedule.{RESET}")
            else:
                print(f"  {'ID':<5} {'Pipeline':<35} {'Owner':<8} "
                      f"{'Overdue':<14} {'Schedule'}")
                print(f"  {'-'*4} {'-'*34} {'-'*7} {'-'*13} {'-'*15}")
                for r, overdue_secs in stale:
                    overdue_h = round(overdue_secs / 3600, 1)
                    print(f"  {YELLOW}{r['pipeline_id']:<5}{RESET} "
                          f"{r['pipeline_name']:<35} {r['owner']:<8} "
                          f"{YELLOW}{str(overdue_h)+'h overdue':<14}{RESET} "
                          f"{r['schedule']}")

            # ── Healthy ───────────────────────────────────────
            section(f"HEALTHY — {len(healthy)} pipeline(s)")
            print(f"  {GREEN}All running on schedule with no failures.{RESET}")

            # ── EXPLAIN ANALYZE: BRIN ─────────────────────────
            section("EXPLAIN ANALYZE — BRIN Index on started_at")
            cur.execute(explain_brin, {"since": since})
            print("\n".join(r["QUERY PLAN"] for r in cur.fetchall()))

            internals_box("BRIN Index", """
What PostgreSQL does internally:
  • BRIN stores (min, max) of started_at per 128-page block range
  • For started_at >= X, PostgreSQL reads BRIN summary and SKIPS block
    ranges whose max < X — no heap access needed for skipped ranges
  • Works because execution records are inserted in chronological order:
    physical ordering matches logical ordering → BRIN summaries are tight

Why BRIN here (not B-tree):
  • B-tree on started_at would cost ~20 MB for 1M rows
  • BRIN costs < 50 KB — 400x smaller
  • B-tree would still work, but wastes space when physical order is guaranteed
  • If records were inserted out of order, BRIN summaries would overlap →
    fewer blocks skipped → BRIN loses its advantage
            """)

            # ── MVCC ──────────────────────────────────────────
            section("MVCC — Concurrent Read/Write (xmin / xmax)")
            cur.execute(mvcc_query)
            mvcc_rows = cur.fetchall()
            print(f"  {'exec_id':<12} {'pipeline_id':<13} {'status':<10} "
                  f"{'xmin':<10} {'xmax':<10}")
            print(f"  {'-'*11} {'-'*12} {'-'*9} {'-'*9} {'-'*9}")
            for r in mvcc_rows:
                print(f"  {r['execution_id']:<12} {r['pipeline_id']:<13} "
                      f"{r['status']:<10} {r['xmin']:<10} {r['xmax']:<10}")

            internals_box("MVCC Tuple Versioning", """
What PostgreSQL does internally:
  • Every row carries two hidden system columns:
      xmin = transaction ID that INSERTED this row (always set)
      xmax = transaction ID that DELETED this row (0 = still live)
  • When pipelines continuously write execution records:
      → Each committed batch gets a unique xmin
      → Old tuples are never overwritten in-place
  • When this overview dashboard reads pipeline_executions:
      → Reader takes a snapshot of the current transaction ID
      → Only rows where xmin ≤ snapshot_txid are visible
      → Writer's uncommitted rows (xmin > snapshot) are invisible
      → NO LOCK acquired — reader and writer run fully in parallel

Why this matters for a monitoring dashboard:
  • MySQL InnoDB: row-level locks cause read/write contention on hot rows
  • PostgreSQL MVCC: readers never block writers, writers never block readers
  • The overview query always returns a consistent snapshot, even during
    peak ingestion periods when hundreds of execution records are being written
            """)

            if failed or stale:
                print(f"\n  {BOLD}Tip: drill into a specific pipeline:{RESET}")
                print(f"  python main.py inspect --pipeline-id <ID> --days 7\n")


# ─────────────────────────────────────────────
# CLI ENTRY POINT
# ─────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(
        description="DSCI 551 — Pipeline Monitor & Lineage Tracker"
    )
    sub = parser.add_subparsers(dest="command")

    # lineage
    p_lin = sub.add_parser("lineage", help="Full lineage chain for a table or pipeline")
    grp = p_lin.add_mutually_exclusive_group(required=True)
    grp.add_argument("--table", help="Target table name")
    grp.add_argument("--pipeline-id", type=int, help="Pipeline ID")

    # inspect
    p_ins = sub.add_parser("inspect", help="Drill into a specific pipeline")
    p_ins.add_argument("--pipeline-id", type=int, required=True)
    p_ins.add_argument("--days", type=int, default=7)

    # overview
    p_ov = sub.add_parser("overview", help="Health summary of all pipelines")
    p_ov.add_argument("--days", type=int, default=30)

    args = parser.parse_args()

    if args.command == "lineage":
        feature_lineage(
            table_name=args.table,
            pipeline_id=args.pipeline_id
        )
    elif args.command == "inspect":
        feature_inspect(args.pipeline_id, args.days)
    elif args.command == "overview":
        feature_overview(args.days)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()