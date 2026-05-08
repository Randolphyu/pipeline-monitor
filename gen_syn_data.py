"""
DSCI 551 - Data Pipeline Monitoring & Lineage Tracking System
Synthetic Data Generation Script

Generates:
  - 50 datasets
  - 100 pipelines
  - 200 lineage edges (DAG structure, no cycles)
  - 1M+ pipeline execution records (last 60 days)
"""

import random
from datetime import datetime, timedelta

import psycopg2
from faker import Faker
from tqdm import tqdm

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────
DB_CONFIG = {
    "dbname": "dsci551",
    "user": "randolphyu",
    "password": "",
    "host": "localhost",
    "port": 5432,
}

BATCH_SIZE = 10_000

fake = Faker()
random.seed(42)
Faker.seed(42)

# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────
SOURCE_SYSTEMS = ["Salesforce", "MySQL", "S3", "Kafka", "Snowflake", "BigQuery", "Oracle", "MongoDB"]
OWNERS         = ["alice", "bob", "carol", "dave", "eve", "frank", "grace", "heidi"]
SCHEDULES      = ["@hourly", "@daily", "@weekly", "0 */6 * * *", "0 2 * * *", "*/30 * * * *"]
STATUSES       = ["success", "failed", "running"]

# Per-pipeline status weights
STATUS_WEIGHTS_HEALTHY   = [0.95, 0.04, 0.01]  # ~4% failure  → HEALTHY
STATUS_WEIGHTS_UNHEALTHY = [0.82, 0.16, 0.02]  # ~16% failure → FAILED


def rand_duration(status):
    if status == "running":
        return None
    return random.randint(10, 3600)


# ─────────────────────────────────────────────
# GENERATE DATA
# ─────────────────────────────────────────────
def generate_datasets(n=50):
    rows = []
    for i in range(1, n + 1):
        name    = f"{fake.word()}_{fake.word()}_data".lower()
        source  = random.choice(SOURCE_SYSTEMS)
        version = random.randint(1, 5)
        created = fake.date_time_between(start_date="-2y", end_date="-1y")
        rows.append((name, source, version, created))
    return rows


def generate_pipelines(n=100):
    rows = []
    for i in range(1, n + 1):
        name     = f"{fake.word()}_{fake.word()}_pipeline".lower()
        owner    = random.choice(OWNERS)
        schedule = random.choice(SCHEDULES)
        created  = fake.date_time_between(start_date="-1y", end_date="-6M")
        rows.append((name, owner, schedule, created))
    return rows


def generate_lineage_edges(dataset_names, pipeline_ids, n=200):
    """
    DAG structure — edges only flow downward across three layers:
      Layer 1 (sources):  dataset  1-15  — raw data, no upstream
      Layer 2 (staging):  dataset 16-35  — intermediate processing
      Layer 3 (marts):    dataset 36-50  — final output tables
    Direction: L1→L2, L2→L3, L1→L3 only. No cycles possible.
    """
    sources = dataset_names[0:15]
    staging = dataset_names[15:35]
    marts   = dataset_names[35:50]

    valid_pairs = (
        [(s, d) for s in sources for d in staging] +
        [(s, d) for s in staging for d in marts]   +
        [(s, d) for s in sources for d in marts]
    )

    selected = random.sample(valid_pairs, min(n, len(valid_pairs)))

    rows = []
    for up, down in selected:
        pid     = random.choice(pipeline_ids)
        created = fake.date_time_between(start_date="-1y", end_date="-6M")
        rows.append((pid, up, down, created))
    return rows


def generate_executions(pipeline_ids, total=1_000_000):
    """
    Generate execution rows sorted by started_at so physical insert order
    matches chronological order — this is the key condition for BRIN to work.

    - All executions fall within the last 60 days
    - 30% of pipelines are unhealthy (>10% failure rate)
    - 70% of pipelines are healthy  (<10% failure rate)

    Strategy: generate all rows in memory, sort by started_at, then yield
    in batches. Memory usage ~400MB for 1M rows — acceptable on modern hardware.
    """
    # Mark 30% of pipelines as unhealthy
    n_unhealthy   = int(len(pipeline_ids) * 0.30)
    unhealthy_set = set(random.sample(pipeline_ids, n_unhealthy))

    start       = datetime.now() - timedelta(days=60)
    window_secs = 60 * 24 * 3600  # 60 days in seconds

    print("    Generating rows in memory...")
    all_rows = []
    for _ in range(total):
        pid        = random.choice(pipeline_ids)
        weights    = STATUS_WEIGHTS_UNHEALTHY if pid in unhealthy_set \
                     else STATUS_WEIGHTS_HEALTHY
        status     = random.choices(STATUSES, weights)[0]
        started_at = start + timedelta(seconds=random.randint(0, window_secs))
        duration   = rand_duration(status)
        ended_at   = (started_at + timedelta(seconds=duration)) if duration else None
        rows_proc  = random.randint(100, 500_000) if status == "success" else None
        all_rows.append((pid, status, started_at, ended_at, duration, rows_proc))

    # Sort by started_at — physical insert order matches time order
    # This ensures BRIN block range summaries are tight (non-overlapping)
    print("    Sorting by started_at for BRIN compatibility...")
    all_rows.sort(key=lambda x: x[2])

    for i in range(0, total, BATCH_SIZE):
        yield all_rows[i:i + BATCH_SIZE]


# ─────────────────────────────────────────────
# INSERT HELPERS
# ─────────────────────────────────────────────
def insert_datasets(cur, rows):
    cur.executemany(
        """INSERT INTO datasets (dataset_name, source_system, schema_version, created_at)
           VALUES (%s, %s, %s, %s)""",
        rows,
    )


def insert_pipelines(cur, rows):
    cur.executemany(
        """INSERT INTO pipelines (pipeline_name, owner, schedule, created_at)
           VALUES (%s, %s, %s, %s)""",
        rows,
    )


def insert_lineage_edges(cur, rows):
    cur.executemany(
        """INSERT INTO lineage_edges (pipeline_id, upstream_table, downstream_table, created_at)
           VALUES (%s, %s, %s, %s)""",
        rows,
    )


def insert_executions_batch(cur, batch):
    cur.executemany(
        """INSERT INTO pipeline_executions
               (pipeline_id, status, started_at, ended_at, duration_secs, rows_processed)
           VALUES (%s, %s, %s, %s, %s, %s)""",
        batch,
    )


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────
def main():
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = False
    cur  = conn.cursor()

    print("🔗 Connected to PostgreSQL")

    # ── datasets ──────────────────────────────
    print("\n[1/4] Inserting 50 datasets...")
    ds_rows = generate_datasets(50)
    insert_datasets(cur, ds_rows)
    conn.commit()
    cur.execute("SELECT dataset_name FROM datasets ORDER BY dataset_id")
    dataset_names = [r[0] for r in cur.fetchall()]
    print(f"    ✅ {len(dataset_names)} datasets inserted")

    # ── pipelines ─────────────────────────────
    print("\n[2/4] Inserting 100 pipelines...")
    pl_rows = generate_pipelines(100)
    insert_pipelines(cur, pl_rows)
    conn.commit()
    cur.execute("SELECT pipeline_id FROM pipelines")
    pipeline_ids = [r[0] for r in cur.fetchall()]
    print(f"    ✅ {len(pipeline_ids)} pipelines inserted")

    # ── lineage edges ─────────────────────────
    print("\n[3/4] Inserting 200 lineage edges (DAG)...")
    le_rows = generate_lineage_edges(dataset_names, pipeline_ids, 200)
    insert_lineage_edges(cur, le_rows)
    conn.commit()
    print(f"    ✅ 200 lineage edges inserted")
    print(f"       Layer 1 (sources) : dataset  1-15")
    print(f"       Layer 2 (staging) : dataset 16-35")
    print(f"       Layer 3 (marts)   : dataset 36-50")

    # ── executions (1M) ───────────────────────
    TOTAL = 1_000_000
    print(f"\n[4/4] Inserting {TOTAL:,} execution records...")
    print(f"       Time range : last 60 days")
    print(f"       ~30 unhealthy pipelines (>10% failure rate)")
    print(f"       ~70 healthy pipelines   (<10% failure rate)")

    total_inserted = 0
    with tqdm(total=TOTAL, unit="rows", unit_scale=True) as pbar:
        for batch in generate_executions(pipeline_ids, TOTAL):
            insert_executions_batch(cur, batch)
            conn.commit()
            total_inserted += len(batch)
            pbar.update(len(batch))

    print(f"    ✅ {total_inserted:,} execution records inserted")

    # ── ANALYZE ───────────────────────────────
    print("\nRunning ANALYZE to update planner statistics...")
    conn.autocommit = True
    cur.execute("ANALYZE pipeline_executions")
    conn.autocommit = False

    # ── summary ───────────────────────────────
    print("\n🎉 All done! Summary:")
    for table in ["datasets", "pipelines", "lineage_edges", "pipeline_executions"]:
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        count = cur.fetchone()[0]
        print(f"   {table:<25} → {count:>10,} rows")

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()