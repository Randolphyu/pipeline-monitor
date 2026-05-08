# Data Pipeline Monitoring & Lineage Tracking System

DSCI 551 Final Project · Spring 2026 · Yu-Huan (Randolph) Yu

A PostgreSQL-powered CLI application that monitors ETL pipeline health, traces table lineage, and tracks data quality across a fleet of 100 pipelines and 1 million execution records.

---

## Project Overview

This system addresses three practical needs for data engineers:

| Feature | Command | DB Internal |
|---|---|---|
| Lineage Graph Query | `python main.py lineage` | Recursive CTE |
| Pipeline Monitoring | `python main.py inspect` | B-tree index + heap storage |
| Data Quality Tracking | `python main.py overview` | BRIN index + MVCC |

---

## Quick Start

If you already have PostgreSQL 17 running and Python 3.9+ installed, run these five commands to go from zero to a working application:

```bash
# 1. Install dependencies
pip install psycopg2-binary faker tqdm

# 2. Create the database and schema
psql -U your_username -c "CREATE DATABASE dsci551;"
psql -U your_username -d dsci551 -f schema.sql

# 3. Generate synthetic data and load into PostgreSQL
#    (generates 100 pipelines, 200 lineage edges, 1M execution records)
python gen_syn_data.py

# 4. Update statistics so indexes work correctly
psql -U your_username -d dsci551 -c "VACUUM ANALYZE pipeline_executions;"

# 5. Run the application
python main.py overview --days 30
python main.py inspect --pipeline-id 78 --days 30
python main.py lineage --pipeline-id 78
```

> Update `your_username` in step 2 and in `DB_CONFIG` inside `main.py` and `gen_syn_data.py` to match your local PostgreSQL username.

For full setup instructions see the sections below.

---

## Requirements

- Python 3.9+
- PostgreSQL 17
- Python packages: `psycopg2-binary`, `faker`, `tqdm`

No API keys or secret credentials are required. All data is synthetically generated locally.

---

## Environment Setup

### 1. Install PostgreSQL 17

macOS:
```bash
brew install postgresql@17
brew services start postgresql@17
```

Ubuntu:
```bash
sudo apt install postgresql-17
sudo service postgresql start
```

### 2. Create the database

```bash
psql -U postgres
```

```sql
CREATE DATABASE dsci551;
\q
```

### 3. Clone the repository

```bash
git clone https://github.com/Randolphyu/pipeline-monitor.git
cd pipeline-monitor
```

### 4. Create a virtual environment and install dependencies

```bash
python -m venv venv
source venv/bin/activate      # macOS / Linux
# venv\Scripts\activate       # Windows

pip install psycopg2-binary faker tqdm
```

### 5. Configure database credentials

Open `gen_syn_data.py` and `main.py` and update the `DB_CONFIG` block at the top of each file:

```python
DB_CONFIG = {
    "dbname": "dsci551",
    "user":   "your_username",   # ← update this
    "password": "",              # ← update if needed
    "host":   "localhost",
    "port":   5432,
}
```

---

## Database Setup

### 1. Create the schema

```bash
psql -U your_username -d dsci551 -f schema.sql
```

This creates the three tables (`pipelines`, `lineage_edges`, `pipeline_executions`) and the required indexes:

```sql
CREATE INDEX idx_btree_pipeline_id ON pipeline_executions (pipeline_id);
CREATE INDEX idx_brin_started_at   ON pipeline_executions USING BRIN (started_at);
```

### 2. Generate and load synthetic data

```bash
python gen_syn_data.py
```

This script will:
- Insert 100 pipelines
- Insert 200 lineage edges (DAG structure — no cycles)
- Generate and insert 1,000,000 execution records sorted by `started_at` (required for BRIN to work correctly)
- Run `ANALYZE` automatically at the end

Expected output:
```
🔗 Connected to PostgreSQL
[1/3] Inserting 100 pipelines...       ✅ 100 pipelines inserted
[2/3] Inserting 200 lineage edges...   ✅ 200 lineage edges inserted
[3/3] Inserting 1,000,000 execution records...
    Generating rows in memory...
    Sorting by started_at for BRIN compatibility...
100%|████████████████| 1.00M/1.00M [02:00<00:00, ...]
    ✅ 1,000,000 execution records inserted
```

> **Note:** The script holds 1M rows in memory during sorting (~400 MB). Ensure your machine has at least 2 GB of available RAM before running.

---

## Running the Application

### Feature 1 — Lineage Graph Query

Trace the full upstream and downstream dependency chain of a table:

```bash
# By table name
python main.py lineage --table result_raise_data

# By pipeline ID (resolves to the table automatically)
python main.py lineage --pipeline-id 78
```

**What it shows:**
- Full upstream dependency chain (root cause analysis)
- Full downstream impact chain (blast radius assessment)
- Schema version warnings for tables with version ≥ 4

---

### Feature 2 — Pipeline Monitoring

Drill into a specific pipeline's execution history:

```bash
python main.py inspect --pipeline-id 78 --days 30
```

**What it shows:**
- Total runs, success rate, failure rate, average duration
- Last 10 execution records with status, timestamp, and rows processed
- EXPLAIN ANALYZE output showing B-tree Index Only Scan

---

### Feature 3 — Data Quality Tracking

Fleet-wide health check across all 100 pipelines:

```bash
python main.py overview --days 30
```

**What it shows:**
- FAILED pipelines (failure rate > 10%)
- STALE pipelines (overdue by more than 1.5× their schedule interval)
- HEALTHY pipelines
- EXPLAIN ANALYZE output showing BRIN Bitmap Index Scan
- MVCC xmin/xmax system columns

---

## Reproducing Results

To reproduce the exact demo results from the final report:

```bash
# 1. Reset all data
psql -U your_username -d dsci551 -c "TRUNCATE pipelines, lineage_edges, pipeline_executions RESTART IDENTITY CASCADE;"

# 2. Regenerate
python gen_syn_data.py

# 3. Run VACUUM to update visibility map (enables Index Only Scan)
psql -U your_username -d dsci551 -c "VACUUM ANALYZE pipeline_executions;"

# 4. Run the three features
python main.py overview --days 30
python main.py inspect --pipeline-id 78 --days 30
python main.py lineage --pipeline-id 78
```

---

## Project Structure

```
pipeline-monitor/
├── main.py              # CLI application (Feature 1, 2, 3)
├── gen_syn_data.py      # Synthetic data generation script
├── schema.sql           # Database schema and index creation
├── README.md            # This file
└── Yuhuan_Yu_Final_Report.pdf
```

