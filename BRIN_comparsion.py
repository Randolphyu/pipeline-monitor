import psycopg2
from datetime import datetime, timedelta

DB_CONFIG = {
    "dbname": "dsci551", "user": "randolphyu",
    "password": "", "host": "localhost", "port": 5432,
}

def run_explain(cur, query, params, label):
    cur.execute(f"EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT) {query}", params)
    rows = cur.fetchall()
    print(f"\n{'='*55}")
    print(f"  {label}")
    print('='*55)
    for r in rows:
        print(r[0])

conn = psycopg2.connect(**DB_CONFIG)
conn.autocommit = True
cur = conn.cursor()

since = datetime.now() - timedelta(days=30)
query = "SELECT COUNT(*) FROM pipeline_executions WHERE started_at >= %s"
params = (since,)

# WITH BRIN index
run_explain(cur, query, params, "WITH BRIN index (--days 30)")

# WITHOUT index (seq scan forced)
cur.execute("SET enable_indexscan = off")
cur.execute("SET enable_bitmapscan = off")
cur.execute("SET enable_indexonlyscan = off")
run_explain(cur, query, params, "WITHOUT index (seq scan forced)")

# restore
cur.execute("SET enable_indexscan = on")
cur.execute("SET enable_bitmapscan = on")
cur.execute("SET enable_indexonlyscan = on")

cur.close()
conn.close()