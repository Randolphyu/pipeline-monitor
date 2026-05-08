import psycopg2

DB_CONFIG = {
    "dbname": "dsci551", "user": "randolphyu",
    "password": "", "host": "localhost", "port": 5432,
}

def run_explain(cur, query, params, label):
    cur.execute(f"EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT) {query}", params)
    rows = cur.fetchall()
    print(f"\n{'='*50}")
    print(f"{label}")
    print('='*50)
    for r in rows:
        print(r[0])

conn = psycopg2.connect(**DB_CONFIG)
conn.autocommit = True
cur = conn.cursor()

query = "SELECT COUNT(*) FROM pipeline_executions WHERE pipeline_id = %s"
params = (78,)

# WITH index
run_explain(cur, query, params, "WITH B-tree index")

# WITHOUT index (session only)
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