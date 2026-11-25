"""
Database check for first and last rows just to see them using sql queries.
"""
import os

import psycopg2
from dotenv import load_dotenv

load_dotenv()

conn = psycopg2.connect(
    host=os.environ["DB_HOST"], port=int(os.environ.get("DB_PORT", "5432")),
    user=os.environ["DB_USER"], password=os.environ["DB_PASSWORD"],
    dbname=os.environ.get("DB_NAME", "airbreda"), connect_timeout=10, sslmode="require",
)
with conn, conn.cursor() as cur:
    cur.execute("SELECT COUNT(*) FROM sensor_readings")
    print(f"Total rows: {cur.fetchone()[0]}\n")

    cur.execute("""
        SELECT station_id, component, COUNT(*),
               COUNT(*) FILTER (WHERE is_flagged), MIN(timestamp), MAX(timestamp)
        FROM sensor_readings GROUP BY station_id, component ORDER BY station_id, component
    """)
    print(f"{'station_id':<30} {'component':<10} {'rows':>5} {'flagged':>8}  first -> last")
    for station, comp, n, flagged, first, last in cur.fetchall():
        print(f"{station:<30} {comp:<10} {n:>5} {flagged:>8}  {first} -> {last}")

    cur.execute("SELECT * FROM sensor_readings ORDER BY timestamp DESC LIMIT 3")
    print("\nLatest 3 rows:")
    for row in cur.fetchall():
        print(" ", row)
conn.close()
