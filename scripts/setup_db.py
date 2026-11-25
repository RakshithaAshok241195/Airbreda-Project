"""
Setting up the'airbreda' database and the sensor_readings table on RDS.

it checks before creating anything.
Credentials come from the .env file, never from the code.
"""
import os

import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv

load_dotenv()

HOST = os.environ["DB_HOST"]
PORT = int(os.environ.get("DB_PORT", "5432"))
USER = os.environ["DB_USER"]
PASSWORD = os.environ["DB_PASSWORD"]
DB_NAME = os.environ.get("DB_NAME", "airbreda")


def connect(dbname):
    # sslmode=require: RDS PostgreSQL enforces encrypted connections by default.
    # connect_timeout: fail fast instead of hanging if the security group blocks us.
    return psycopg2.connect(
        host=HOST, port=PORT, user=USER, password=PASSWORD,
        dbname=dbname, connect_timeout=10, sslmode="require",
    )


# Step 1: create the database.
# CREATE DATABASE cannot run inside a transaction and has no IF NOT EXISTS,
# so we use autocommit and check pg_database first.
conn = connect("postgres")
conn.autocommit = True
with conn.cursor() as cur:
    cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (DB_NAME,))
    if cur.fetchone():
        print(f"Database '{DB_NAME}' already exists")
    else:
        cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(DB_NAME)))
        print(f"Created database '{DB_NAME}'")
conn.close()

# Step 2: create the table (schema from Day 1, Lab 2).
# Deviation from the course schema: station_id is VARCHAR(40) instead of VARCHAR(20),
# because NDW site IDs such as RWS01_MONIBAS_0271hrl0063ra are 27 characters long.
CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS sensor_readings (
    station_id  VARCHAR(40)   NOT NULL,
    timestamp   TIMESTAMPTZ   NOT NULL,
    component   VARCHAR(10)   NOT NULL,
    value       FLOAT,
    PRIMARY KEY (station_id, timestamp, component)
);
"""

conn = connect(DB_NAME)
with conn, conn.cursor() as cur:  # 'with conn' commits the transaction on success
    cur.execute(CREATE_TABLE)
    # Day 2: flag stale / null air-quality readings instead of dropping them.
    # IF NOT EXISTS makes this safe to run again.
    cur.execute("ALTER TABLE sensor_readings ADD COLUMN IF NOT EXISTS is_flagged BOOLEAN DEFAULT FALSE")
    cur.execute(
        """
        SELECT column_name, data_type, character_maximum_length, is_nullable
        FROM information_schema.columns
        WHERE table_name = 'sensor_readings'
        ORDER BY ordinal_position
        """
    )
    print("\nsensor_readings columns:")
    for name, dtype, maxlen, nullable in cur.fetchall():
        print(f"  {name:<12} {dtype:<26} max_len={maxlen}  nullable={nullable}")
conn.close()
