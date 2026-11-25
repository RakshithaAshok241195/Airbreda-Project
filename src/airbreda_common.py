"""
Shared building blocks for the AirBreda ingestion services.

SHARED CODEBASE, SEPARATE SERVICES (Day 2 design choice)
air-ingest and traffic-ingest stay separate containers: they have different
sources, failure modes and polling intervals, and one crashing must not take the
other down. But logging, health checks, the database write and the queue publish
are identical, so they live in this one module that both images copy in.
This is a "modular monolith" in miniature: one codebase, two deployables.
"""
import json
import logging
import os
import sys
import threading
import time
from collections import deque
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import psycopg2
from psycopg2.extras import execute_values

try:
    from dotenv import load_dotenv  # local runs; in Docker the env comes from Compose
    load_dotenv()
except ImportError:
    pass

log = logging.getLogger("airbreda")


# ---------------------------------------------------------------- logging

def setup_logging():
    """One JSON object per line on stdout: what `docker logs` and CloudWatch can query."""
    logging.basicConfig(level=logging.INFO, format="%(message)s", stream=sys.stdout, force=True)


def utc_iso(dt):
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def log_event(level, event, **fields):
    payload = {"ts": utc_iso(datetime.now(timezone.utc)),
               "level": logging.getLevelName(level), "event": event, **fields}
    log.log(level, json.dumps(payload, default=str))


# ---------------------------------------------------------------- health + bad-data counter

class HealthState:
    """In-memory health of one ingestion service.

    bad_data_count is the total since the container started (this is the
    luchtmeetnet_bad_data_count / ndw_bad_data_count from the lab, one per service).
    For the alert we also keep the timestamps of recent bad readings, so we can
    count how many happened in the last hour (a sliding window).
    In-memory means: a restart resets the counter. Acceptable for now, noted in ADR-003.
    """

    def __init__(self, source, threshold=10, window=timedelta(hours=1), clock=None):
        self.source = source
        self.threshold = threshold
        self.window = window
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._lock = threading.Lock()  # the health server reads from another thread
        self._recent = deque()
        self._alerted = False
        self.last_successful_fetch = None
        self.bad_data_count = 0

    def record_success(self):
        with self._lock:
            self.last_successful_fetch = self._clock()

    def record_bad_data(self, n=1):
        """Count bad readings; log ONE error when the last hour passes the threshold."""
        now = self._clock()
        with self._lock:
            self.bad_data_count += n
            self._recent.extend([now] * n)
            while self._recent and now - self._recent[0] > self.window:
                self._recent.popleft()
            recent = len(self._recent)
            fire = recent > self.threshold and not self._alerted
            if fire:
                self._alerted = True           # log once, not on every further bad reading
            elif recent <= self.threshold:
                self._alerted = False          # re-arm once the window has calmed down
        if fire:
            log_event(logging.ERROR, "BAD_DATA_THRESHOLD_EXCEEDED", source=self.source, count=recent)
        return fire

    def snapshot(self):
        with self._lock:
            last = utc_iso(self.last_successful_fetch) if self.last_successful_fetch else None
            return {"last_successful_fetch": last, "bad_data_count": self.bad_data_count,
                    "source": self.source}


def start_health_server(state, port):
    """Serve GET /health in a background thread, next to the polling loop."""

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path.rstrip("/") != "/health":
                self.send_error(404)
                return
            body = json.dumps(state.snapshot()).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args):  # keep stdout for our structured logs only
            pass

    server = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    log_event(logging.INFO, "health_server_started", source=state.source, port=port)
    return server


# ---------------------------------------------------------------- database

# Idempotent write, extended on Day 2 for is_flagged:
# - a new reading is inserted;
# - an existing reading is NEVER overwritten, except that a flag can be added
#   (a value can become "stale" later, once the 3rd identical hour arrives);
# - everything else is skipped.
# RETURNING (xmax = 0) is a PostgreSQL trick: true for inserted rows, false for updated ones.
INSERT_SQL = """
    INSERT INTO sensor_readings (station_id, timestamp, component, value, is_flagged)
    VALUES %s
    ON CONFLICT (station_id, timestamp, component) DO UPDATE
        SET is_flagged = TRUE
        WHERE EXCLUDED.is_flagged AND NOT sensor_readings.is_flagged
    RETURNING (xmax = 0) AS inserted
"""


def get_connection():
    return psycopg2.connect(
        host=os.environ["DB_HOST"], port=int(os.environ.get("DB_PORT", "5432")),
        user=os.environ["DB_USER"], password=os.environ["DB_PASSWORD"],
        dbname=os.environ.get("DB_NAME", "airbreda"), connect_timeout=10, sslmode="require",
    )


def write_readings(rows):
    """rows: (station_id, timestamp, component, value, is_flagged). Returns (inserted, newly_flagged)."""
    if not rows:
        return 0, 0
    conn = get_connection()
    try:
        with conn, conn.cursor() as cur:
            result = execute_values(cur, INSERT_SQL, rows, page_size=1000, fetch=True)
        inserted = sum(1 for (was_insert,) in result if was_insert)
        return inserted, len(result) - inserted
    finally:
        conn.close()


# ---------------------------------------------------------------- queue

QUEUE_NAME = "readings"


def publish(messages, queue=QUEUE_NAME):
    """RPUSH each message as JSON onto the Redis list. Raises if Redis is unreachable."""
    import redis  # imported here so unit tests do not need a Redis server
    client = redis.Redis(host=os.environ.get("REDIS_HOST", "redis"),
                         port=int(os.environ.get("REDIS_PORT", "6379")),
                         socket_connect_timeout=2, socket_timeout=2)
    if messages:
        client.rpush(queue, *[json.dumps(m) for m in messages])
    return len(messages)


# ---------------------------------------------------------------- polling loop

def run_forever(job, interval_seconds, service):
    """Run job, sleep until the next slot, repeat. One run never overlaps the next.

    A crash inside one run is logged and the loop continues: one bad hour must
    not kill the service. RUN_ONCE=1 runs a single cycle (handy for manual tests).
    """
    while True:
        started = time.monotonic()
        try:
            job()
        except Exception as exc:  # last line of defence; specific errors are handled inside job()
            log_event(logging.ERROR, "run_failed", service=service, error=repr(exc))
        if os.environ.get("RUN_ONCE") == "1":
            return
        pause = max(0.0, interval_seconds - (time.monotonic() - started))
        log_event(logging.INFO, "sleeping", service=service, seconds=round(pause))
        time.sleep(pause)
