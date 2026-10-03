"""
AirBreda - air quality ingestion service (Day 1 + Day 2).

Every poll: fetch ~50 hours of hourly NO2 for Luchtmeetnet station NL10240,
flag null and stale values, write all rows to sensor_readings (idempotent),
and publish new readings to the Redis list "readings". Runs forever, with a
/health endpoint on HEALTH_PORT.

fetch_measurements() is adapted from get_latest_no2() in helpers/getNO2readings.py:
same endpoint and query parameters, but it keeps the whole first page instead of
only the newest record, because (1) every run re-fetches an overlapping window,
so a failed run is filled in by the next one, and (2) stale detection needs
several consecutive hours.

CAP TRADE-OFF (required comment, Day 1)
The Luchtmeetnet sensor network behaves like an AP system. When a station loses
its connection to the central system (a network partition), the API stays
available and keeps answering, but the value for that hour may be missing (null)
or carried over from the last successful measurement. Availability is kept;
consistency (getting the true, latest value) is given up. Kleppmann argues the
CP/AP labels oversimplify real systems, but the practical lesson holds:
"the API responded" does not mean "the data is correct".

HANDLING NULL / MISSING VALUES IN PRODUCTION (required comment, Day 1)
Never silently drop or impute them at ingestion time. Keep the row with a NULL
value so the gap stays visible, flag it (is_flagged = TRUE + structured WARNING),
and count how often it happens so monitoring can alert when the station goes
quiet. Interpolation is a downstream decision, documented and reversible.

POLLING INTERVAL: 1 HOUR (required comment, Day 2)
Luchtmeetnet publishes one hourly average per component, about 30 minutes after
the hour ends (measured on Day 1). Polling hourly picks up each new value once,
and because every poll re-reads ~50 hours, a late or missed poll is harmless.
Polling every minute instead would:
  - return the same data 59 times out of 60 (nothing new to ingest);
  - eat into the fair-use limit (100 requests / 5 min). Harmless for one station,
    but at 50 stations x 1/min = 250 requests per 5 minutes, we would be blocked;
  - break nothing in the data itself (writes are idempotent), so the cost is
    pure waste plus the risk of being rate-limited.
"""
import logging
import os
import sys
import time

import pandas as pd
import psycopg2
import requests

from airbreda_common import (
    HealthState, log_event, publish, run_forever, setup_logging,
    start_health_server, utc_iso, write_readings,
)

STATION = "NL10240"
FORMULA = "NO2"
SOURCE = "Luchtmeetnet"
BASE_URL = "https://api.luchtmeetnet.nl/open_api/stations/{station}/measurements"

POLL_SECONDS = int(os.environ.get("AIR_POLL_SECONDS", "3600"))
MAX_ATTEMPTS = 3
TIMEOUT_SECONDS = 10
STALE_RUN_LENGTH = 3   # same value for 3+ consecutive hours = suspect
COLUMNS = ["station_id", "timestamp", "component", "value"]


# ---------------------------------------------------------------- fetch

def fetch_measurements(station=STATION, formula=FORMULA):
    """Return the first page of measurements (newest first) as a list of dicts.

    Retries timeouts, connection errors, HTTP 429 and 5xx with backoff (1s, 2s).
    Other 4xx errors are raised immediately: repeating a bad request won't fix it.
    """
    params = {"formula": formula, "order_by": "timestamp_measured",
              "order_direction": "desc", "page": 1}
    url = BASE_URL.format(station=station)
    last_error = None
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            response = requests.get(url, params=params, timeout=TIMEOUT_SECONDS)
            response.raise_for_status()
            return response.json().get("data", [])
        except requests.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else None
            if status is not None and status < 500 and status != 429:
                raise
            last_error = exc
        except (requests.Timeout, requests.ConnectionError) as exc:
            last_error = exc
        log_event(logging.WARNING, "fetch_retry", source=SOURCE, attempt=attempt,
                  max_attempts=MAX_ATTEMPTS, error=str(last_error))
        if attempt < MAX_ATTEMPTS:
            time.sleep(2 ** (attempt - 1))
    raise last_error


# ---------------------------------------------------------------- transform

def to_dataframe(records, station=STATION):
    """Map API records onto the sensor_readings schema (the API gives no station ID)."""
    df = pd.DataFrame(records, columns=["formula", "value", "timestamp_measured"])
    df = df.rename(columns={"formula": "component", "timestamp_measured": "timestamp"})
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    df["station_id"] = station
    return df[COLUMNS]


def filter_no2_readings(df):
    """Keep only NO2 rows. Rows with a null value are KEPT, not dropped."""
    return df[df["component"] == "NO2"].reset_index(drop=True)


def flag_quality(df, stale_run=STALE_RUN_LENGTH):
    """Add is_flagged and flag_reason ('null' or 'stale').

    Stale = the same value for `stale_run` or more consecutive HOURLY timestamps.
    A gap in time or a null breaks the run. Every row in such a run is flagged,
    because we cannot tell which of the identical values (if any) was a real measurement.
    """
    df = df.sort_values("timestamp").reset_index(drop=True)
    is_null = df["value"].isna()
    gap = df["timestamp"].diff() != pd.Timedelta(hours=1)
    new_run = df["value"].ne(df["value"].shift()) | is_null | gap
    run_length = df.groupby(new_run.cumsum())["value"].transform("size")
    is_stale = ~is_null & (run_length >= stale_run)

    df["is_flagged"] = is_null | is_stale
    df["flag_reason"] = None
    df.loc[is_stale, "flag_reason"] = "stale"
    df.loc[is_null, "flag_reason"] = "null"
    return df


def to_db_rows(df):
    """Plain tuples for psycopg2. pandas NaN -> None (SQL NULL)."""
    flagged = df["is_flagged"] if "is_flagged" in df else pd.Series(False, index=df.index)
    return [
        (row.station_id, row.timestamp.to_pydatetime(), row.component,
         None if pd.isna(row.value) else float(row.value), bool(flag))
        for row, flag in zip(df.itertuples(index=False), flagged)
    ]


def to_message(row):
    """Queue message, same schema for every source."""
    return {"station_id": row.station_id, "timestamp": utc_iso(row.timestamp),
            "component": row.component,
            "value": None if pd.isna(row.value) else float(row.value),
            "is_flagged": bool(row.is_flagged)}


# ---------------------------------------------------------------- one poll

class AirIngestor:
    """Holds the in-memory state of the service between polls."""

    def __init__(self, state=None, write=write_readings, publish_fn=publish):
        self.state = state or HealthState(SOURCE)   # = luchtmeetnet_bad_data_count
        self.write = write
        self.publish = publish_fn
        # Every poll re-reads ~50 hours. Remember what we already reported and
        # published, so one stale hour is warned about (and counted) once, not 50 times.
        self._reported = set()
        self._published = set()

    def process(self, records):
        df = filter_no2_readings(to_dataframe(records))
        if df.empty:
            log_event(logging.WARNING, "no_data", source=SOURCE, station_id=STATION,
                      detail="API responded with no NO2 measurements")
            return
        self.state.record_success()
        df = flag_quality(df)

        latest = df.iloc[-1]
        log_event(logging.INFO, "fetch_success", source=SOURCE, station_id=STATION,
                  value=None if pd.isna(latest["value"]) else float(latest["value"]),
                  timestamp=utc_iso(latest["timestamp"]), rows=len(df))

        for row in df[df["is_flagged"]].itertuples(index=False):
            key = utc_iso(row.timestamp)
            if key in self._reported:
                continue
            self._reported.add(key)
            log_event(logging.WARNING, "DATA_QUALITY_ERROR", source=SOURCE, station_id=STATION,
                      field="NO2", reason="stale_or_null", detail=row.flag_reason,
                      value=None if pd.isna(row.value) else float(row.value), timestamp=key)
            self.state.record_bad_data()

        # 1. Database first: it is the source of truth.
        rows = to_db_rows(df)
        try:
            inserted, newly_flagged = self.write(rows)
            log_event(logging.INFO, "db_write", source=SOURCE, inserted=inserted,
                      newly_flagged=newly_flagged, skipped=len(rows) - inserted - newly_flagged)
        except (psycopg2.Error, KeyError) as exc:
            log_event(logging.ERROR, "db_write_failed", source=SOURCE, error=str(exc))

        # 2. Queue second, independently: a broker outage must not stop the DB write.
        new = [r for r in df.itertuples(index=False) if utc_iso(r.timestamp) not in self._published]
        try:
            count = self.publish([to_message(r) for r in new])
            self._published.update(utc_iso(r.timestamp) for r in new)
            log_event(logging.INFO, "queue_publish", source=SOURCE, queue="readings", count=count)
        except Exception as exc:
            log_event(logging.ERROR, "queue_publish_failed", source=SOURCE,
                      count=len(new), error=str(exc))

    def poll(self):
        try:
            records = fetch_measurements()
        except requests.RequestException as exc:
            log_event(logging.ERROR, "fetch_failed", source=SOURCE, error=str(exc))
            return
        self.process(records)


def main():
    setup_logging()
    ingestor = AirIngestor()
    start_health_server(ingestor.state, int(os.environ.get("HEALTH_PORT", "8001")))
    run_forever(ingestor.poll, POLL_SECONDS, service="air-ingest")
    return 0


if __name__ == "__main__":
    sys.exit(main())
