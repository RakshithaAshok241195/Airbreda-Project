"""
AirBreda - traffic ingestion service (Day 1 + Day 2).

Every poll: download NDW's site-config and measured-values feeds, extract the
four A27/Breda sites (hrl, hrr, vwd, vwa), and for each site:
  1. save lane-level readings as ndw/YYYY-MM-DD/HH-<label>.csv (MEASUREMENT time)
     and upload the file to S3;
  2. log speed = -1 lanes as DATA_QUALITY_ERROR and keep them out of the database;
  3. write a per-site summary (total flow, mean speed) to sensor_readings;
  4. publish the summary to the Redis list "readings".
Runs forever, with a /health endpoint on HEALTH_PORT.

The XML parsing is reused unchanged from helpers/getTrafficReading.py.

WHY BOTH A DATABASE AND A BUCKET? () Day 1)
They answer different questions.
- The bucket (S3) keeps the lane-level parsed files as received, cheaply and
  durably, organised by measurement time: the audit trail and the raw material
  for reprocessing. Files are not queryable by value.
- The database (RDS PostgreSQL) holds cleaned per-site summaries in a fixed
  schema with an index on (station_id, timestamp, component). It answers
  time-range queries and joins with NO2 quickly, but only holds what our CURRENT
  code decided to keep (totals, sentinels removed).
Retraining the model six months from now: if feature engineering changes (weight
speed by flow, per-lane features, different -1 handling), the database cannot
help, because the detail was summarised away. The bucket still has every
lane-level reading per hour, so a new parser can rebuild the training set.

POLLING INTERVAL: 1 HOUR ( Day 2)
NDW refreshes every minute, but each poll downloads and parses the whole national
feed: ~10-15 s to download and ~40-50 s to parse four sites on a laptop.
Polling every minute instead would:
  - not finish in time: a run takes about a minute, so runs would queue up
    back-to-back and the service would do nothing but download;
  - multiply bandwidth and CPU by 60 for data we then summarise per hour anyway,
    because the NO2 target we model is an hourly average;
  - overwrite the same hourly S3 file (HH-<site>.csv) 59 times, keeping only the
    last minute, while the database grows 60x faster.
Trade-off we accept: one poll per hour means each hour is represented by a single
one-minute snapshot, which is noisy (we saw 2,100 -> 1,680 veh/h between two
consecutive minutes on Day 1). A middle ground (e.g. every 15 min, averaged per
hour) is possible via TRAFFIC_POLL_SECONDS and is discussed in the ADR.
"""
import io
import logging
import os
import sys
import urllib.error
from pathlib import Path

import pandas as pd
import psycopg2

from airbreda_common import (
    HealthState, log_event, publish, run_forever, setup_logging,
    start_health_server, utc_iso, write_readings,
)
from helpers.getTrafficReading import (
    CONFIG_URL, MEASURED_URL, build_index_map, download_and_decompress, extract_measurements,
)

SOURCE = "NDW"
SITES = {
    "hrl": "RWS01_MONIBAS_0271hrl0063ra",  # A27 mainline, direction 1
    "hrr": "RWS01_MONIBAS_0271hrr0063ra",  # A27 mainline, direction 2
    "vwd": "RWS01_MONIBAS_0270vwd0063ra",  # entry slip road (leaving Breda)
    "vwa": "RWS01_MONIBAS_0270vwa0063ra",  # exit slip road (entering Breda)
}
POLL_SECONDS = int(os.environ.get("TRAFFIC_POLL_SECONDS", "3600"))
DATA_DIR = Path("data")


# ---------------------------------------------------------------- parse

def _to_float(text):
    try:
        return float(text)
    except (TypeError, ValueError):
        return None


def parse_site(config_bytes, measured_bytes, site_id):
    """Lane-level rows for one site: flow and speed, all-vehicle totals only."""
    index_map = build_index_map(io.BytesIO(config_bytes), site_id)
    readings = extract_measurements(io.BytesIO(measured_bytes), site_id)
    rows = []
    for r in readings:
        info = index_map.get(r["index"])
        if not info or info["vehicle"] != "anyVehicle":
            continue
        metric = {"trafficFlow": "flow", "trafficSpeed": "speed"}.get(info["type"])
        if metric is None:
            continue
        rows.append({"site_id": site_id, "timestamp": r.get("timestamp"),
                     "lane_index": r["index"], "metric": metric, "value": _to_float(r["value"])})
    return rows


def summarise(rows):
    """Total flow (veh/h) and mean lane speed (km/h).

    Negative values (NDW's -1 = "no valid measurement") are excluded.
    Speed 0 is KEPT: on a motorway it can mean standstill traffic, which is exactly
    what this project studies. (The course helper drops 0 as well; we do not.)
    """
    flows = [r["value"] for r in rows if r["metric"] == "flow" and r["value"] is not None and r["value"] >= 0]
    speeds = [r["value"] for r in rows if r["metric"] == "speed" and r["value"] is not None and r["value"] >= 0]
    invalid = sum(1 for r in rows if r["value"] is None or r["value"] < 0)
    return {"flow": sum(flows) if flows else None,
            "speed": sum(speeds) / len(speeds) if speeds else None,
            "invalid_values": invalid}


def measurement_time(rows):
    stamps = [r["timestamp"] for r in rows if r.get("timestamp")]
    return pd.to_datetime(stamps[0], utc=True) if stamps else None


def object_key(label, ts):
    """ndw/YYYY-MM-DD/HH-<label>.csv, from the measurement time (UTC)."""
    return f"ndw/{ts:%Y-%m-%d}/{ts:%H}-{label}.csv"


def site_readings(site_id, rows, ts, state):
    """Data-quality handling + DB rows + queue messages for one site.

    Our DB stores one summary per site, not one row per lane, so "skip the DB
    write for a speed=-1 row" means: each -1 lane is logged and counted, kept out
    of the average, and if NO lane has a valid speed, no speed row is written at all.
    """
    for r in rows:
        if r["value"] is not None and r["value"] < 0:
            log_event(logging.WARNING, "DATA_QUALITY_ERROR", source=SOURCE, location=site_id,
                      field=r["metric"], value=r["value"], lane=r["lane_index"], timestamp=utc_iso(ts))
            state.record_bad_data()

    summary = summarise(rows)
    db_rows, messages = [], []
    for component in ("flow", "speed"):
        value = summary[component]
        if value is None:
            continue
        db_rows.append((site_id, ts.to_pydatetime(), component, float(value), False))
        messages.append({"station_id": site_id, "timestamp": utc_iso(ts), "component": component,
                         "value": float(value), "is_flagged": False})
    return db_rows, messages, summary


# ---------------------------------------------------------------- storage

def save_csv(rows, key):
    path = DATA_DIR / key
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(path, index=False)
    return path


def upload_to_s3(path, key):
    import boto3  # imported here so tests do not need AWS
    boto3.client("s3").upload_file(str(path), os.environ["S3_BUCKET"], key)


# ---------------------------------------------------------------- one poll

class TrafficIngestor:
    def __init__(self, state=None, write=write_readings, publish_fn=publish, upload=upload_to_s3):
        self.state = state or HealthState(SOURCE)   # = ndw_bad_data_count
        self.write = write
        self.publish = publish_fn
        self.upload = upload

    def poll(self):
        try:
            config_bytes = download_and_decompress(CONFIG_URL).read()
            measured_bytes = download_and_decompress(MEASURED_URL).read()
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            log_event(logging.ERROR, "fetch_failed", source=SOURCE, error=str(exc))
            return
        self.state.record_success()

        all_db_rows, all_messages = [], []
        for label, site_id in SITES.items():
            rows = parse_site(config_bytes, measured_bytes, site_id)
            ts = measurement_time(rows)
            if not rows or ts is None:
                log_event(logging.WARNING, "no_data", source=SOURCE, location=site_id, site=label)
                continue

            db_rows, messages, summary = site_readings(site_id, rows, ts, self.state)
            all_db_rows += db_rows
            all_messages += messages
            log_event(logging.INFO, "fetch_success", source=SOURCE, location=site_id, site=label,
                      timestamp=utc_iso(ts), flow=summary["flow"], speed=summary["speed"],
                      invalid_values=summary["invalid_values"])

            key = object_key(label, ts)
            try:
                self.upload(save_csv(rows, key), key)
                log_event(logging.INFO, "s3_upload", source=SOURCE, site=label, key=key)
            except Exception as exc:  # one site's upload failure must not stop the others
                log_event(logging.ERROR, "s3_upload_failed", source=SOURCE, site=label, key=key, error=str(exc))

        try:
            inserted, _ = self.write(all_db_rows)
            log_event(logging.INFO, "db_write", source=SOURCE, inserted=inserted,
                      skipped=len(all_db_rows) - inserted)
        except (psycopg2.Error, KeyError) as exc:
            log_event(logging.ERROR, "db_write_failed", source=SOURCE, error=str(exc))

        try:
            count = self.publish(all_messages)
            log_event(logging.INFO, "queue_publish", source=SOURCE, queue="readings", count=count)
        except Exception as exc:
            log_event(logging.ERROR, "queue_publish_failed", source=SOURCE,
                      count=len(all_messages), error=str(exc))


def main():
    setup_logging()
    ingestor = TrafficIngestor()
    start_health_server(ingestor.state, int(os.environ.get("HEALTH_PORT", "8002")))
    run_forever(ingestor.poll, POLL_SECONDS, service="traffic-ingest")
    return 0


if __name__ == "__main__":
    sys.exit(main())
