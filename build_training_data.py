"""
build_training_data.py - join the pipeline's exhaust into one training table (Day 4).

  NO2      : sensor_readings in RDS, station NL10240 (via the SSH tunnel on a laptop)
  Traffic  : every NDW CSV in the S3 bucket, ndw/YYYY-MM-DD/HH-<site>.csv
  Output   : training_data.csv, one row per HOUR:
             timestamp, no2_ug_m3, hrl, hrr, vwd, vwa,
             total_intensity_veh_per_hr, hour_of_day, no2_label

Run (laptop, tunnel open, .env with DB_HOST=localhost DB_PORT=5433 S3_BUCKET=...):
    python build_training_data.py
Re-run any time: it rebuilds the file from scratch, so it grows with the data.

DESIGN DECISIONS (-> ADR-006)
1. Join on HOUR WINDOWS, not "nearest hour". A traffic snapshot at 08:23 and one
   at 08:47 both belong to the hour 08:00-09:00; "nearest" would split them.
   A NO2 label is mapped to the hour it describes (features.NO2_LABEL_MARKS).
2. Flagged NO2 (null or stale) is EXCLUDED from training. It was kept in the
   database on purpose (flag and keep, Day 2) - and this is where the flag is used.
3. An hour needs ALL FOUR sites. A missing site would make the hour look quieter
   than it was, so incomplete hours are reported and left out, not under-counted.
4. Two traffic sources in the bucket: ndw/ (VM pipeline, lane format) and
   ndw_laptop/ (Day 1-2 laptop runs, summary format). If both have the same
   site-hour, the FIRST prefix wins (the pipeline's own file).
5. Features come from features.py - the same code the dashboard uses at serving
   time, so training and serving can't drift apart (training-serving skew).
"""
import argparse
import os
import re
import sys
from pathlib import Path

import pandas as pd

try:  # read .env BEFORE the arguments are parsed (S3_BUCKET, DB_* settings)
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from features import (SITES, hour_of_day, no2_window, site_intensity, to_utc,
                      total_intensity, traffic_window)

STATION = "NL10240"
DEFAULT_PREFIXES = ("ndw/", "ndw_laptop/")      # priority order
KEY_PATTERN = re.compile(r"(?P<date>\d{4}-\d{2}-\d{2})/(?P<hour>\d{2})-(?P<site>[a-z]{3})\.csv$")

OUTPUT_COLUMNS = ["timestamp", "no2_ug_m3", *SITES, "total_intensity_veh_per_hr",
                  "hour_of_day", "no2_label"]


# ---------------------------------------------------------------- sources

def s3_objects(bucket, prefixes):
    """Yield (key, bytes) for every CSV under the prefixes, in priority order."""
    import boto3  # imported here so tests don't need AWS
    s3 = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")
    for prefix in prefixes:
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                if obj["Key"].endswith(".csv"):
                    yield obj["Key"], s3.get_object(Bucket=bucket, Key=obj["Key"])["Body"].read()


def local_objects(root, prefixes):
    """Same as s3_objects, but from a local folder laid out like the bucket (offline/testing)."""
    root = Path(root)
    for prefix in prefixes:
        for path in sorted((root / prefix).rglob("*.csv")):
            yield path.relative_to(root).as_posix(), path.read_bytes()


def no2_from_db():
    """All NO2 rows for the station: (timestamp, value, is_flagged)."""
    from airbreda_common import get_connection
    conn = get_connection()
    try:
        return pd.read_sql(
            "SELECT timestamp, value, is_flagged FROM sensor_readings "
            "WHERE station_id = %(station)s AND component = 'NO2' ORDER BY timestamp",
            conn, params={"station": STATION})
    finally:
        conn.close()


# ---------------------------------------------------------------- transform

def load_traffic(objects):
    """(key, bytes) pairs -> one row per (hour window, site) with its intensity.

    Returns (DataFrame[window, site, intensity, key], report dict).
    """
    seen, rows, report = set(), [], {"files": 0, "duplicates": 0, "unreadable": [], "invalid_intensity": 0}
    for key, body in objects:
        match = KEY_PATTERN.search(key)
        if not match or match["site"] not in SITES:
            continue
        report["files"] += 1
        try:
            intensity, measured_at = site_intensity(body)
        except Exception as exc:                       # a broken file must not stop the build
            report["unreadable"].append(f"{key}: {exc}")
            continue
        # The measurement time inside the file is the truth; the key is the fallback.
        window = (traffic_window(measured_at) if measured_at
                  else to_utc(f"{match['date']}T{match['hour']}:00:00Z"))
        if (window, match["site"]) in seen:
            report["duplicates"] += 1                  # lower-priority copy of the same site-hour
            continue
        seen.add((window, match["site"]))
        if intensity is None:
            report["invalid_intensity"] += 1
        rows.append({"window": window, "site": match["site"], "intensity": intensity, "key": key})
    return pd.DataFrame(rows, columns=["window", "site", "intensity", "key"]), report


def prepare_no2(raw):
    """Usable NO2 per hour window: null and flagged rows excluded (and counted)."""
    report = {"no2_rows": len(raw),
              "no2_null": int(raw["value"].isna().sum()),
              "no2_flagged": int((raw["is_flagged"].fillna(False) & raw["value"].notna()).sum())}
    good = raw[raw["value"].notna() & ~raw["is_flagged"].fillna(False)].copy()
    good["no2_label"] = good["timestamp"].map(to_utc)
    good["window"] = good["no2_label"].map(no2_window)
    good = good.rename(columns={"value": "no2_ug_m3"})
    return good[["window", "no2_ug_m3", "no2_label"]], report


def build(no2, traffic):
    """Pivot traffic per site, require all four, join NO2 on the hour window."""
    if traffic.empty:
        return pd.DataFrame(columns=OUTPUT_COLUMNS), {"traffic_hours": 0, "complete_hours": 0, "joined_rows": 0}
    wide = traffic.pivot_table(index="window", columns="site", values="intensity", aggfunc="first")
    wide = wide.reindex(columns=list(SITES))
    wide["total_intensity_veh_per_hr"] = [total_intensity(r.to_dict()) for _, r in wide.iterrows()]
    complete = wide.dropna(subset=["total_intensity_veh_per_hr"])

    joined = complete.join(no2.set_index("window"), how="inner").reset_index()
    joined = joined.rename(columns={"window": "timestamp"})
    joined["hour_of_day"] = joined["timestamp"].map(hour_of_day)
    joined["timestamp"] = joined["timestamp"].map(lambda t: t.strftime("%Y-%m-%dT%H:%M:%SZ"))
    joined["no2_label"] = joined["no2_label"].map(lambda t: t.strftime("%Y-%m-%dT%H:%M:%SZ"))
    report = {"traffic_hours": len(wide), "complete_hours": len(complete),
              "incomplete_hours": [w.strftime("%Y-%m-%d %H:00") for w in wide.index.difference(complete.index)],
              "complete_hours_without_no2": [w.strftime("%Y-%m-%d %H:00")
                                             for w in complete.index.difference(no2["window"])],
              "joined_rows": len(joined)}
    return joined[OUTPUT_COLUMNS].sort_values("timestamp").reset_index(drop=True), report


# ---------------------------------------------------------------- main

def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--out", default="training_data.csv")
    parser.add_argument("--bucket", default=os.environ.get("S3_BUCKET"))
    parser.add_argument("--prefix", action="append", dest="prefixes",
                        help="bucket prefix(es) to read, in priority order (default: ndw/ ndw_laptop/)")
    parser.add_argument("--local-dir", help="read traffic files from a local folder instead of S3")
    args = parser.parse_args(argv)
    prefixes = tuple(args.prefixes or DEFAULT_PREFIXES)

    if args.local_dir:
        objects = local_objects(args.local_dir, prefixes)
    elif args.bucket:
        objects = s3_objects(args.bucket, prefixes)
    else:
        parser.error("set S3_BUCKET (or --bucket), or use --local-dir")

    traffic, traffic_report = load_traffic(objects)
    no2, no2_report = prepare_no2(no2_from_db())
    table, build_report = build(no2, traffic)
    table.to_csv(args.out, index=False)

    print("=== training data build report ===")
    for name, value in {**traffic_report, **no2_report, **build_report}.items():
        print(f"{name:28} {value}")
    print(f"\nwrote {len(table)} rows to {args.out}")
    if len(table) < 24:
        print("NOTE: fewer than 24 rows - expected this early; record it as a limitation in ADR-006.")
    return 0


if __name__ == "__main__":
    sys.exit(main())