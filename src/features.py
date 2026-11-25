"""
features.py - the model's features, computed ONE way for training and serving.

build_training_data.py (training) and predict.py / dashboard.py (serving) both
import these functions. If training computed "hour_of_day" or "intensity"
differently from serving, the model would receive inputs at serving time that
don't mean what they meant during training: training-serving skew (ADR-006).
"""
import io
from datetime import timedelta

import pandas as pd

SITES = ("hrl", "hrr", "vwd", "vwa")

# Traffic follows Dutch clocks: rush hour is 07-09 LOCAL time, which is 05-07 UTC
# in summer and 06-08 UTC in winter. hour_of_day in UTC would shift the rush hour
# by one hour when daylight saving time changes, so features use local time.
LOCAL_TZ = "Europe/Amsterdam"

# Which hour does a Luchtmeetnet value labelled "12:00" describe?
#   "end"   -> the hour 11:00-12:00 (label = END of the averaging hour)
#   "start" -> the hour 12:00-13:00
# Evidence for "end" (Day 3): at 13:03 the newest value was labelled 12:00, and
# values appear ~30 min after their hour ends - a 12:00-13:00 average could not
# exist yet at 13:03.Verify against the Luchtmeetnet API documentation.
NO2_LABEL_MARKS = "end"


def to_utc(ts):
    """Any timestamp-like value -> tz-aware pandas Timestamp in UTC."""
    ts = pd.Timestamp(ts)
    return ts.tz_localize("UTC") if ts.tzinfo is None else ts.tz_convert("UTC")


def traffic_window(ts):
    """Start (UTC) of the hour a traffic snapshot was taken in: 08:23 -> 08:00."""
    return to_utc(ts).floor("h")


def no2_window(label):
    """Start (UTC) of the hour a NO2 value describes, from its label."""
    label = to_utc(label).floor("h")
    return label - timedelta(hours=1) if NO2_LABEL_MARKS == "end" else label


def hour_of_day(window_start):
    """Local (Europe/Amsterdam) hour, 0-23, of an hour window's start."""
    return int(to_utc(window_start).tz_convert(LOCAL_TZ).hour)


def site_intensity(csv_bytes):
    """Total intensity (veh/h) of one site-hour file, for BOTH file formats:

    - summary format (Day 1-2):  site_id,label,timestamp,total_flow,avg_speed[,invalid_values]
    - lane format (Day 3+):      site_id,timestamp,lane_index,metric,value

    Same definition in both: the sum of the valid lane flows; NDW's -1 sentinel
    (any negative value) never counts. Returns (intensity or None, measured_at).
    """
    df = pd.read_csv(io.BytesIO(csv_bytes))
    if "total_flow" in df.columns:
        row = df.iloc[0]
        value = row["total_flow"]
        intensity = None if pd.isna(value) or value < 0 else float(value)
        return intensity, row["timestamp"]
    if {"metric", "value"} <= set(df.columns):
        flows = pd.to_numeric(df.loc[df["metric"] == "flow", "value"], errors="coerce")
        flows = flows[flows >= 0]
        measured_at = df["timestamp"].dropna().iloc[0] if df["timestamp"].notna().any() else None
        return (float(flows.sum()) if len(flows) else None), measured_at
    raise ValueError(f"unknown NDW file format, columns: {list(df.columns)}")


def total_intensity(per_site):
    """Sum of the four sites, or None if any site is missing.

    A missing site would make the hour look quieter than it was, so incomplete
    hours are left out of training rather than silently under-counted.
    """
    values = [per_site.get(site) for site in SITES]
    if any(v is None or pd.isna(v) for v in values):
        return None
    return float(sum(values))


# ---------------------------------------------------------------- model input transforms
# Every model receives the SAME two raw inputs, in this order:
#     [total_intensity_veh_per_hr, hour_of_day]
# so predict(total_intensity_veh_per_hr, hour_of_day) never changes, whichever
# candidate wins. These functions live HERE (not in train_model.py) because the
# saved model.pkl refers to them: the dashboard image only needs features.py.

# Typical Dutch commuter peaks (local time). Adjust if your data shows otherwise.
RUSH_HOURS = frozenset({7, 8, 9, 16, 17, 18})


def is_rush_hour(hour):
    return 1 if int(hour) in RUSH_HOURS else 0


def traffic_only(X):
    """[intensity, hour] -> [intensity]"""
    import numpy as np
    return np.asarray(X, dtype=float)[:, [0]]


def traffic_and_rush_hour(X):
    """[intensity, hour] -> [intensity, rush_hour_flag]: 23:00 and 00:00 are neighbours, not opposites."""
    import numpy as np
    X = np.asarray(X, dtype=float)
    return np.column_stack([X[:, 0], [is_rush_hour(h) for h in X[:, 1]]])
