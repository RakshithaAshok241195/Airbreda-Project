import pandas as pd

from ingest_traffic import object_key, summarise


def _row(metric, value):
    return {"site_id": "X", "timestamp": "2026-10-01T09:39:00Z", "lane_index": "1",
            "metric": metric, "value": value}


def test_summarise_excludes_negative_sentinel_but_keeps_zero_speed():
    rows = [_row("flow", 600), _row("flow", 0), _row("speed", 0), _row("speed", -1), _row("speed", 90)]
    s = summarise(rows)
    assert s["flow"] == 600
    assert s["speed"] == 45.0          # mean of 0 and 90; -1 is excluded
    assert s["invalid_values"] == 1


def test_summarise_returns_none_when_no_valid_speed():
    s = summarise([_row("flow", 120), _row("speed", -1), _row("speed", -1)])
    assert s["speed"] is None
    assert s["invalid_values"] == 2


def test_object_key_uses_measurement_time_in_utc():
    ts = pd.to_datetime("2026-10-01T23:59:00+02:00", utc=True)   # = 21:59 UTC
    assert object_key("hrl", ts) == "ndw/2026-10-01/21-hrl.csv"
