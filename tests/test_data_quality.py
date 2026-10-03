"""Day 2 data-quality tests."""
import logging
from datetime import datetime, timedelta, timezone

import pandas as pd

from airbreda_common import HealthState
from ingest_air import AirIngestor
from ingest_traffic import site_readings


def _hourly(values, start="2026-10-01T00:00:00+00:00"):
    t0 = pd.Timestamp(start)
    return [{"formula": "NO2", "value": v,
             "timestamp_measured": (t0 + pd.Timedelta(hours=i)).isoformat()}
            for i, v in enumerate(values)]


class FakeWriter:
    def __init__(self):
        self.rows = []

    def __call__(self, rows):
        self.rows.extend(rows)
        return len(rows), 0


def test_stale_and_null_luchtmeetnet_rows_are_written_with_is_flagged_true():
    """Required: stale/null readings are written to sensor_readings, flagged, NOT dropped."""
    writer = FakeWriter()
    ingestor = AirIngestor(write=writer, publish_fn=lambda msgs: len(msgs))

    # 10 | 20 20 20 (stale run of 3) | None (null) | 30
    ingestor.process(_hourly([10.0, 20.0, 20.0, 20.0, None, 30.0]))

    assert len(writer.rows) == 6                     # nothing dropped
    flags = [row[4] for row in writer.rows]          # (station, ts, component, value, is_flagged)
    assert flags == [False, True, True, True, True, False]
    assert writer.rows[4][3] is None                 # the null is stored as NULL
    assert ingestor.state.bad_data_count == 4        # luchtmeetnet_bad_data_count


def test_refetching_the_same_window_does_not_recount_bad_data():
    """Every poll re-reads ~50 hours; one bad hour must be counted once, not every hour."""
    ingestor = AirIngestor(write=FakeWriter(), publish_fn=lambda msgs: len(msgs))
    records = _hourly([10.0, None, 12.0])
    ingestor.process(records)
    ingestor.process(records)
    assert ingestor.state.bad_data_count == 1


def test_two_identical_hours_are_not_stale():
    writer = FakeWriter()
    AirIngestor(write=writer, publish_fn=lambda m: len(m)).process(_hourly([20.0, 20.0, 25.0]))
    assert [row[4] for row in writer.rows] == [False, False, False]


def test_ndw_speed_minus_one_is_not_written_and_increments_ndw_bad_data_count():
    """Required: NDW speed=-1 is not written to sensor_readings and is counted."""
    state = HealthState("NDW")   # = ndw_bad_data_count
    site = "RWS01_MONIBAS_0271hrl0063ra"
    ts = pd.Timestamp("2026-10-01T09:54:00Z")
    rows = [
        {"site_id": site, "timestamp": "2026-10-01T09:54:00Z", "lane_index": "1", "metric": "flow", "value": 600.0},
        {"site_id": site, "timestamp": "2026-10-01T09:54:00Z", "lane_index": "2", "metric": "speed", "value": -1.0},
        {"site_id": site, "timestamp": "2026-10-01T09:54:00Z", "lane_index": "3", "metric": "flow", "value": 300.0},
        {"site_id": site, "timestamp": "2026-10-01T09:54:00Z", "lane_index": "4", "metric": "speed", "value": -1.0},
    ]
    db_rows, messages, _ = site_readings(site, rows, ts, state)

    components = [r[2] for r in db_rows]
    assert components == ["flow"]                    # no speed row: every speed lane was -1
    assert all(r[3] != -1 for r in db_rows)          # sentinel never reaches the DB
    assert db_rows[0][3] == 900.0
    assert state.bad_data_count == 2


def test_threshold_logs_a_single_error_when_more_than_10_in_an_hour(caplog):
    now = datetime(2026, 10, 1, 12, 0, tzinfo=timezone.utc)
    state = HealthState("NDW", clock=lambda: now)
    with caplog.at_level(logging.ERROR, logger="airbreda"):
        for _ in range(10):
            state.record_bad_data()
        assert not caplog.records                    # 10 is not "more than 10"
        state.record_bad_data()                      # 11th
        state.record_bad_data()                      # 12th: still one alert, not two
    errors = [r for r in caplog.records if "BAD_DATA_THRESHOLD_EXCEEDED" in r.getMessage()]
    assert len(errors) == 1


def test_bad_data_older_than_an_hour_does_not_count_towards_threshold():
    t = [datetime(2026, 10, 1, 10, 0, tzinfo=timezone.utc)]
    state = HealthState("NDW", clock=lambda: t[0])
    for _ in range(10):
        state.record_bad_data()
    t[0] += timedelta(hours=2)
    assert state.record_bad_data() is False          # the old 10 have left the window
    assert state.bad_data_count == 11                # but the lifetime total keeps them


def test_health_snapshot_has_the_required_fields():
    state = HealthState("Luchtmeetnet")
    state.record_success()
    snap = state.snapshot()
    assert set(snap) == {"last_successful_fetch", "bad_data_count", "source"}
    assert snap["source"] == "Luchtmeetnet"
