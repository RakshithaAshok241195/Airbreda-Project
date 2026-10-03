import pandas as pd

from ingest_air import filter_no2_readings, to_dataframe, to_db_rows


def test_filter_no2_readings_handles_null_value():
    """Course-provided test: null NO2 rows are kept, not silently dropped."""
    df = pd.DataFrame({
        "component": ["NO2", "NO2", "PM10"],
        "value": [18.4, None, 22.1],
        "timestamp": ["2024-01-15T08:00:00Z", "2024-01-15T09:00:00Z", "2024-01-15T08:00:00Z"],
    })
    result = filter_no2_readings(df)
    assert len(result) == 2
    assert result["value"].isnull().sum() == 1


def test_to_dataframe_maps_api_fields_and_adds_station_id():
    """The API record has no station ID; to_dataframe must add it and match the table schema."""
    records = [{
        "value": 44.7,
        "timestamp_measured": "2026-10-01T08:00:00+00:00",
        "formula": "NO2",
        "timestamp_measured_start": "2026-10-01T07:00:00+00:00",
        "timestamp_measured_end": "2026-10-01T08:00:00+00:00",
    }]
    df = to_dataframe(records)
    assert list(df.columns) == ["station_id", "timestamp", "component", "value"]
    assert df.loc[0, "station_id"] == "NL10240"
    assert df.loc[0, "component"] == "NO2"
    assert str(df.loc[0, "timestamp"].tz) == "UTC"


def test_to_dataframe_handles_empty_response():
    """An empty API response must give an empty frame with the right columns, not crash."""
    df = to_dataframe([])
    assert df.empty
    assert list(df.columns) == ["station_id", "timestamp", "component", "value"]


def test_to_db_rows_converts_nan_to_none():
    """pandas NaN must become None so PostgreSQL stores NULL instead of failing."""
    records = [
        {"value": 44.7, "timestamp_measured": "2026-10-01T08:00:00+00:00", "formula": "NO2"},
        {"value": None, "timestamp_measured": "2026-10-01T09:00:00+00:00", "formula": "NO2"},
    ]
    rows = to_db_rows(to_dataframe(records))
    assert rows[0][3] == 44.7
    assert rows[1][3] is None
    assert rows[0][0] == "NL10240"
