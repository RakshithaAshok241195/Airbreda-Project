"""Tests for building the training data using the features"""
import pandas as pd
import pytest

import build_training_data as btd
import features

# Real summary-format files (Day 1-2 laptop runs, uploaded to ndw_laptop/)
SUMMARY = {
    ("01", "hrl"): "RWS01_MONIBAS_0271hrl0063ra,hrl,2026-10-01T01:49:00Z,60.0,89.0,1",
    ("01", "hrr"): "RWS01_MONIBAS_0271hrr0063ra,hrr,2026-10-01T01:49:00Z,0.0,,2",
    ("01", "vwa"): "RWS01_MONIBAS_0270vwa0063ra,vwa,2026-10-01T01:49:00Z,0.0,,2",
    ("01", "vwd"): "RWS01_MONIBAS_0270vwd0063ra,vwd,2026-10-01T01:49:00Z,0.0,,1",
    ("08", "hrl"): "RWS01_MONIBAS_0271hrl0063ra,hrl,2026-10-01T08:23:00Z,1680.0,108.5,0",
    ("08", "hrr"): "RWS01_MONIBAS_0271hrr0063ra,hrr,2026-10-01T08:23:00Z,1680.0,102.0,0",
    ("08", "vwa"): "RWS01_MONIBAS_0270vwa0063ra,vwa,2026-10-01T08:23:00Z,300.0,39.5,0",
    ("08", "vwd"): "RWS01_MONIBAS_0270vwd0063ra,vwd,2026-10-01T08:23:00Z,420.0,35.0,0",
}
HEADER = "site_id,label,timestamp,total_flow,avg_speed,invalid_values\n"


def summary_objects():
    return [(f"ndw_laptop/2026-10-01/{h}-{s}.csv", (HEADER + line + "\n").encode())
            for (h, s), line in SUMMARY.items()]


def lane_file(site_id, ts, flows):
    lines = ["site_id,timestamp,lane_index,metric,value"]
    lines += [f"{site_id},{ts},{i},flow,{v}" for i, v in enumerate(flows, 1)]
    lines += [f"{site_id},{ts},{i + 10},speed,-1.0" for i in range(len(flows))]
    return ("\n".join(lines) + "\n").encode()


def no2(rows):
    return pd.DataFrame(rows, columns=["timestamp", "value", "is_flagged"])


# ---------------------------------------------------------------- features

def test_both_file_formats_give_the_same_kind_of_intensity():
    summary, _ = features.site_intensity((HEADER + SUMMARY[("08", "hrl")] + "\n").encode())
    lane, _ = features.site_intensity(lane_file("X", "2026-10-01T08:23:00Z", [1000.0, 680.0, -1.0]))
    assert summary == 1680.0 and lane == 1680.0      # -1 lane excluded in both


def test_traffic_snapshots_in_the_same_hour_share_a_window():
    assert features.traffic_window("2026-10-01T08:23:00Z") == features.traffic_window("2026-10-01T08:47:00Z")


def test_no2_label_marks_the_end_of_its_hour():
    # Luchtmeetnet "09:00" describes 08:00-09:00 -> joins traffic measured at 08:23
    assert features.no2_window("2026-10-01T09:00:00+00:00") == features.traffic_window("2026-10-01T08:23:00Z")


def test_hour_of_day_is_dutch_local_time():
    assert features.hour_of_day(pd.Timestamp("2026-10-01T06:00:00Z")) == 8   # summer: UTC+2
    assert features.hour_of_day(pd.Timestamp("2026-12-01T06:00:00Z")) == 7   # winter: UTC+1


def test_total_needs_all_four_sites():
    assert features.total_intensity({"hrl": 1, "hrr": 2, "vwd": 3, "vwa": 4}) == 10.0
    assert features.total_intensity({"hrl": 1, "hrr": 2, "vwd": 3}) is None


# ---------------------------------------------------------------- build

def test_build_joins_real_laptop_files_with_no2():
    traffic, report = btd.load_traffic(summary_objects())
    readings, _ = btd.prepare_no2(no2([("2026-10-01T02:00:00+00:00", 10.75, False),
                                       ("2026-10-01T09:00:00+00:00", 21.9, False)]))
    table, build_report = btd.build(readings, traffic)

    assert report["files"] == 8 and build_report["complete_hours"] == 2
    assert list(table["total_intensity_veh_per_hr"]) == [60.0, 4080.0]
    assert list(table["no2_ug_m3"]) == [10.75, 21.9]
    assert list(table["hour_of_day"]) == [3, 10]              # 01:00 / 08:00 UTC = 03 / 10 Dutch time
    assert list(table.columns) == btd.OUTPUT_COLUMNS


def test_flagged_and_null_no2_are_excluded_from_training():
    readings, report = btd.prepare_no2(no2([("2026-10-01T02:00:00+00:00", 20.0, True),
                                            ("2026-10-01T09:00:00+00:00", None, True),
                                            ("2026-10-01T10:00:00+00:00", 18.0, False)]))
    assert len(readings) == 1
    assert report["no2_flagged"] == 1 and report["no2_null"] == 1


def test_an_hour_with_a_missing_site_is_left_out():
    objects = [o for o in summary_objects() if not o[0].endswith("08-vwd.csv")]
    traffic, _ = btd.load_traffic(objects)
    readings, _ = btd.prepare_no2(no2([("2026-10-01T02:00:00+00:00", 10.75, False),
                                       ("2026-10-01T09:00:00+00:00", 21.9, False)]))
    table, report = btd.build(readings, traffic)
    assert len(table) == 1 and report["incomplete_hours"] == ["2026-10-01 08:00"]


def test_pipeline_file_wins_over_a_laptop_copy_of_the_same_site_hour():
    pipeline = ("ndw/2026-10-01/08-hrl.csv",
                lane_file("RWS01_MONIBAS_0271hrl0063ra", "2026-10-01T08:10:00Z", [999.0]))
    traffic, report = btd.load_traffic([pipeline] + summary_objects())
    hrl_08 = traffic[(traffic["site"] == "hrl") &
                     (traffic["window"] == features.traffic_window("2026-10-01T08:00:00Z"))]
    assert list(hrl_08["intensity"]) == [999.0] and report["duplicates"] == 1


def test_an_unreadable_file_is_reported_not_fatal():
    traffic, report = btd.load_traffic([("ndw/2026-10-01/08-hrl.csv", b"garbage,only\n1,2\n")])
    assert traffic.empty and len(report["unreadable"]) == 1
