"""testing the dashboard

 
"""
from datetime import datetime, timedelta, timezone

import pytest
from fastapi.testclient import TestClient

import dashboard

NOW = datetime(2026, 10, 1, 15, 20, tzinfo=timezone.utc)
TRAFFIC = {"window": datetime(2026, 10, 1, 14, tzinfo=timezone.utc),
           "per_site": {"hrl": 1900.0, "hrr": 1600.0, "vwd": 600.0, "vwa": 300.0},
           "total": 4400.0, "uploaded_at": NOW - timedelta(minutes=10)}


@pytest.fixture
def client(monkeypatch):
    dashboard._cache.clear()
    monkeypatch.setattr(dashboard, "latest_no2", lambda: (21.5, NOW - timedelta(minutes=50)))
    monkeypatch.setattr(dashboard, "latest_traffic", lambda: TRAFFIC)
    monkeypatch.setattr(dashboard.model_api, "predict",
                        lambda total, hour: {"no2_ug_m3_predicted": 22.4, "no2_exceedance_risk": 0.37})
    return TestClient(dashboard.app)


# ---------------------------------------------------------------- /site/{id}

@pytest.mark.parametrize("site", ["hrl", "hrr", "vwd", "vwa"])
def test_site_returns_the_day5_contract(client, site):
    body = client.get(f"/site/{site}").json()
    assert body["site_id"] == site
    assert isinstance(body["no2_ug_m3"], float)
    assert body["intensity_veh_per_hr"] == TRAFFIC["per_site"][site]
    assert 0 <= body["no2_exceedance_risk"] <= 1
    assert body["timestamp"].endswith("Z")


def test_all_sites_share_one_prediction_from_the_total(client, monkeypatch):
    calls = []
    monkeypatch.setattr(dashboard.model_api, "predict",
                        lambda total, hour: calls.append((total, hour)) or
                        {"no2_ug_m3_predicted": 22.4, "no2_exceedance_risk": 0.37})
    risks = {client.get(f"/site/{s}").json()["no2_exceedance_risk"] for s in dashboard.SITES}
    assert risks == {0.37}
    assert calls == [(4400.0, 16)]      # the TOTAL goes in; 14:00 UTC = 16 Dutch summer time


def test_actual_and_predicted_hour_are_reported_for_comparison(client):
    body = client.get("/site/hrl").json()
    assert body["no2_hour_utc"] == "2026-10-01T13:00:00Z"      # label 14:30 -> hour 13:00-14:00
    assert body["traffic_hour_utc"] == "2026-10-01T14:00:00Z"
    assert body["threshold_ug_m3"] == 25


def test_unknown_site_is_404(client):
    assert client.get("/site/xyz").status_code == 404


def test_predict_failure_degrades_instead_of_failing(client, monkeypatch):
    def broken(total, hour):
        raise RuntimeError("model.pkl is corrupt")
    monkeypatch.setattr(dashboard.model_api, "predict", broken)
    response = client.get("/site/hrl")
    body = response.json()
    assert response.status_code == 200
    assert body["no2_ug_m3"] == 21.5 and body["intensity_veh_per_hr"] == 1900.0   # real data kept
    assert body["no2_exceedance_risk"] is None and "prediction_unavailable" in body["warnings"]


def test_no_data_at_all_is_503(client, monkeypatch):
    monkeypatch.setattr(dashboard, "latest_no2", lambda: (None, None))
    monkeypatch.setattr(dashboard, "latest_traffic", lambda: None)
    assert client.get("/site/hrl").status_code == 503


# ---------------------------------------------------------------- /health and /

def test_health_has_the_day5_shape(client, monkeypatch):
    recent = datetime.now(timezone.utc) - timedelta(minutes=20)
    monkeypatch.setattr(dashboard, "no2_health", lambda: (recent, 2))
    monkeypatch.setattr(dashboard, "ndw_health", lambda: (recent, 7))
    body = client.get("/health").json()
    assert body["status"] == "ok"
    for source, bad in (("luchtmeetnet", 2), ("ndw", 7)):
        assert set(body[source]) >= {"last_successful_fetch", "bad_data_count"}
        assert body[source]["bad_data_count"] == bad
        assert body[source]["last_successful_fetch"].endswith("Z")


def test_health_reports_degraded_when_a_source_is_stale(client, monkeypatch):
    old = datetime.now(timezone.utc) - timedelta(hours=6)
    monkeypatch.setattr(dashboard, "no2_health", lambda: (old, 0))
    monkeypatch.setattr(dashboard, "ndw_health", lambda: (datetime.now(timezone.utc), 0))
    assert client.get("/health").json()["status"] == "degraded"


def test_page_is_a_client_of_the_site_api(client):
    html = client.get("/").text
    for site in ("hrl", "hrr", "vwd", "vwa"):
        assert f'fetch("/site/{site}")' in html
    assert "setInterval(" in html


# ---------------------------------------------------------------- S3 lookup without ListBucket

LANE = "site_id,timestamp,lane_index,metric,value\nX,2026-10-01T{h}:10:00Z,1,flow,{f}\nX,2026-10-01T{h}:10:00Z,2,speed,-1\n"


def fake_bucket(hours_complete, hour_partial=None):
    files = {}
    for h in hours_complete:
        for site in dashboard.SITES:
            files[f"ndw/2026-10-01/{h:02d}-{site}.csv"] = LANE.format(h=f"{h:02d}", f=1000).encode()
    if hour_partial is not None:                     # an upload still in progress
        files[f"ndw/2026-10-01/{hour_partial:02d}-hrl.csv"] = LANE.format(h=f"{hour_partial:02d}", f=999).encode()
    return lambda key: (files[key], NOW) if key in files else None


def test_traffic_uses_the_newest_complete_hour_not_a_half_uploaded_one():
    traffic = dashboard.latest_traffic(get=fake_bucket([12, 13], hour_partial=15), now=NOW)
    assert traffic["window"].hour == 13
    assert traffic["total"] == 4000.0


def test_no_complete_hour_in_lookback_returns_none():
    assert dashboard.latest_traffic(get=fake_bucket([]), now=NOW, lookback=3) is None


def test_ndw_health_counts_the_minus_one_values_kept_in_the_files():
    last, bad = dashboard.ndw_health(get=fake_bucket([13, 14]), now=NOW, hours=3)
    assert bad == 8 and last == NOW                  # 2 hours x 4 sites x one -1 lane


def test_simultaneous_requests_compute_the_snapshot_only_once(monkeypatch):
    """Four /site calls arrive together on page load: one computation, not four."""
    import threading
    calls = []
    def slow_snapshot():
        calls.append(1)
        import time; time.sleep(0.2)
        return "data"
    dashboard._cache.clear()
    threads = [threading.Thread(target=dashboard.cached, args=("t", 60, slow_snapshot)) for _ in range(4)]
    [t.start() for t in threads]; [t.join() for t in threads]
    assert len(calls) == 1


@pytest.mark.parametrize("env, label", [("local", "Local (laptop)"), ("docker-laptop", "Docker (laptop)"),
                                        ("aws-ec2", "AWS EC2")])
def test_runtime_badge_says_where_the_dashboard_runs(monkeypatch, env, label):
    monkeypatch.setenv("DEPLOY_ENV", env)
    assert label in dashboard.runtime()["label"]


# ---------------------------------------------------------------- /history

def test_history_aligns_actual_and_prediction_on_the_same_hour(monkeypatch):
    from datetime import timedelta
    now = NOW
    newest = dashboard.features.traffic_window(now)
    h1, h2 = newest - timedelta(hours=1), newest - timedelta(hours=2)
    monkeypatch.setattr(dashboard.model_api, "predict",
                        lambda total, hour: {"no2_ug_m3_predicted": 20.0, "no2_exceedance_risk": 0.27})
    out = dashboard.build_history({h1: 30.0, h2: 18.0}, {h1: 4000.0, newest: 3000.0}, now=now, hours=3)
    assert [r["hour_utc"] for r in out["hours"]] == [dashboard.utc_iso(h) for h in (h2, h1, newest)]
    assert out["hours"][0]["no2_predicted"] is None          # h2: NO2 but no traffic
    assert out["hours"][2]["no2_actual"] is None             # newest: traffic, NO2 not published yet
    assert out["live_error"] == {"hours_compared": 1, "mae_ug_m3": 10.0}   # only h1 has both


def test_history_endpoint_degrades_when_a_source_fails(client, monkeypatch):
    def broken(): raise RuntimeError("db down")
    monkeypatch.setattr(dashboard, "no2_history", broken)
    monkeypatch.setattr(dashboard, "traffic_history", lambda: {})
    body = client.get("/history").json()
    assert "no2_unavailable" in body["warnings"] and len(body["hours"]) == 24


def test_page_contains_the_chart(client):
    html = client.get("/").text
    assert 'id="chart"' in html and "chart.umd.min.js" in html and 'getJSON("/history")' in html


def test_history_reports_the_24h_mean_against_the_guideline(monkeypatch):
    from datetime import timedelta
    newest = dashboard.features.traffic_window(NOW)
    no2 = {newest - timedelta(hours=k): v for k, v in [(1, 20.0), (2, 30.0), (3, 34.0)]}
    out = dashboard.build_history(no2, {}, now=NOW, hours=24)
    assert out["no2_24h_mean"] == {"mean_ug_m3": 28.0, "hours_measured": 3, "above_guideline": True}
