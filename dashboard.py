"""
dashboard.py - AirBreda dashboard + API (Day 4 Lab 2, graded on Day 5).

    uvicorn dashboard:app --host 0.0.0.0 --port 8000

Routes
    GET /site/{site_id}   site_id in hrl, hrr, vwd, vwa
    GET /health           both ingestion sources side by side (Day 5 shape)
    GET /model            the model card of the model that is serving
    GET /history          the last 24 hours: measured NO2, total traffic and the model's
                          prediction for that traffic, plus the live error of the model
    GET /                 the human dashboard - just another client of /site/{id}

WHERE EACH FIELD OF /site/{id} COMES FROM (required comment, Day 4)
    site_id                  the URL
    no2_ug_m3                RDS sensor_readings: newest NO2 for station NL10240 that is
                             not null and not flagged (stale/null rows are kept in the
                             database by ingest_air.py, but never shown as "the" reading).
                             The same real station covers all four sites (Day 3/4).
    intensity_veh_per_hr     S3: that site's file for the newest COMPLETE traffic hour,
                             ndw/YYYY-MM-DD/HH-<site>.csv, summed over valid lanes
                             (features.site_intensity: NDW's -1 never counts).
    no2_ug_m3_predicted      predict.predict(total_intensity_veh_per_hr, hour_of_day)
    no2_exceedance_risk      same call: sigmoid around the threshold in predict.py
    timestamp                when the NO2 value was measured (its Luchtmeetnet label, UTC)
    no2_hour_utc             the hour the NO2 value describes (its label marks the END of
                             that hour, features.no2_window). When it equals traffic_hour_utc,
                             actual and predicted describe the SAME hour and can be compared.
    threshold_ug_m3          the exceedance threshold the risk refers to (predict.py)
    total_intensity_veh_per_hr, traffic_hour_utc
                             the model's INPUT: the sum of all four sites in that hour and
                             the hour itself. The model was trained on the TOTAL, so the
                             total - not this one site - goes in (no training-serving
                             skew). That's why all four sites share one prediction.

IF predict() RAISES (required comment, Day 4)
The request does NOT fail. It degrades: no2_ug_m3 and intensity_veh_per_hr are still
returned (they are real measurements, and independent of the model), while
no2_ug_m3_predicted and no2_exceedance_risk are null and "warnings" says why. A broken
model must not hide real air-quality data from a policy officer; and a null is honest,
where a made-up default (e.g. risk 0) would look like a real, reassuring answer.
The same applies the other way: if S3 or RDS is unreachable, whatever is still
available is returned. Only if NO data at all is available does the route answer 503.

S3 ACCESS WITH ONLY s3:GetObject (Day 4 least privilege)
The latest file is found by BUILDING its key from the clock (this hour, then the hours
before), not by listing the bucket - so the VM role needs no s3:ListBucket. Note: without
ListBucket, S3 answers a missing key with AccessDenied (403) instead of NoSuchKey (404),
so both are treated as "not there (yet)".
"""
import io
import json
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from functools import lru_cache
from pathlib import Path

import pandas as pd
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse

import features
import predict as model_api
from airbreda_common import get_connection, log_event, setup_logging, utc_iso

import logging

STATION = "NL10240"
SITES = features.SITES                       # ("hrl", "hrr", "vwd", "vwa")
S3_PREFIX = "ndw/"                            # the live pipeline's files
LOOKBACK_HOURS = int(os.environ.get("TRAFFIC_LOOKBACK_HOURS", "6"))
HEALTH_WINDOW_HOURS = 24                      # bad_data_count covers the last 24 hours
CACHE_SECONDS = int(os.environ.get("DASHBOARD_CACHE_SECONDS", "60"))
STALE_AFTER = {"luchtmeetnet": timedelta(hours=3), "ndw": timedelta(hours=2)}
MODEL_CARD = Path(__file__).with_name("model_card.json")

setup_logging()
# Keep stdout pure JSON (Day 5: structured logging). boto3 would otherwise print
# plain-text lines like "Found credentials in environment variables."
for noisy in ("botocore", "boto3", "urllib3", "s3transfer"):
    logging.getLogger(noisy).setLevel(logging.WARNING)
S3_PARALLEL = 16                              # S3 GETs in flight at once

# Where is this instance running? Shown as a badge on the page and in /model.
RUNTIMES = {"local": "Local (laptop)", "docker-laptop": "Docker (laptop)",
            "aws-ec2": "AWS EC2 (eu-west-1)"}


def runtime():
    env = os.environ.get("DEPLOY_ENV") or ("docker" if Path("/.dockerenv").exists() else "local")
    return {"environment": env, "label": RUNTIMES.get(env, env)}
app = FastAPI(title="AirBreda", version="1.0")


# ---------------------------------------------------------------- small TTL cache

_cache, _cache_lock, _key_locks = {}, threading.Lock(), {}


def cached(key, seconds, compute):
    """Return compute() at most once per `seconds`, and compute it only ONCE at a time.

    One page load fires four /site calls at the same moment. Without the per-key
    lock, all four would miss the empty cache and each do the full RDS + S3 work
    (a "cache stampede"). With it, the first computes and the others wait for it.
    """
    with _cache_lock:
        lock = _key_locks.setdefault(key, threading.Lock())
    with lock:
        hit = _cache.get(key)
        if hit and time.monotonic() - hit[0] < seconds:
            return hit[1]
        value = compute()
        _cache[key] = (time.monotonic(), value)
        return value


def now_utc():
    return datetime.now(timezone.utc)


# ---------------------------------------------------------------- data access: RDS

def latest_no2():
    """(value, measured_at) of the newest usable NO2 reading."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""SELECT timestamp, value FROM sensor_readings
                           WHERE station_id = %s AND component = 'NO2'
                             AND value IS NOT NULL AND NOT COALESCE(is_flagged, FALSE)
                           ORDER BY timestamp DESC LIMIT 1""", (STATION,))
            row = cur.fetchone()
    finally:
        conn.close()
    return (float(row[1]), row[0]) if row else (None, None)


def no2_health(hours=HEALTH_WINDOW_HOURS):
    """Newest NO2 reading time + flagged (null/stale) readings in the window."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""SELECT MAX(timestamp),
                                  COUNT(*) FILTER (WHERE is_flagged AND timestamp > now() - %s * interval '1 hour')
                           FROM sensor_readings WHERE station_id = %s AND component = 'NO2'""",
                        (hours, STATION))
            newest, flagged = cur.fetchone()
    finally:
        conn.close()
    return newest, int(flagged or 0)


# ---------------------------------------------------------------- data access: S3

@lru_cache(maxsize=1)
def s3_client():
    """One client for the whole process: creating one per call repeats the credential lookup."""
    import boto3
    from botocore.config import Config
    return boto3.client("s3", config=Config(max_pool_connections=S3_PARALLEL))


def s3_get(key):
    """(bytes, last_modified) for a key, or None if it isn't there (yet)."""
    from botocore.exceptions import ClientError
    try:
        obj = s3_client().get_object(Bucket=os.environ["S3_BUCKET"], Key=key)
        return obj["Body"].read(), obj["LastModified"]
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") in ("NoSuchKey", "AccessDenied", "404", "403"):
            return None
        raise


def site_key(site, window):
    return f"{S3_PREFIX}{window:%Y-%m-%d}/{window:%H}-{site}.csv"


def fetch_many(get, keys):
    """GET many keys IN PARALLEL: 96 files take about as long as a handful, not 96 in a row."""
    keys = list(keys)
    with ThreadPoolExecutor(max_workers=S3_PARALLEL) as pool:
        return dict(zip(keys, pool.map(get, keys)))


def count_invalid(csv_bytes):
    """NDW sentinel values (-1) in one file: the lane format keeps them, by design."""
    df = pd.read_csv(io.BytesIO(csv_bytes))
    if "invalid_values" in df.columns:                      # summary format
        return int(df["invalid_values"].fillna(0).sum())
    return int((pd.to_numeric(df.get("value"), errors="coerce") < 0).sum())


def latest_traffic(get=s3_get, now=None, lookback=LOOKBACK_HOURS):
    """The newest hour for which ALL FOUR sites have a file.

    Returns {"window", "per_site", "total", "uploaded_at"} or None. The newest hour
    can be half-written (the cron job uploads site by site), hence "complete".
    """
    newest = features.traffic_window(now or now_utc())
    windows = [newest - timedelta(hours=back) for back in range(lookback + 1)]
    found = fetch_many(get, (site_key(site, w) for w in windows for site in SITES))
    for window in windows:                                    # newest first
        files = {site: found[site_key(site, window)] for site in SITES}
        if all(files.values()):
            per_site = {site: features.site_intensity(body)[0] for site, (body, _) in files.items()}
            return {"window": window, "per_site": per_site,
                    "total": features.total_intensity(per_site),
                    "uploaded_at": max(modified for _, modified in files.values())}
    return None


def ndw_health(get=s3_get, now=None, hours=HEALTH_WINDOW_HOURS):
    """Newest upload time + NDW sentinel values in the files of the last `hours`."""
    newest_window = features.traffic_window(now or now_utc())
    keys = [site_key(site, newest_window - timedelta(hours=back)) for back in range(hours) for site in SITES]
    last_upload, bad = None, 0
    for found in fetch_many(get, keys).values():
        if found:
            body, modified = found
            bad += count_invalid(body)
            last_upload = modified if last_upload is None else max(last_upload, modified)
    return last_upload, bad


# ---------------------------------------------------------------- history (chart + live error)

HISTORY_HOURS = 24


def no2_history(hours=HISTORY_HOURS):
    """Usable (not flagged, not null) NO2 per hour WINDOW for the last `hours`."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""SELECT timestamp, value FROM sensor_readings
                           WHERE station_id = %s AND component = 'NO2'
                             AND value IS NOT NULL AND NOT COALESCE(is_flagged, FALSE)
                             AND timestamp > now() - %s * interval '1 hour'""",
                        (STATION, hours + 2))
            rows = cur.fetchall()
    finally:
        conn.close()
    return {features.no2_window(ts): float(v) for ts, v in rows}


def traffic_history(get=s3_get, now=None, hours=HISTORY_HOURS):
    """Total intensity per hour window, only for hours where all four sites have a file."""
    newest = features.traffic_window(now or now_utc())
    windows = [newest - timedelta(hours=back) for back in range(hours)]
    found = fetch_many(get, (site_key(site, w) for w in windows for site in SITES))
    totals = {}
    for w in windows:
        files = [found[site_key(site, w)] for site in SITES]
        if all(files):
            per_site = {site: features.site_intensity(f[0])[0] for site, f in zip(SITES, files)}
            totals[w] = features.total_intensity(per_site)
    return totals


def build_history(no2_by_hour, traffic_by_hour, now=None, hours=HISTORY_HOURS):
    """One row per hour; prediction made from that hour's traffic with the deployed model.

    The live error only uses hours that have BOTH a measured NO2 value and a
    prediction, so measured and predicted always describe the same hour.
    """
    newest = features.traffic_window(now or now_utc())
    rows, errors = [], []
    for back in reversed(range(hours)):
        w = newest - timedelta(hours=back)
        total = traffic_by_hour.get(w)
        predicted = risk = None
        if total is not None:
            try:
                p = model_api.predict(total, features.hour_of_day(w))
                predicted, risk = p["no2_ug_m3_predicted"], p["no2_exceedance_risk"]
            except Exception as exc:
                log_event(logging.ERROR, "predict_failed", error=repr(exc), hour=utc_iso(w))
        actual = no2_by_hour.get(w)
        if actual is not None and predicted is not None:
            errors.append(abs(predicted - actual))
        rows.append({"hour_utc": utc_iso(w), "local_hour": features.hour_of_day(w),
                     "no2_actual": actual, "no2_predicted": predicted,
                     "exceedance_risk": risk, "total_intensity_veh_per_hr": total})
    live = {"hours_compared": len(errors),
            "mae_ug_m3": round(sum(errors) / len(errors), 2) if errors else None}
    # The threshold is the WHO 24-HOUR guideline, so the meaningful comparison is the
    # average of the measured hours over the window - the guideline used as intended.
    measured = [r["no2_actual"] for r in rows if r["no2_actual"] is not None]
    mean = round(sum(measured) / len(measured), 1) if measured else None
    daily = {"mean_ug_m3": mean, "hours_measured": len(measured),
             "above_guideline": (mean > model_api.THRESHOLD_UG_M3) if mean is not None else None}
    return {"hours": rows, "live_error": live, "no2_24h_mean": daily,
            "threshold_ug_m3": model_api.THRESHOLD_UG_M3}


# ---------------------------------------------------------------- one snapshot for all sites

def snapshot():
    """Everything /site/{id} needs, computed once and shared by the four sites."""
    warnings = []
    try:
        no2, no2_at = latest_no2()
        if no2 is None:
            warnings.append("no_no2_reading_in_database")
    except Exception as exc:
        no2, no2_at = None, None
        warnings.append("no2_unavailable")
        log_event(logging.ERROR, "no2_read_failed", error=str(exc))

    try:
        traffic = latest_traffic()
        if traffic is None:
            warnings.append(f"no_complete_traffic_hour_in_last_{LOOKBACK_HOURS}h")
    except Exception as exc:
        traffic = None
        warnings.append("traffic_unavailable")
        log_event(logging.ERROR, "traffic_read_failed", error=str(exc))

    prediction = {"no2_ug_m3_predicted": None, "no2_exceedance_risk": None}
    if traffic and traffic["total"] is not None:
        try:
            prediction = model_api.predict(traffic["total"], features.hour_of_day(traffic["window"]))
        except Exception as exc:                 # degrade, don't fail (see module docstring)
            warnings.append("prediction_unavailable")
            log_event(logging.ERROR, "predict_failed", error=repr(exc))
    return {"no2": no2, "no2_at": no2_at, "traffic": traffic, "prediction": prediction,
            "warnings": warnings}


# ---------------------------------------------------------------- routes

@app.middleware("http")
async def log_requests(request: Request, call_next):
    started = time.perf_counter()
    response = await call_next(request)
    log_event(logging.INFO, "http_request", method=request.method, path=request.url.path,
              status=response.status_code, duration_ms=round((time.perf_counter() - started) * 1000, 1))
    return response


@app.get("/site/{site_id}")
def site(site_id: str):
    if site_id not in SITES:
        raise HTTPException(status_code=404, detail=f"unknown site '{site_id}', use one of {list(SITES)}")
    s = cached("snapshot", CACHE_SECONDS, snapshot)
    traffic = s["traffic"]
    if s["no2"] is None and traffic is None:
        raise HTTPException(status_code=503, detail={"warnings": s["warnings"]})
    return {
        # --- the Day 5 contract ---
        "site_id": site_id,
        "no2_ug_m3": s["no2"],
        "intensity_veh_per_hr": traffic["per_site"][site_id] if traffic else None,
        "no2_exceedance_risk": s["prediction"]["no2_exceedance_risk"],
        "timestamp": utc_iso(s["no2_at"]) if s["no2_at"] else None,
        # --- extra, for the dashboard and for transparency ---
        "no2_ug_m3_predicted": s["prediction"]["no2_ug_m3_predicted"],
        "total_intensity_veh_per_hr": traffic["total"] if traffic else None,
        "traffic_hour_utc": utc_iso(traffic["window"]) if traffic else None,
        "no2_hour_utc": utc_iso(features.no2_window(s["no2_at"])) if s["no2_at"] else None,
        "threshold_ug_m3": model_api.THRESHOLD_UG_M3,
        "warnings": s["warnings"],
    }


@app.get("/health")
def health():
    def compute():
        status, result = "ok", {}
        for source, read in (("luchtmeetnet", no2_health), ("ndw", ndw_health)):
            try:
                last, bad = read()
                result[source] = {"last_successful_fetch": utc_iso(last) if last else None,
                                  "bad_data_count": bad}
                if last is None or now_utc() - last > STALE_AFTER[source]:
                    status = "degraded"
            except Exception as exc:
                result[source] = {"last_successful_fetch": None, "bad_data_count": None,
                                  "error": str(exc)}
                status = "degraded"
                log_event(logging.ERROR, "health_check_failed", source=source, error=str(exc))
        return {"status": status, **result, "bad_data_window_hours": HEALTH_WINDOW_HOURS}
    return cached("health", 300, compute)


@app.get("/history")
def history():
    def compute():
        no2, traffic, warnings = {}, {}, []
        try:
            no2 = no2_history()
        except Exception as exc:
            warnings.append("no2_unavailable")
            log_event(logging.ERROR, "history_no2_failed", error=str(exc))
        try:
            traffic = traffic_history()
        except Exception as exc:
            warnings.append("traffic_unavailable")
            log_event(logging.ERROR, "history_traffic_failed", error=str(exc))
        return {**build_history(no2, traffic), "warnings": warnings}
    return cached("history", 300, compute)


@app.get("/model")
def model():
    try:
        card = json.loads(MODEL_CARD.read_text(encoding="utf-8"))
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="model_card.json not found")
    keep = ("chosen_candidate", "description", "trained_at", "training_rows", "training_period_utc",
            "coefficients", "metrics", "limitations")
    return {**{k: card.get(k) for k in keep}, "threshold_ug_m3": model_api.THRESHOLD_UG_M3,
            "runtime": runtime()}


@app.get("/", response_class=HTMLResponse)
def page():
    return HTMLResponse(PAGE)


PAGE = """<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>AirBreda · A27/Breda NO₂</title>
<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.min.js"></script>
<style>
  :root{--ink:#1f2933;--muted:#616e7c;--line:#e4e7eb;--bg:#f5f7fa;--card:#fff;
        --ok:#1e7e46;--okbg:#e3f5ea;--warn:#8a5a00;--warnbg:#fff3d6;--bad:#a61b1b;--badbg:#fde2e2;--accent:#0b3d5c}
  *{box-sizing:border-box}
  body{font-family:system-ui,"Segoe UI",Arial,sans-serif;margin:0;background:var(--bg);color:var(--ink)}
  header{background:var(--accent);color:#fff;padding:16px 24px}
  header h1{margin:0;font-size:20px;display:flex;flex-wrap:wrap;align-items:center;gap:10px}
  .env{font-size:13px;font-weight:600;background:rgba(255,255,255,.18);border:1px solid rgba(255,255,255,.35);
       padding:3px 10px;border-radius:999px}
  header p{margin:4px 0 0;font-size:13px;opacity:.85}
  main{max-width:1060px;margin:0 auto;padding:18px 20px 28px}
  .status{display:flex;flex-wrap:wrap;gap:10px;align-items:center;background:var(--card);border-radius:10px;
          padding:10px 14px;box-shadow:0 1px 2px rgba(0,0,0,.06);font-size:14px}
  .pill{font-weight:700;padding:3px 10px;border-radius:999px}
  .ok{background:var(--okbg);color:var(--ok)} .warn{background:var(--warnbg);color:var(--warn)} .bad{background:var(--badbg);color:var(--bad)}
  .grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(230px,1fr));gap:12px;margin-top:12px}
  .card{background:var(--card);border-radius:10px;padding:14px 16px;box-shadow:0 1px 2px rgba(0,0,0,.06)}
  .label{font-size:12px;color:var(--muted);text-transform:uppercase;letter-spacing:.05em}
  .big{font-size:32px;font-weight:700;margin:6px 0 2px} .unit{font-size:14px;font-weight:400;color:var(--muted)}
  .sub{font-size:13px;color:var(--muted);line-height:1.45}
  .bar{height:8px;background:var(--line);border-radius:4px;overflow:hidden;margin-top:6px}
  .bar>span{display:block;height:100%}
  h2{font-size:15px;margin:22px 0 8px}
  table{width:100%;border-collapse:collapse;background:var(--card);border-radius:10px;overflow:hidden;box-shadow:0 1px 2px rgba(0,0,0,.06)}
  th,td{padding:9px 12px;text-align:left;border-bottom:1px solid var(--line);font-size:14px}
  th{background:#eef2f5;font-size:12px;color:#3e4c59;text-transform:uppercase;letter-spacing:.04em}
  td.num{font-variant-numeric:tabular-nums}
  .share{display:flex;align-items:center;gap:8px} .share .bar{flex:1;margin:0;max-width:140px}
  .note{font-size:12px;color:var(--muted);margin-top:6px}
  .two{display:grid;grid-template-columns:repeat(auto-fit,minmax(320px,1fr));gap:12px}
  footer{font-size:12px;color:var(--muted);margin-top:18px}
  a{color:var(--accent)}
</style></head>
<body>
<header>
  <h1>AirBreda — NO₂ and traffic at the A27 / Breda interchange <span class="env" id="env">…</span></h1>
  <p>Measured NO₂ at Luchtmeetnet station NL10240 (Breda-Tilburgseweg), traffic at four NDW sites, and a model prediction</p>
</header>
<main>
  <div class="status" id="status">Loading…</div>

  <div class="grid">
    <div class="card"><div class="label">Actual NO₂ · measured</div>
      <div class="big" id="actual">–</div><div class="sub" id="actualSub"></div></div>
    <div class="card"><div class="label">Predicted NO₂ · model</div>
      <div class="big" id="pred">–</div><div class="sub" id="predSub"></div></div>
    <div class="card"><div class="label">Exceedance risk</div>
      <div class="big" id="risk">–</div><div class="bar"><span id="riskBar"></span></div>
      <div class="sub" id="riskSub"></div></div>
    <div class="card"><div class="label">Total traffic · 4 sites</div>
      <div class="big" id="total">–</div><div class="sub" id="totalSub"></div></div>
  </div>

  <h2>Per site</h2>
  <table>
    <thead><tr><th>Site</th><th>Location</th><th>Intensity</th><th>Share of total</th>
      <th>Predicted NO₂</th><th>Exceedance risk</th></tr></thead>
    <tbody id="rows"></tbody>
  </table>
  <div class="note">The model predicts NO₂ for the whole interchange from the <b>total</b> traffic (that is how it was trained),
    so predicted NO₂ and risk are the same for every site. Intensity and share are per site.</div>

  <h2>Last 24 hours — measured vs predicted NO₂</h2>
  <div class="status" id="daily" style="margin-bottom:10px">Loading…</div>
  <div class="card">
    <div style="position:relative;height:300px"><canvas id="chart" aria-label="Measured and predicted NO2 with total traffic, last 24 hours"></canvas></div>
    <div class="sub" id="chartNote" style="margin-top:8px">Loading…</div>
  </div>

  <div class="two">
    <div><h2>Data pipeline</h2><table><thead><tr><th>Source</th><th>Last successful fetch</th><th>Bad data (24 h)</th></tr></thead>
      <tbody id="health"><tr><td colspan="3">Loading…</td></tr></tbody></table>
      <div class="note">Bad data: NO₂ readings flagged as null or stale (kept, flagged) · NDW lanes reporting −1 (left out of totals).</div></div>
    <div><h2>Model accuracy</h2><div class="card sub" id="model">Loading…</div></div>
  </div>

  <footer id="footer"></footer>
</main>
<script>
const SITES = {hrl:"A27 mainline, direction 1", hrr:"A27 mainline, direction 2",
               vwd:"Entry slip road (leaving Breda)", vwa:"Exit slip road (entering Breda)"};
const STALE_MIN = {no2: 180, traffic: 120};
const $ = id => document.getElementById(id);
const num = (v, d=1) => (v === null || v === undefined) ? "–" : Number(v).toFixed(d);
const minutesAgo = iso => iso ? Math.round((Date.now() - new Date(iso)) / 60000) : null;
const ago = iso => { const m = minutesAgo(iso); if (m === null) return "unknown";
  return m < 60 ? `${m} min ago` : `${Math.floor(m/60)} h ${m%60} min ago`; };
const TZ = {timeZone: "Europe/Amsterdam"};
const local = iso => iso ? new Date(iso).toLocaleString("en-GB", {...TZ, weekday:"short", hour:"2-digit", minute:"2-digit"}) : "unknown";
const hourRange = iso => { if (!iso) return "unknown"; const a = new Date(iso), b = new Date(a.getTime() + 3600e3);
  const f = d => d.toLocaleTimeString("en-GB", {...TZ, hour:"2-digit", minute:"2-digit"}); return `${f(a)}–${f(b)}`; };
const riskClass = r => r < 0.33 ? "ok" : r < 0.66 ? "warn" : "bad";
const riskColour = r => r < 0.33 ? "#2f9e5b" : r < 0.66 ? "#e0a020" : "#d64545";

async function getJSON(url) { const r = await fetch(url); return r.ok ? r.json() : null; }

let chart = null;
function drawHistory(hist, model) {
  const rows = hist.hours;
  const labels = rows.map(r => `${String(r.local_hour).padStart(2, "0")}:00`);
  const data = {
    labels,
    datasets: [
      {type: "line", label: "Measured NO₂ (µg/m³)", data: rows.map(r => r.no2_actual), borderColor: "#0b3d5c",
       backgroundColor: "#0b3d5c", borderWidth: 2.5, pointRadius: 2, spanGaps: false, yAxisID: "y"},
      {type: "line", label: "Predicted NO₂ (µg/m³)", data: rows.map(r => r.no2_predicted), borderColor: "#e0a020",
       backgroundColor: "#e0a020", borderWidth: 2.5, borderDash: [6, 4], pointRadius: 2, yAxisID: "y"},
      {type: "line", label: `Threshold ${hist.threshold_ug_m3} µg/m³`, data: rows.map(() => hist.threshold_ug_m3),
       borderColor: "#d64545", borderWidth: 1.5, borderDash: [2, 3], pointRadius: 0, yAxisID: "y"},
      {type: "bar", label: "Total traffic (veh/h)", data: rows.map(r => r.total_intensity_veh_per_hr),
       backgroundColor: "rgba(74,127,167,0.18)", borderWidth: 0, yAxisID: "y2"},
    ],
  };
  const options = {
    responsive: true, maintainAspectRatio: false, interaction: {mode: "index", intersect: false},
    scales: {
      y:  {position: "left", beginAtZero: true, title: {display: true, text: "NO₂ (µg/m³)"}},
      y2: {position: "right", beginAtZero: true, grid: {drawOnChartArea: false}, title: {display: true, text: "vehicles / hour"}},
      x:  {title: {display: true, text: "hour (Dutch local time)"}},
    },
    plugins: {legend: {position: "bottom"}},
  };
  if (typeof Chart === "undefined") {
    $("chartNote").textContent = "Chart library could not be loaded.";
  } else if (chart) {
    chart.data = data; chart.update();
  } else {
    chart = new Chart(document.getElementById("chart"), {data, options});
  }
  const d = hist.no2_24h_mean;
  $("daily").innerHTML = d.mean_ug_m3 === null
    ? "No measured NO₂ in the last 24 hours."
    : `<span class="pill ${d.above_guideline ? "bad" : "ok"}">${d.above_guideline ? "Above" : "Below"} WHO 24-hour guideline</span>
       <span><b>Average measured NO₂, last 24 h: ${num(d.mean_ug_m3)} µg/m³</b> (${d.hours_measured} hours)
       · guideline ${hist.threshold_ug_m3} µg/m³ as a 24-hour average</span>`;

  const live = hist.live_error;
  const trained = model && model.metrics ? num(model.metrics.held_out.mae) : "–";
  $("chartNote").innerHTML = live.hours_compared
    ? `<b>Live error, last 24 h:</b> ${num(live.mae_ug_m3)} µg/m³ on average over ${live.hours_compared} hours where both
       the measured and the predicted value exist (training estimate: ${trained} µg/m³). Gaps mean that hour's NO₂ is not
       published yet, or not all four traffic sites were collected.`
    : `No hour in the last 24 h has both a measured and a predicted value yet.`;
}

async function refresh() {
  // The page is just another client of the API: one fetch per site, as specified.
  const responses = await Promise.all([
    fetch("/site/hrl"), fetch("/site/hrr"), fetch("/site/vwd"), fetch("/site/vwa")]);
  const sites = (await Promise.all(responses.map(r => r.ok ? r.json() : null))).filter(Boolean);
  const [health, model] = await Promise.all([getJSON("/health"), getJSON("/model")]);
  if (!sites.length) { $("status").innerHTML = '<span class="pill bad">API unreachable</span>'; return; }

  const s = sites[0];                                   // NO2 + prediction are shared by all sites
  const total = sites.reduce((sum, r) => sum + (r.intensity_veh_per_hr || 0), 0);
  const threshold = s.threshold_ug_m3;

  // ---- status bar: can these numbers be trusted right now?
  const no2Age = minutesAgo(s.timestamp), trafficAge = minutesAgo(s.traffic_hour_utc && new Date(new Date(s.traffic_hour_utc).getTime()+3600e3).toISOString());
  const stale = [];
  if (no2Age === null || no2Age > STALE_MIN.no2) stale.push("NO₂");
  if (trafficAge === null || trafficAge > STALE_MIN.traffic) stale.push("traffic");
  $("status").innerHTML = (stale.length
      ? `<span class="pill warn">Stale: ${stale.join(" and ")}</span><span>The ingestion jobs may have stopped.</span>`
      : `<span class="pill ok">Live</span>`)
    + `<span>NO₂ measured ${ago(s.timestamp)}</span><span>· traffic hour ${hourRange(s.traffic_hour_utc)} (Dutch time)</span>`
    + (s.warnings && s.warnings.length ? `<span class="pill warn">${s.warnings.join(", ")}</span>` : "");

  // ---- actual
  $("actual").innerHTML = `${num(s.no2_ug_m3)} <span class="unit">µg/m³</span>`;
  $("actualSub").innerHTML = s.no2_ug_m3 === null ? "No usable reading" :
    `Hour ${hourRange(s.no2_hour_utc)} · ${s.no2_ug_m3 > threshold ? "<b>above</b>" : "below"} the ${threshold} µg/m³ threshold`;

  // ---- predicted, and how far off it is (only when both describe the same hour)
  $("pred").innerHTML = `${num(s.no2_ug_m3_predicted)} <span class="unit">µg/m³</span>`;
  if (s.no2_ug_m3_predicted === null) {
    $("predSub").textContent = "Prediction unavailable — real readings are still shown";
  } else if (s.no2_hour_utc && s.no2_hour_utc === s.traffic_hour_utc && s.no2_ug_m3 !== null) {
    const diff = s.no2_ug_m3_predicted - s.no2_ug_m3;
    $("predSub").innerHTML = `Same hour as the measurement · model is <b>${Math.abs(diff).toFixed(1)} µg/m³ ${diff >= 0 ? "above" : "below"}</b> the actual`;
  } else {
    $("predSub").textContent = `From traffic in hour ${hourRange(s.traffic_hour_utc)} (the measured NO₂ for that hour isn't published yet)`;
  }

  // ---- risk
  const r = s.no2_exceedance_risk;
  $("risk").innerHTML = r === null ? "–" : `<span class="pill ${riskClass(r)}" style="font-size:26px">${Math.round(r*100)}%</span>`;
  $("riskBar").style.width = r === null ? "0" : `${Math.round(r*100)}%`;
  $("riskBar").style.background = r === null ? "transparent" : riskColour(r);
  $("riskSub").textContent = `Chance the predicted hour exceeds ${threshold} µg/m³ (WHO 24-hour guideline, applied hourly)`;

  // ---- total traffic
  $("total").innerHTML = `${Math.round(total).toLocaleString("en-GB")} <span class="unit">veh/h</span>`;
  $("totalSub").textContent = `Sum of the four sites · hour ${hourRange(s.traffic_hour_utc)}`;

  // ---- per site
  $("rows").innerHTML = sites.map(x => {
    const share = total ? (x.intensity_veh_per_hr || 0) / total : 0;
    const rr = x.no2_exceedance_risk;
    return `<tr><td><b>${x.site_id}</b></td><td>${SITES[x.site_id]}</td>
      <td class="num">${x.intensity_veh_per_hr === null ? "–" : Math.round(x.intensity_veh_per_hr).toLocaleString("en-GB")} veh/h</td>
      <td><div class="share"><div class="bar"><span style="width:${Math.round(share*100)}%;background:#4a7fa7"></span></div>${Math.round(share*100)}%</div></td>
      <td class="num">${num(x.no2_ug_m3_predicted)} µg/m³</td>
      <td>${rr === null ? "–" : `<span class="pill ${riskClass(rr)}">${Math.round(rr*100)}%</span>`}</td></tr>`; }).join("");

  // ---- pipeline health (same data as /health)
  if (health) {
    const row = (name, h) => `<tr><td>${name}</td><td>${h && h.last_successful_fetch ? `${local(h.last_successful_fetch)} <span class="sub">(${ago(h.last_successful_fetch)})</span>` : "unknown"}</td>
      <td class="num">${h && h.bad_data_count !== null ? h.bad_data_count : "–"}</td></tr>`;
    $("health").innerHTML = row("Luchtmeetnet (NO₂)", health.luchtmeetnet) + row("NDW (traffic)", health.ndw);
  }

  // ---- where is this page served from?
  if (model && model.runtime) $("env").textContent = model.runtime.label;

  // ---- model accuracy (from the model card: measured on unseen hours during training)
  if (model && model.metrics) {
    const ho = model.metrics.held_out;
    $("model").innerHTML = `Model <b>${model.chosen_candidate}</b>, trained on <b>${model.training_rows} hours</b> of real data,
      tested on hours it had not seen (leave-one-out).<br>
      <b>Average error (MAE): ${num(ho.mae)} µg/m³</b> · simply predicting the average: ${num(ho.baseline_mae)} µg/m³ ·
      R² on unseen hours: ${num(ho.r2, 2)}<br>
      ${model.metrics.beats_baseline
        ? "The model beats the baseline."
        : "<b>It does not beat the baseline yet</b> — treat the prediction as indicative until more data is collected."}`;
  }

  // ---- last 24 hours: measured vs predicted, with traffic
  const hist = await getJSON("/history");
  if (hist) drawHistory(hist, model);

  $("footer").innerHTML = `Refreshes every 60 s · last refresh ${new Date().toLocaleTimeString("en-GB", TZ)} · all times Dutch local time ·
    API: <a href="/site/hrl">/site/hrl</a> · <a href="/health">/health</a> · <a href="/model">/model</a>`;
}
refresh().catch(e => { $("status").innerHTML = `<span class="pill bad">Error: ${e}</span>`; });
setInterval(() => refresh().catch(() => {}), 60000);
</script>
</body></html>
"""
