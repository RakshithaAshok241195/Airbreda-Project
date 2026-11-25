# AirBreda — Lab notebook

My own answers, in my own words, written while rebuilding the system (retake, November 2026).

---

## Day 1 — System thinking, cloud fundamentals & storage

### Zalando reflection
*If I were an engineer at Zalando in 2016, what would have been my biggest concern about moving to the cloud? What would have excited me?*

>

### Estimation exercise — the €500 cluster
*A cluster runs 24/7 for a job that only runs 08:00–20:00 and costs €500/month. How much is wasted? How would I fix it with cloud-native tooling?*

>

### Lab 1 — Luchtmeetnet API (from running `python src/helpers/getNO2readings.py` on 25 Nov 2026 at 00:04 UTC / 01:04 Dutch time)
1. Fields in a single measurement record:
   > Five fields: `value` (19.26), `formula` (NO2), `timestamp_measured`, `timestamp_measured_start` and `timestamp_measured_end`. There is **no station ID** in the record — I only know the station from the URL I called. That's why my `ingest_air.py` adds `station_id = "NL10240"` itself before saving.
2. Time resolution:
   > **Hourly.** The timestamps are exactly one hour apart (18:00, 19:00, … 23:00 UTC), and start → end shows each value is the **average over one hour** (22:00–23:00). `timestamp_measured` is the **end** of that hour. So polling more than once an hour gives me nothing new.
3. What happens if I request a formula the station doesn't measure:
   > I asked for CO. The API did **not** give an error: it returned **HTTP 200 with an empty list** (`"data": []`). The course helper turns that into a `ValueError`. Lesson: "no data" can look like success, so my pipeline must check for empty or null data itself instead of trusting the status code.
4. How recent is the latest reading compared to when I ran it:
   > At 00:04 UTC the newest value was for the hour 22:00–23:00 UTC, which ended about **1 hour earlier**. The 23:00–00:00 hour had ended only 4 minutes before and wasn't published yet: RIVM first has to average and check the hour, and publishes it roughly **30 minutes after the hour ends**. So the newest reading is always between ~30 and ~90 minutes old — which is why my `/health` endpoint only calls Luchtmeetnet data stale after 3 hours.
   > *Extra observation:* NO₂ fell from 37 µg/m³ (18:00 UTC, evening rush) to 19 µg/m³ (23:00 UTC) as traffic got lighter — the kind of pattern my model should learn.

### Lab 1 — Follow task results
- `aws sts get-caller-identity` → IAM user `Rakshitha`, account ****-****-9231 (free plan, region eu-west-1). Root is only used for billing (MFA on); a $10/month budget alert is set.
- `docker run hello-world` → works. Client → daemon → pulled image from Docker Hub (amd64) → container ran. *Image = the recipe, container = the dish being cooked.*
- Resilience seen for real: when Docker Desktop started, my old October containers restarted by themselves (`restart: unless-stopped`). Their database was deleted, so they logged `db_write_failed` as structured JSON — but **did not crash**, and kept waiting for the next hour.

### Lab 1 — what my `ingest_air.py` does (read the code first, then explain)
Built and ran it as a container (`docker build -t airbreda-air -f docker/air.Dockerfile .` → `docker run --rm -e RUN_ONCE=1 airbreda-air`): `fetch_success`, value 19.26 at 23:00 UTC, 50 rows. `db_write_failed` (no database yet) and `queue_publish_failed` (no Redis yet) were logged as JSON errors — the service did **not** crash. `pytest tests/test_ingest_air.py` → 4 passed.

- `filter_no2_readings()`:
  > Keeps only rows where the component is NO2. Rows with a null value are **kept**, not dropped — tested in `tests/test_ingest_air.py`.
- Why fetch ~50 hours instead of only the newest value:
  > Self-healing: a failed run is filled in by the next one. And to detect a stuck sensor I need several consecutive hours. Re-fetching is safe because database writes are idempotent.
- What happens when the API is briefly unreachable:
  > Each request waits max 10 s. Timeouts, connection errors, 429 and 5xx are retried 3 times (backoff 1 s, 2 s); other 4xx errors stop immediately, because repeating a bad request won't fix it. If all attempts fail: a structured `fetch_failed` log, the service keeps running, and the next hour re-reads ~50 hours, so nothing is lost.
- The CAP comment at the top, in my own words:
  > The sensor network is AP: when a station loses connection, the API still answers, but with null or old values. "The API responded" ≠ "the data is correct". So a null is kept, flagged (`is_flagged = TRUE`), logged as `DATA_QUALITY_ERROR` and counted — never deleted or silently filled in. Filling gaps is a later, visible, reversible decision.

### Spotify reflection
*At what scale does a single Postgres instance become the wrong answer? What signals would I watch for in a dashboard?*

>

### Lab 2 — what I built
| Step | What I did | Why | What breaks without it |
|------|-----------|-----|------------------------|
| RDS | | | |
| Security group | | | |
| Table (`scripts/setup_db.py`) | | | |
| Air ingest ×2 | rows inserted: 1st run ___, 2nd run ___ | | |
| S3 bucket | | | |
| Traffic ingest | files in S3: ___ | | |

- Why both a database **and** a bucket (my own words), and what happens when I retrain in six months:
  >

### Reflection
*Which decision from today would I be least confident defending in a design review?*

>

### Day 1 wrap-up
1. *"Let's use DynamoDB — it scales infinitely." My three questions:*
   >
2. *5 years, 10 stations — total rows? Does it change my database choice?*
   >
3. *Most recent NO₂ value right now: ____ µg/m³ at ____. What would a null look like, and what does my pipeline do with it?*
   >

---

## Day 2 — Architecture patterns, resilience & observability

### Lab results (25 Nov 2026)
- `docker compose up --build -d` → `redis`, `air-ingest`, `traffic-ingest` running on network `airbreda-project_airbreda`. `redis-cli -h redis ping` from both services → **PONG** (service names work as hostnames on the named network).
- Queue: `LLEN readings` = **57** (50 NO₂ + 7 traffic), all with the same JSON shape (`station_id, timestamp, component, value, is_flagged`). `LRANGE` reads without removing.
- `/health`: air `{"bad_data_count": 0, "source": "Luchtmeetnet"}`, traffic `{"bad_data_count": 2, "source": "NDW"}` (the two -1 lanes at vwa).
- Database: **0 rows with -1**, NDW speed 6 rows (vwa had no valid speed), NO₂ 56 rows, 0 flagged (station healthy). `pytest tests/test_data_quality.py` → **7 passed** (null/stale flagged, -1 dropped, threshold, health fields).
- ⭐ Air run after a 6-hour pause: `inserted: 6, skipped: 44` → NO₂ filled its own gap; traffic for those hours is lost.
- ⚠️ Observation: `vwa` (exit slip road) had flow 0 and only -1 speeds at 02:00 **and** at the 08:00 rush hour. Watch it — possibly a broken sensor or closed road. My checks catch -1 but not "stuck at 0".

### Understanding check
1. *Database down, Redis up — what happens to a new reading?*
   > DB write fails → structured `db_write_failed`, service keeps running, the reading is still published to Redis (independent steps). NO₂ is re-inserted by the next run (50-hour window); traffic is already in S3 and can be reloaded.
2. *Why keep a bad NO₂ value (flagged) but drop NDW -1?*
   > A null/stale NO₂ value is a real but suspicious reading: dropping it would hide the problem, flagging keeps the series honest. -1 is not a measurement but a code for "no vehicle" — as a speed it would corrupt averages and the model. It stays in the S3 file.
3. *`inserted: 6, skipped: 44` — what does it say about the two sources?*
   > NO₂ heals itself (each call re-reads ~50 h; idempotent writes skip the overlap). NDW traffic is live-only, so missed hours are lost — hence the S3 copy and the need for 24/7 collection.

**In one sentence:** Day 1 made the workers work; Day 2 made them a system — connected, observable, and honest about bad data.

## Day 3 — DataLab
*(filled in on Day 3)*
