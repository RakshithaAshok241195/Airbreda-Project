# air-ingest image (Day 1 + Day 2)
#   docker build -t airbreda-air -f docker/air.Dockerfile .   (run from the repo root)
FROM python:3.12-slim

# Print logs immediately (no buffering), so `docker logs` shows them in real time.
ENV PYTHONUNBUFFERED=1

# redis-tools gives us redis-cli inside the container, for the course's
# `docker exec air-ingest redis-cli -h redis ping` connectivity check.
RUN apt-get update \
 && apt-get install -y --no-install-recommends redis-tools \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Dependencies first: this layer is cached and only rebuilt when the list changes.
RUN pip install --no-cache-dir requests pandas psycopg2-binary redis

COPY src/airbreda_common.py src/ingest_air.py ./

EXPOSE 8001
CMD ["python", "ingest_air.py"]
