# traffic-ingest image (Day 1 + Day 2)
#   docker build -t airbreda-traffic -f docker/traffic.Dockerfile .   (run from the repo root)
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1

RUN apt-get update \
 && apt-get install -y --no-install-recommends redis-tools \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

RUN pip install --no-cache-dir pandas boto3 psycopg2-binary redis

# The course helper is reused, not rewritten
COPY src/helpers/ helpers/
COPY src/airbreda_common.py src/ingest_traffic.py ./

EXPOSE 8002
CMD ["python", "ingest_traffic.py"]
