# Image for the Kafka -> Iceberg consumer (also runs the Postgres load generator and smoke check).
FROM python:3.12-slim-bookworm
WORKDIR /app
COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install --no-cache-dir ".[stream]"
CMD ["cdc-lake", "consume"]
