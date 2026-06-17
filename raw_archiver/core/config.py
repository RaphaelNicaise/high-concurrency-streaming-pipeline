import os
import uuid

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
STREAM_NAME = os.getenv("REDIS_STREAM", "telemetry:events")
CONSUMER_GROUP = "archiver_group"
CONSUMER_NAME = f"archiver-{uuid.uuid4().hex[:6]}"
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "50000"))
BATCH_TIMEOUT_MS = int(os.getenv("BATCH_TIMEOUT_MS", "15000"))

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
if MINIO_ENDPOINT.startswith("http://"):
    MINIO_ENDPOINT = MINIO_ENDPOINT[7:]

MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "tapdrink")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "tapdrink_minio_secret")
MINIO_BUCKET = os.getenv("MINIO_BUCKET_BRONZE", "bronze")
