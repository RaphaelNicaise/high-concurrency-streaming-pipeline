import logging
from redis import Redis
from minio import Minio
from core import config

logger = logging.getLogger("raw-archiver")

redis_client = Redis(host=config.REDIS_HOST, port=config.REDIS_PORT, decode_responses=True)
minio_client = Minio(
    config.MINIO_ENDPOINT,
    access_key=config.MINIO_ACCESS_KEY,
    secret_key=config.MINIO_SECRET_KEY,
    secure=False
)

def setup_redis():
    try:
        redis_client.ping()
        try:
            redis_client.xgroup_create(config.STREAM_NAME, config.CONSUMER_GROUP, id="0", mkstream=True)
            logger.info(f"Created Consumer Group '{config.CONSUMER_GROUP}' on stream '{config.STREAM_NAME}'")
        except Exception as e:
            if "BUSYGROUP Consumer Group name already exists" in str(e):
                logger.info(f"Consumer Group '{config.CONSUMER_GROUP}' already exists.")
            else:
                raise e
    except Exception as e:
        logger.error(f"Redis setup failed: {e}")
        raise e

def setup_minio():
    try:
        if not minio_client.bucket_exists(config.MINIO_BUCKET):
            minio_client.make_bucket(config.MINIO_BUCKET)
            logger.info(f"Created MinIO bucket: '{config.MINIO_BUCKET}'")
        else:
            logger.info(f"MinIO bucket '{config.MINIO_BUCKET}' already exists.")
    except Exception as e:
        logger.error(f"MinIO setup failed: {e}")
        raise e
