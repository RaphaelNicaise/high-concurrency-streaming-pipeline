import os
import tempfile
import uuid
import logging
from datetime import datetime, timezone
from minio.error import S3Error

from core import config
from core.connections import minio_client, redis_client

logger = logging.getLogger("raw-archiver")

def upload_batch(stream_data):
    logger.info(f"Uploading batch of {len(stream_data)} events to MinIO.")
    
    now = datetime.now(timezone.utc)
    batch_id = uuid.uuid4().hex[:8]
    partition_path = f"telemetry/year={now.year}/month={now.month:02d}/day={now.day:02d}/hour={now.hour:02d}"
    filename = f"{batch_id}.jsonl"
    object_name = f"{partition_path}/{filename}"

    msg_ids = []
    
    with tempfile.NamedTemporaryFile(mode='w', delete=False, encoding='utf-8') as tmp_file:
        for msg_id, payload in stream_data:
            msg_ids.append(msg_id)
            json_str = payload.get("data", "{}")
            tmp_file.write(json_str + "\n")
        tmp_path = tmp_file.name

    try:
        minio_client.fput_object(
            bucket_name=config.MINIO_BUCKET,
            object_name=object_name,
            file_path=tmp_path,
            content_type="application/jsonlines"
        )
        logger.info(f"Uploaded {filename} to s3://{config.MINIO_BUCKET}/{object_name}")

        if msg_ids:
            redis_client.xack(config.STREAM_NAME, config.CONSUMER_GROUP, *msg_ids)
            logger.debug(f"Acknowledged {len(msg_ids)} messages in Redis.")

    except S3Error as e:
        logger.error(f"Failed to upload to MinIO: {e}")
    finally:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
