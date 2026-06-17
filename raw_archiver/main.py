import logging
import time

from core import config
from core.connections import setup_redis, setup_minio, redis_client
from services.batcher import upload_batch

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("raw-archiver")

def main():
    logger.info("Starting Raw Archiver...")
    time.sleep(5)
    setup_redis()
    setup_minio()

    logger.info("Listening for events...")
    
    batch = []
    batch_start_time = time.time()
    
    while True:
        try:
            elapsed_ms = (time.time() - batch_start_time) * 1000
            remaining_timeout = max(1, config.BATCH_TIMEOUT_MS - int(elapsed_ms))
            remaining_count = config.BATCH_SIZE - len(batch)
            
            messages = redis_client.xreadgroup(
                groupname=config.CONSUMER_GROUP,
                consumername=config.CONSUMER_NAME,
                streams={config.STREAM_NAME: ">"},
                count=remaining_count,
                block=remaining_timeout
            )

            if messages and messages[0][1]:
                batch.extend(messages[0][1])

            time_is_up = ((time.time() - batch_start_time) * 1000) >= config.BATCH_TIMEOUT_MS
            size_is_met = len(batch) >= config.BATCH_SIZE

            if (time_is_up and len(batch) > 0) or size_is_met:
                upload_batch(batch)
                batch = []
                batch_start_time = time.time()

        except Exception as e:
            logger.error(f"Error in processing loop: {e}")
            time.sleep(2)

if __name__ == "__main__":
    main()
