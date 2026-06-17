from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, TimestampType

redis_schema = StructType([
    StructField("_id", StringType(), True),
    StructField("data", StringType(), True)
])

telemetry_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("user_id", LongType(), True),
    StructField("event_type", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("_corrupt_record", StringType(), True)
])
