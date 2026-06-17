from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, sum, count

from schemas import redis_schema, telemetry_schema
from sinks import write_to_postgres

def main():
    spark = SparkSession.builder \
        .appName("TapDrink_RealTime_Analytics") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    stream_df = spark.readStream \
        .format("redis") \
        .option("stream.keys", "telemetry:events") \
        .option("stream.read.batch.size", "5000") \
        .schema(redis_schema) \
        .load()

    parsed_df = stream_df.withColumn("parsed_data", from_json(col("data"), telemetry_schema))
    expanded_df = parsed_df.select("parsed_data.*")

    good_data_df = expanded_df.filter(col("_corrupt_record").isNull())
    bad_data_df = expanded_df.filter(col("_corrupt_record").isNotNull())

    metrics_df = good_data_df \
        .filter(col("event_type") == "purchase_completed") \
        .withWatermark("timestamp", "10 seconds") \
        .groupBy(window(col("timestamp"), "5 seconds")) \
        .agg(
            count("*").alias("purchase_count"),
            sum("price").alias("revenue")
        )

    pg_query = metrics_df.writeStream \
        .outputMode("append") \
        .foreachBatch(write_to_postgres) \
        .start()

    console_query = metrics_df.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "false") \
        .start()

    dlq_query = bad_data_df.select("event_id", "_corrupt_record").writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "false") \
        .start()

    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()
