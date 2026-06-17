from pyspark.sql.functions import col

def write_to_postgres(df, epoch_id):
    url = "jdbc:postgresql://postgres:5432/tapdrink_telemetry"
    properties = {
        "user": "tapdrink",
        "password": "tapdrink_secret",
        "driver": "org.postgresql.Driver"
    }
    
    flat_df = df.select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("purchase_count"),
        col("revenue")
    )
    
    flat_df.write.jdbc(url=url, table="realtime_metrics", mode="append", properties=properties)
