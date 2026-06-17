#!/bin/bash

# Este script se ejecuta DENTRO del contenedor spark-master
# Descarga los conectores de Redis y PostgreSQL al vuelo.

/opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages com.redislabs:spark-redis_2.12:3.1.0,org.postgresql:postgresql:42.6.0 \
  --conf "spark.redis.host=redis" \
  --conf "spark.redis.port=6379" \
  /opt/spark-jobs/main.py
