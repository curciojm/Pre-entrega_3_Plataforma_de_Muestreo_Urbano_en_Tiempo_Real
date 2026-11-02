
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    to_timestamp,
    window,
    avg
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType
)


KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"
)

KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "urban_sensors")

CHECKPOINT_LOCATION = os.getenv(
    "SPARK_CHECKPOINT_LOCATION", "/tmp/spark-checkpoint"
)


# Esquema de los eventos JSON
schema = StructType([
    StructField("sensor_id", StringType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("humidity", DoubleType(), True),
    StructField("air_quality_index", IntegerType(), True),
    StructField("timestamp", StringType(), True)
])


spark = (
    SparkSession.builder
    .appName("UrbanSensorStreaming")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


raw_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "latest")
    .load()
)


events = (
    raw_stream
    .select(
        from_json(
            col("value").cast("string"),
            schema
        ).alias("data")
    )
    .select("data.*")
    .withColumn("event_time", to_timestamp(col("timestamp")))
)


valid_events = events.filter(
    col("sensor_id").isNotNull()
    & col("event_time").isNotNull()
)


aggregated = (
    valid_events
    .withWatermark("event_time", "30 seconds")
    .groupBy(
        window(col("event_time"), "1 minute"),
        col("sensor_id")
    )
    .agg(
        avg("temperature").alias("avg_temperature"),
        avg("air_quality_index").alias("avg_air_quality_index")
    )
)


query = (
    aggregated.writeStream
    .outputMode("append")
    .format("console")
    .option("truncate", "false")
    .option("checkpointLocation", CHECKPOINT_LOCATION)
    .start()
)

query.awaitTermination()
