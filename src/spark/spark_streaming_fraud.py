#!/usr/bin/env python3
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, DoubleType, IntegerType, ArrayType
import pyspark.sql.functions as F

def create_streaming_session():
    """Initializes Spark Session optimized with native Kafka streaming connectors for Scala 2.13."""
    spark = SparkSession.builder \
    .appName("RealTimeFraudDetection") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.2") \
    .config("spark.sql.streaming.metricsEnabled", "false") \
    .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark

def build_incoming_schema():
    """Builds explicit structural maps based on the discovered 'food_item' payload signature."""
    item_schema = StructType([
        StructField("food_item", StringType(), True),
        StructField("category", StringType(), True),
        StructField("unit_price", DoubleType(), True),
        StructField("quantity", IntegerType(), True)
    ])
    
    return StructType([
        StructField("order_id", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("restaurant_id", IntegerType(), True),
        StructField("restaurant_name", StringType(), True),
        StructField("restaurant_city", StringType(), True),
        StructField("cuisines", ArrayType(StringType()), True),
        StructField("phone_number", StringType(), True),
        StructField("request_online", BooleanType(), True),
        StructField("request_table", BooleanType(), True),
        StructField("order_time", StringType(), True),
        StructField("items", ArrayType(item_schema), True),
        StructField("order_price", DoubleType(), True)
    ])

def process_stream_pipeline():
    print("📡 Instantiating Real-Time Spark Streaming Framework Engine...")
    spark = create_streaming_session()
    
    # Subscribe directly to the validated Kafka ingestion topic feed
    kafka_raw_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "ashpaz.valid") \
        .option("startingOffsets", "latest") \
        .load()
    
    # Cast incoming binary payloads to string and parse out structures
    json_schema = build_incoming_schema()
    parsed_stream = kafka_raw_stream \
        .withColumn("string_value", F.col("value").cast("string")) \
        .withColumn("data", F.from_json(F.col("string_value"), json_schema)) \
        .select("data.*") \
        .withColumn("timestamp", F.to_timestamp(F.col("order_time")))

    # Apply structural watermarking to manage in-memory window constraints safely
    watermarked_stream = parsed_stream.withWatermark("timestamp", "1 minute")

    # RULE 1 DETECTOR: GEOGRAPHICAL IMPOSSIBILITY
    
    # Stream-compliant workaround: Collect list first, then perform array_distinct size check
    geo_window = watermarked_stream.groupBy(
        F.window(F.col("timestamp"), "6 seconds", "2 seconds"),
        F.col("user_id")
    ).agg(
        F.collect_list("restaurant_city").alias("cities_visited"),
        F.collect_list("order_id").alias("associated_orders")
    ).withColumn("unique_city_count", F.size(F.array_distinct(F.col("cities_visited")))) \
     .filter(F.col("unique_city_count") > 1)

    # RULE 2 DETECTOR: VELOCITY CHECK SPAM
    
    velocity_window = watermarked_stream.groupBy(
        F.window(F.col("timestamp"), "12 seconds", "4 seconds"),
        F.col("user_id")
    ).agg(
        F.count("order_id").alias("total_order_frequency"),
        F.collect_list("order_id").alias("associated_orders")
    ).filter(F.col("total_order_frequency") > 5)

    # CONVERT ALERTS INTO COMPLIANT KAFKA STRING MESSAGES
    geo_alerts = geo_window.select(
        F.col("user_id"),
        F.lit("GEOGRAPHICAL_IMPOSSIBILITY").alias("alert_type"),
        F.concat(F.lit("User placed orders across cities: "), F.col("cities_visited").cast("string")).alias("trigger_reason")
    ).selectExpr("CAST(user_id AS STRING) AS key", "to_json(struct(*)) AS value")

    velocity_alerts = velocity_window.select(
        F.col("user_id"),
        F.lit("VELOCITY_SPAM_BOT").alias("alert_type"),
        F.concat(F.lit("User exceeded checkout rate with frequency count: "), F.col("total_order_frequency").cast("string")).alias("trigger_reason")
    ).selectExpr("CAST(user_id AS STRING) AS key", "to_json(struct(*)) AS value")

    # Combine alert streams
    combined_alerts = geo_alerts.union(velocity_alerts)

    # EXECUTE STREAM PRODUCTION
    print("Live Threat Isolation Rules Activated. Running Analytics queries...")
    
    checkpoint_path = "./spark_checkpoints/fraud_detection"
    
    # Primary production output loop to Kafka
    query = combined_alerts.writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", "orders.fraud_alerts") \
        .option("checkpointLocation", checkpoint_path) \
        .outputMode("update") \
        .start()

    # Visual verify sink to standard output console
    console_query = combined_alerts.writeStream \
        .format("console") \
        .outputMode("update") \
        .start()

    query.awaitTermination()
    console_query.awaitTermination()

if __name__ == "__main__":
    process_stream_pipeline()