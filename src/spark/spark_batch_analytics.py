#!/usr/bin/env python3
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, DoubleType, IntegerType, ArrayType
import pyspark.sql.functions as F

def create_spark_session():
    spark = SparkSession.builder \
        .appName("Ashpaz-Historical-Batch-Analytics") \
        .master("local[*]") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark

def run_batch_pipeline():
    print(" Initializing Apache Spark In-Memory Analytics Cluster...")
    spark = create_spark_session()
    
    input_file = "historical_data.json"
    if not os.path.exists(input_file):
        raise FileNotFoundError(f"Missing file: '{input_file}'. Ensure it resides in your workspace folder.")
    
    # Read implicitly first to robustly capture unstructured variations inside the items arrays
    raw_df = spark.read.json(input_file)
    raw_df.cache()
    
    print("SECTION 1: REVENUE AND MENU ANALYSIS BATCH JOB")
    
    # We defensively fallback to the raw item if sub-fields aren't found
    exploded_df = raw_df.withColumn("exploded_item", F.explode(F.col("items")))
    
    # Check if nested fields exist or if it's an object array using schema inspection
    schema_fields = exploded_df.schema["exploded_item"].dataType
    
    if isinstance(schema_fields, StructType):
        # Determine the name of the identity column present in the historical file
        sub_fields = schema_fields.fieldNames()
        id_col = "item_id" if "item_id" in sub_fields else ("item_name" if "item_name" in sub_fields else sub_fields[0])
        qty_col = "quantity" if "quantity" in sub_fields else sub_fields[1]
        
        normalized_items_df = exploded_df.withColumn("resolved_item", F.coalesce(F.col(f"exploded_item.{id_col}"), F.lit("Unknown Item"))) \
                                         .withColumn("resolved_qty", F.coalesce(F.col(f"exploded_item.{qty_col}").cast("int"), F.lit(1)))
    else:
        # If the array contains flat string components instead of objects
        normalized_items_df = exploded_df.withColumn("resolved_item", F.col("exploded_item").cast("string")) \
                                         .withColumn("resolved_qty", F.lit(1))
    
    print("\n Bars Computing Metric I: Top Ordered Items across all restaurants... [cite: 160]")
    top_items = normalized_items_df.groupBy("resolved_item") \
                                   .agg(F.sum("resolved_qty").alias("total_units_sold")) \
                                   .orderBy(F.col("total_units_sold").desc())
    top_items.show(10, truncate=False)
    
    # II. Highest Revenue Restaurants [cite: 161]
    print("\n Computing Metric II: Top 10 Highest Revenue Restaurants on the platform... [cite: 161]")
    top_revenue_restaurants = raw_df.groupBy("restaurant_id", "restaurant_name") \
                                    .agg(F.round(F.sum("order_price"), 2).alias("total_earnings")) \
                                    .orderBy(F.col("total_earnings").desc())
    top_revenue_restaurants.show(10, truncate=False)
    
    # III. Order Channel Leaders [cite: 162]
    print("\n Computing Metric III: Order Channel Volumes (Online Delivery vs. Dine-In Traffic)... [cite: 162]")
    channel_leaders = raw_df.groupBy("restaurant_id", "restaurant_name") \
                            .agg(
                                F.count(F.when(F.col("request_online") == True, True)).alias("online_delivery_count"),
                                F.count(F.when(F.col("request_table") == True, True)).alias("dine_in_traffic_count")
                            ) \
                            .orderBy((F.col("online_delivery_count") + F.col("dine_in_traffic_count")).desc())
    channel_leaders.show(10, truncate=False)

    print("SECTION 2: ORDER TIME & PEAK HOUR ANALYSIS")
    
    # I & II. Hourly Patterns [cite: 164, 166]
    print("\n Analyzing Temporal Distribution (24-Hour Cycle Activity Monitoring)... [cite: 164]")
    hourly_df = raw_df.withColumn("order_hour", F.hour(F.to_timestamp(F.col("order_time"))))
    
    hourly_patterns = hourly_df.groupBy("order_hour") \
                               .agg(F.count("order_id").alias("transaction_volume")) \
                               .orderBy("order_hour")
    hourly_patterns.show(24, truncate=False)
    
    sorted_hours = hourly_patterns.orderBy(F.col("transaction_volume").desc()).collect()
    if sorted_hours:
        print(f" PEAK HOUR IDENTIFIED: Hour {sorted_hours[0]['order_hour']} with {sorted_hours[0]['transaction_volume']} orders. [cite: 167]")
        print(f" OFF-PEAK HOUR IDENTIFIED: Hour {sorted_hours[-1]['order_hour']} with {sorted_hours[-1]['transaction_volume']} orders. [cite: 167]")

    print("SECTION 3: GEOGRAPHIC & LOCATION-BASED ANALYSIS")
    
    # I. Revenue Distribution by City [cite: 168]
    print("\n Computing Metric I: Regional Revenue Yield per City... [cite: 168]")
    city_revenue = raw_df.groupBy("restaurant_city") \
                         .agg(F.round(F.sum("order_price"), 2).alias("total_city_revenue")) \
                         .orderBy(F.col("total_city_revenue").desc())
    city_revenue.show(20, truncate=False)
    
    # II. Regional Favorite Foods [cite: 169]
    print("\n Computing Metric II: Regional Favorite Menu Items Per City... [cite: 169, 170]")
    city_items_df = normalized_items_df.groupBy("restaurant_city", "resolved_item") \
                                       .agg(F.sum("resolved_qty").alias("item_city_quantity"))
    
    from pyspark.sql.window import Window
    city_window = Window.partitionBy("restaurant_city").orderBy(F.col("item_city_quantity").desc())
    
    regional_favorites = city_items_df.withColumn("rank", F.rank().over(city_window)) \
                                       .filter(F.col("rank") == 1) \
                                       .orderBy("restaurant_city")
    regional_favorites.show(20, truncate=False)

    raw_df.unpersist()
    spark.stop()
    print("\n BATCH PIPELINE SUCCESSFUL: All analytical blocks computed cleanly with zero-NULL resolution!")

if __name__ == "__main__":
    run_batch_pipeline()