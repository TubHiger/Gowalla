import pandas as pd
import pyspark
import math
import numpy as np
from google.colab import drive

drive.mount('/content/drive')

f = pd.read_csv("/content/drive/MyDrive/Colab Notebooks/gowalla.csv", sep='\t', header=None, names=['user', 'checkin_time', 'latitude', 'longitude', 'location_id'])

print(f.head(10))

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct, radians, min, max, sqrt, pow
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType

from pyspark.sql.functions import row_number
from pyspark.sql.window import Window

# Create a SparkSession.
spark = SparkSession.builder.getOrCreate()

# Define the schema for the dataset.
schema = StructType([
    StructField("user", IntegerType(), True),
    StructField("checkin_time", IntegerType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("location_id", IntegerType(), True)])

# Read the dataset into a Spark DataFrame.
df = spark.read.csv("/content/drive/MyDrive/Colab Notebooks/gowalla.csv", sep='\t', header=False, schema=schema)

# Calculate the intermediate values.
distinct_location_count = df.groupBy("user").agg(countDistinct("location_id").alias("distinct_location_count"))
distinct_user_count = df.groupBy("location_id").agg(countDistinct("user").alias("distinct_user_count"))

# Join the intermediate values to calculate the trendsetter score.
joined_df = df.join(distinct_location_count, "user", "left").join(distinct_user_count, "location_id", "left")

# Calculate the trendsetter score for each user.
trendsetter_scores = joined_df.withColumn("trendsetter_score", col("distinct_location_count") * col("distinct_user_count"))

# Calculate the radius of influence for each user (assuming a flat Earth).
def euclidean_distance(lat1, lon1, lat2, lon2):
    # Calculate the Euclidean distance between two points.
    return sqrt(pow(lat2 - lat1, 2) + pow(lon2 - lon1, 2))

# Calculate the minimum and maximum latitude and longitude for each user.
user_extremes = df.groupBy("user").agg(
    radians(min("latitude")).alias("min_lat_rad"),
    radians(max("latitude")).alias("max_lat_rad"),
    radians(min("longitude")).alias("min_lon_rad"),
    radians(max("longitude")).alias("max_lon_rad")
)

# Calculate the radius of influence for each user.
user_extremes = user_extremes.withColumn("radius_of_influence", euclidean_distance(
    col("min_lat_rad"), col("min_lon_rad"),
    col("max_lat_rad"), col("max_lon_rad")
) * 6371.0)  # Approximate Earth's radius in kilometers

# Select only the desired columns for the final result.
final_result_score = trendsetter_scores.select("user", "trendsetter_score")
final_result_radius = user_extremes.select("user", "radius_of_influence")

# Sort the users by trendsetter score in descending order and show the top 10.
top_10_trendsetters_by_score = final_result_score.orderBy("trendsetter_score", ascending=False).limit(10)

# Calculate the distinct users and their trendsetter scores.
distinct_users_scores = final_result_score.groupBy("user").agg({"trendsetter_score": "max"})

# Use the row_number() function to assign row numbers to each user based on their trendsetter score.
window_spec = Window.orderBy(col("max(trendsetter_score)").desc())
distinct_users_scores = distinct_users_scores.withColumn("row_number", row_number().over(window_spec))

# Select the top 10 unique users with their trendsetter scores.
top_10_unique_trendsetters = distinct_users_scores.filter(col("row_number") <= 10).select("user", "max(trendsetter_score)")
top_10_unique_trendsetters = top_10_unique_trendsetters.withColumnRenamed("max(trendsetter_score)", "trendsetter_score")

#Add the "radius_of_influence" column to the "Top 10 Unique Trendsetters by Score" table.
top_10_unique_trendsetters = top_10_unique_trendsetters.join(final_result_radius, "user", "inner")
top_10_unique_trendsetters = top_10_unique_trendsetters.orderBy("trendsetter_score", ascending=False)

# Display the table of top 10 unique trendsetters by score with "radius_of_influence."
print("Top 10 Unique Trendsetters by Score (user, trendsetter_score, radius_of_influence):")
top_10_unique_trendsetters.show()

# Sort the users by radius of influence in descending order and show the top 10.
top_10_trendsetters_by_radius = final_result_radius.orderBy("radius_of_influence", ascending=False).limit(10)

#Display the table of top 10 trendsetters by radius of influence.
print("Top 10 Trendsetters by Radius of Influence (user, radius_of_influence):")
top_10_trendsetters_by_radius.show()
