from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import geohash2

# Initialize Spark session
spark = SparkSession.builder.appName("WeatherRestaurantJoin").getOrCreate()

# Load the weather data from Snappy Parquet format
weather_df = spark.read.parquet("weather.parquet")

# Load restaurant data from CSV (assuming CSV format)
restaurant_df = spark.read.option("header", "true").csv("file.csv")

# Function to generate a 4-character geohash from latitude and longitude
def generate_geohash(lat, lon):
    return geohash2.encode(lat, lon)[:4]  # Geohash truncated to 4 characters

# UDF to apply geohashing to each row in the DataFrame
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

geohash_udf = udf(lambda lat, lon: generate_geohash(lat, lon), StringType())

# Rename lat/lng columns if necessary to avoid conflict
restaurant_df = restaurant_df.withColumnRenamed("lat", "restaurant_lat").withColumnRenamed("lng", "restaurant_lng")
weather_df = weather_df.withColumnRenamed("lat", "weather_lat").withColumnRenamed("lng", "weather_lng")

# Add geohash column to the weather DataFrame
weather_df = weather_df.withColumn("geohash", geohash_udf(col("weather_lat").cast("double"), col("weather_lng").cast("double")))

# Add geohash column to the restaurant DataFrame
restaurant_df = restaurant_df.withColumn("geohash", geohash_udf(col("restaurant_lat").cast("double"), col("restaurant_lng").cast("double")))

# Perform a left join on the geohash column, which may result in null values in the weather columns
joined_df = restaurant_df.join(weather_df, on="geohash", how="left")

# Remove duplicates to prevent data multiplication
joined_df = joined_df.dropDuplicates()

# Store the enriched data in Parquet format, partitioned by 'geohash'
output_path = "/output/folder"  # Change this to your desired local output folder

# Save the data in Parquet format, partitioned by 'geohash' and handling null values
joined_df.write.mode("overwrite").partitionBy("geohash").parquet(output_path)

# Optionally, you can also check if the data is stored properly by reading back
stored_df = spark.read.parquet(output_path)
stored_df.show()
