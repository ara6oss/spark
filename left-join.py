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

# Add geohash column to the weather DataFrame
# Assuming weather_df has 'lat' and 'lng' columns for latitude and longitude
weather_df = weather_df.withColumn("geohash", geohash_udf(col("lat").cast("double"), col("lng").cast("double")))

# Add geohash column to the restaurant DataFrame
# Corrected column names to 'lat' and 'lng' in restaurant_df
restaurant_df = restaurant_df.withColumn("geohash", geohash_udf(col("lat").cast("double"), col("lng").cast("double")))

# Perform a left join on the geohash column
joined_df = restaurant_df.join(weather_df, on="geohash", how="left")

# Remove duplicates to prevent data multiplication
joined_df = joined_df.dropDuplicates()

# Show result
joined_df.show()

# Optionally, save the result to a new CSV file
# joined_df.write.option("header", "true").csv("joined_data.csv")
