from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType
import geohash2  # Import geohash2 library

# Initialize Spark session
spark = SparkSession.builder.appName("Geohash from CSV").getOrCreate()

# Path to your CSV file
csv_file_path = 'file.csv'

# Load the CSV data into a PySpark DataFrame
# Assuming the CSV file has headers and the columns for lat/lng are named 'lat' and 'lng'
df = spark.read.option("header", "true").csv(csv_file_path)

# Show the first few rows to verify the data
df.show()

# UDF to generate geohash from latitude and longitude
@udf(returnType=StringType())
def generate_geohash(lat, lng):
    if lat is not None and lng is not None:
        # Generate geohash with precision of 4 characters
        return geohash2.encode(float(lat), float(lng), precision=4)  # Ensure lat/lng are floats
    return None

# Add the geohash column
df_with_geohash = df.withColumn("geohash", generate_geohash(col("lat"), col("lng")))

# Show the DataFrame with the geohash column
df_with_geohash.show()

# Optionally, save the DataFrame with the new geohash column to a new CSV file
df_with_geohash.write.option("header", "true").csv("/path/to/save/your/output.csv")
