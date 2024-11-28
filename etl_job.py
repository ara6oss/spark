from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Spark ETL Job") \
    .getOrCreate()

# Step 1: Extract - Read data from local storage
# Specify the path to your dataset file
file_path = "file.csv"  # Replace with the actual path

# Determine the format of your file (e.g., CSV, JSON, Parquet)
# Example for CSV with headers
df = spark.read.option("header", True).option("inferSchema", True).csv(file_path)

def geocode_address(address):
    api_key = "a3f2a52d53a54191b907abd0a2b3aeb9"  # Replace with your OpenCage API key
    base_url = "https://api.opencagedata.com/geocode/v1/json"
    params = {
        "q": address,
        "key": api_key,
        "language": "en"
    }
    response = requests.get(base_url, params=params)
    data = response.json()
    if data['results']:
        return data['results'][0]['geometry']['lat'], data['results'][0]['geometry']['lng']
    return None, None

# Step 3: Check for invalid latitude and longitude and update
def get_lat_lng(row):
    if row['lat'] is None or row['lng'] is None:
        address = f"{row['city']}, {row['country']}"
        lat, lng = geocode_address(address)
        time.sleep(1)  # To avoid hitting API limits too quickly
        return lat, lng
    return row['lat'], row['lng']

# Apply geocoding to rows with invalid latitude/longitude
updated_data = df.rdd.map(lambda row: row.asDict())
updated_data_with_coords = updated_data.map(lambda row: {
    **row,
    'lat': get_lat_lng(row)[0],
    'lng': get_lat_lng(row)[1]
})

# Convert back to DataFrame
updated_df = spark.createDataFrame(updated_data_with_coords)

# Step 4: Write the transformed data to output
output_path = "/app/output"  # Update with your desired output path
updated_df.write.mode("overwrite").csv(output_path)

# Print schema and preview data
updated_df.printSchema()
updated_df.show()

# Stop Spark session
spark.stop()