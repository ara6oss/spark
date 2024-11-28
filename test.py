from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import FloatType, StructType, StructField, StringType
import requests

# Инициализация SparkSession
spark = SparkSession.builder.appName("Spark API Test").getOrCreate()

# Функция для получения координат через OpenCage API
def geocode_address(address):
    api_key = "a3f2a52d53a54191b907abd0a2b3aeb9"  # Замени на свой ключ
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

# Пример данных с пропущенными координатами
data = [("Milan", "Italy", None, None), ("Paris", "France", 48.8566, 2.3522)]
columns = ["city", "country", "lat", "lng"]

# Явно указываем схему
schema = StructType([
    StructField("city", StringType(), True),
    StructField("country", StringType(), True),
    StructField("lat", FloatType(), True),
    StructField("lng", FloatType(), True)
])

# Создаем DataFrame
df = spark.createDataFrame(data, schema)

# УDF (User Defined Function) для получения широты
@udf(returnType=FloatType())
def get_lat(city, country, lat):
    if lat is None:  # Если широта отсутствует
        address = f"{city}, {country}"
        lat, _ = geocode_address(address)
        return lat
    return lat

# УDF для получения долготы
@udf(returnType=FloatType())
def get_lng(city, country, lng):
    if lng is None:  # Если долгота отсутствует
        address = f"{city}, {country}"
        _, lng = geocode_address(address)
        return lng
    return lng

# Применяем UDF для обновления lat и lng
df_updated = df.withColumn("lat", get_lat(col("city"), col("country"), col("lat"))) \
               .withColumn("lng", get_lng(col("city"), col("country"), col("lng")))

# Показываем результат
df_updated.show()
