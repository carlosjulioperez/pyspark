from pyspark.sql import SparkSession
from pyspark.sql.functions import concat_ws, col

# Initialize Spark session
spark = SparkSession.builder \
 .appName("CSV Loader") \
 .getOrCreate()

df = spark.read.csv('cars.csv', header=True, sep=";")
# df.show(5)
# Add new transformation column by concatenating Car, Model, and Origin
df = df.withColumn("transformation", 
                  concat_ws(" - ", col("Car"), col("Model"), col("Origin")))

# Show the first 5 rows with the new column
df.show(5, truncate=False)