from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Examples") \
    .getOrCreate()

rdd = spark.sparkContext.parallelize([("a", 1),("b", 1),("a", 1)])
for (kee, val) in rdd.countByKey().items():
    print(kee, val)