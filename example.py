from pyspark.sql import SparkSession
import sys

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Examples") \
    .getOrCreate()

sc = spark.sparkContext
numbRDD = sc.parallelize([1, 2, 3, 4])

# Create map() transformation to cube numbers
cubedRDD = numbRDD.map(lambda x: x ** 3)

# Collect the results
numbers_all = cubedRDD.collect()

# Print the numbers from numbers_all
for numb in numbers_all:
	print(numb)

# CJ additions:
# List comprehension
[print(numb) for numb in numbers_all]

# Using map function
list(map(print, numbers_all))

# Don't use print for millions of times
sys.stdout.write('\n'.join(map(str, numbers_all)) + '\n')

#map(str, numbers_all): converts all numbers to strings without creating a list immediately.
#'\n'.join(...): joins them into one big string.
#Then only one write happens.

# This is way faster because you're not making a system call (write) for every number.

rdd = sc.parallelize([("a", 1),("b", 1),("a", 1)])
for (kee, val) in rdd.countByKey().items():
    print(kee, val)