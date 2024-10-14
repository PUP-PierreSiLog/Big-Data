from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("CSV Reader") \
    .getOrCreate()

df_csv = spark.read.csv("data.csv", header=True, inferSchema=True)
print(type(df_csv))