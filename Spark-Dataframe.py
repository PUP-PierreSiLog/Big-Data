from pyspark.sql import SparkSession
#Imports the Spark Session for Reading
spark = SparkSession.builder \
    .appName("CSV Reader") \
    .getOrCreate()


df_csv = spark.read.csv("data.csv", header=True, inferSchema=True)
#Test printing table
# df_id_course=df_csv.show(4)
# print(df_id_course)

df_csv_by_type = df_csv.groupby('COURSE TYPE')
df_csv_by_type.count().show(4)

df_csv_filtered_fees=df_csv.filter(df_csv["FEES"] > 5500000)
df_csv_filtered_fees.show(14)

df_csv_renamed = df_csv.withColumnRenamed("FEES", "TUITION")
df_csv_renamed.show(7)