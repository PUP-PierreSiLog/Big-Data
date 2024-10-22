from pyspark.sql import SparkSession
#Imports the Spark Session for Reading
spark = SparkSession.builder \
    .appName("CSV Reader") \
    .getOrCreate()

df= spark.read.csv("data.csv", header=True, inferSchema=True)

#Grouping by Course type
df_csv_by_type = df.groupby('COURSE TYPE')
df_csv_by_type.count().show(4)

#Filtering fees
df_csv_filtered_fees=df.filter(df["FEES"] > 5500000)
df_csv_filtered_fees.show(14)

#Renaming 2 columns
df_renamed= df.withColumnRenamed("FEES", "TUITION")
df_renamed_2= df_renamed.withColumnRenamed("COURSE TYPE", "FIELD")
df_renamed_2.show(7)

#Importing for SQL query
df_renamed_2.createOrReplaceTempView("PROGRAMS")

#Question 1: How much is the most expensive course in each country?
query_1 = "SELECT COUNTRY, max(TUITION) FROM PROGRAMS GROUP BY COUNTRY"
test_df=spark.sql(query_1)
test_df.show()

#Question 2: What is the most expensive field?
query_2 = "SELECT FIELD, max(TUITION) FROM PROGRAMS GROUP BY FIELD"
test_df_2=spark.sql(query_2)
test_df_2.show()

#Question 3: What is the cheapest country to perform an MBA with?
query_3 = "SELECT COUNTRY, min(TUITION) as Min_Tuition FROM PROGRAMS WHERE FIELD = 'MBA' GROUP BY COUNTRY ORDER BY Min_Tuition"
test_df_3=spark.sql(query_3)
test_df_3.show()

#Stop the spark session
spark.stop()