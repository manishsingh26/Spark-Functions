
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType
from pyspark.sql import functions as fun

spark = SparkSession.builder.master("local[*]").appName("parquet_demo").getOrCreate()

df1 = spark.read.format("parquet").parquet("/home/manish/PycharmProjects/Spark-Functions/SampleData/flight.parquet")
# df.show()
# df.printSchema()
#
flight_data_array = [("Indigo", "{'company_id': 1001, 'country': 'London'}"),
                      ("Air_India", "{'company_id': 1002, 'country': 'India'}"),
                      ("GO_FIRST", "{'company_id': 1003, 'country': 'USA'}"),
                      ("SpiceJet", "{'company_id': 1006, 'country': 'Australia'}")]
flight_array_columns = ["airline", "details", "activities"]
df2 = spark.createDataFrame(flight_data_array, flight_array_columns)

json_schema = StructType([StructField("company_id", IntegerType(), True),
                          StructField("country", StringType(), True)])
df2 = df2.withColumn("details_json", fun.from_json(fun.col("details"), json_schema))\
        .select("airline", "details_json.*")

'''
Inner Join: Returns only the rows with matching keys in both DataFrames.
Left Join: Returns all rows from the left DataFrame and matching rows from the right DataFrame.
Right Join: Returns all rows from the right DataFrame and matching rows from the left DataFrame.
Full Outer Join: Returns all rows from both DataFrames, including matching and non-matching rows.
Left Semi Join: Returns all rows from the left DataFrame where there is a match in the right DataFrame.
Left Anti Join: Returns all rows from the left DataFrame where there is no match in the right DataFrame.
'''

# Inner Join
print("Inner Join")
df3 = df1.join(fun.broadcast(df2), on="airline", how="inner")
df3.show(truncate=False)

# Outer Join
print("Outer Join")
df3 = df1.join(fun.broadcast(df2), on="airline", how="outer")
df3.show(truncate=False)

# Full Join
print("Full Join")
df3 = df1.join(fun.broadcast(df2), on="airline", how="full")
df3.show(truncate=False)

# Full Outer Join
print("Full Outer Join")
df3 = df1.join(fun.broadcast(df2), on="airline", how="fullouter")
df3.show(truncate=False)

# Left Join
print("Left Join")
df3 = df1.join(fun.broadcast(df2), on="airline", how="left")
df3.show(truncate=False)

# Left Outer Join
print("Left Outer Join")
df3 = df1.join(fun.broadcast(df2), on="airline", how="leftouter")
df3.show(truncate=False)

# Right Join
print("Right Join")
df3 = df1.join(fun.broadcast(df2), on="airline", how="right")
df3.show(truncate=False)

# Right Outer Join
print("Right Outer Join")
df3 = df1.join(fun.broadcast(df2), on="airline", how="rightouter")
df3.show(truncate=False)

# Left Semi
print("Right Outer Join")
df3 = df1.join(fun.broadcast(df2), on="airline", how="leftsemi")
df3.show(truncate=False)

# Left Anti Join
print("Left Anti Join")
df3 = df1.join(fun.broadcast(df2), on="airline", how="leftanti")
df3.show(truncate=False)
