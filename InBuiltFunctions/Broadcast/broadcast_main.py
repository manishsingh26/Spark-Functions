
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType


from pyspark.sql import functions as fun

spark = SparkSession.builder.master("local[*]").appName("parquet_demo").getOrCreate()

flight_schema = StructType([StructField("airline", StringType(), True), StructField("flight", StringType(), True),
                            StructField("source_city", StringType(), True), StructField("departure_time", StringType(), True),
                            StructField("stops", StringType(), True), StructField("arrival_time", StringType(), True),
                            StructField("destination_city", StringType(), True), StructField("class", StringType(), True),
                            StructField("duration", IntegerType(), True), StructField("days_left", IntegerType(), True),
                            StructField("price", IntegerType(), True)])
#
df = spark.read.format("csv").option("header", "true").schema(flight_schema).load("../../SampleData/flight_data.csv")
df.write.format("parquet").partitionBy("airline").mode("overwrite").save("../../SampleData/flight.parquet")

df1 = spark.read.format("parquet").parquet("../../SampleData/flight.parquet")
# df.show()
# df.printSchema()
#
flight_data_array = [("Indigo", "{'company_id': 1001, 'country': 'London'}", "[A, B, C]"),
                      ("Air_India", "{'company_id': 1002, 'country': 'India'}", "['D', 'E', 'F']"),
                      ("GO_FIRST", "{'company_id': 1003, 'country': 'USA'}", "['G', 'H', 'I']"),
                      ("Vistara", "{'company_id': 1004, 'country': 'Dubai'}", "['J', 'K', 'L']"),
                      ("AirAsia", "{'company_id': 1005, 'country': 'Germany'}", "['A', 'B', 'C']"),
                      ("SpiceJet", "{'company_id': 1006, 'country': 'Australia'}", "['A', 'B', 'C']")]
flight_array_columns = ["airline", "details", "activities"]
df2 = spark.createDataFrame(flight_data_array, flight_array_columns)
df2.show()
df2.printSchema()

json_schema = StructType([StructField("company_id", IntegerType(), True),
                          StructField("country", StringType(), True),
                          StructField("activities", ArrayType(StringType(), True), True)])
df2 = df2.withColumn("details_json", fun.from_json(fun.col("details"), json_schema))\
        .select("airline", "details_json.*")
df2.show()

df3 = df1.join(fun.broadcast(df2), on="airline", how="inner")
df3.show()
df3.printSchema()
