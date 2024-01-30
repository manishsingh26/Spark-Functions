
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.

spark = SparkSession.builder.master("local[*]").appName("parquet_demo").getOrCreate()

flight_schema = StructType([StructField("airline", StringType(), True), StructField("flight", StringType(), True),
                            StructField("source_city", StringType(), True), StructField("departure_time", StringType(), True),
                            StructField("stops", StringType(), True), StructField("arrival_time", StringType(), True),
                            StructField("destination_city", StringType(), True), StructField("class", StringType(), True),
                            StructField("duration", IntegerType(), True), StructField("days_left", IntegerType(), True),
                            StructField("price", IntegerType(), True)])

df = spark.read.format("csv").option("header", "true").schema(flight_schema).load("../../SampleData/flight_data.csv")
df.printSchema()


# df.write.format("parquet").partitionBy("airline").mode("overwrite").save("../../SampleData/flight.parquet")