
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql import functions as fun

spark = SparkSession.builder.master("local[*]").appName("filter_demo").getOrCreate()

flight_schema = StructType([StructField("airline", StringType(), True), StructField("flight", StringType(), True),
                            StructField("source_city", StringType(), True), StructField("departure_time", StringType(), True),
                            StructField("stops", StringType(), True), StructField("arrival_time", StringType(), True),
                            StructField("destination_city", StringType(), True), StructField("class", StringType(), True),
                            StructField("duration", IntegerType(), True), StructField("days_left", IntegerType(), True),
                            StructField("price", IntegerType(), True)])

df = spark.read.format("csv").option("header", "true").schema(flight_schema).load("../../SampleData/flight_data.csv")

# Filter Example
df_filter = df.filter((df.airline == "Indigo") & (df.price >= 5955))
df_filter.show()

df.createOrReplaceTempView("flight_dt")

# Filter SQL Example
spark.sql("SELECT * FROM flight_dt WHERE price=5955").show()
