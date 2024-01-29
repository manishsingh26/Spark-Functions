

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql import functions as fun
from pyspark.sql import Window

spark = SparkSession.builder.master("local[*]").appName("filer_demo").getOrCreate()

flight_schema = StructType([StructField("airline", StringType(), True), StructField("flight", StringType(), True),
                            StructField("source_city", StringType(), True), StructField("departure_time", StringType(), True),
                            StructField("stops", StringType(), True), StructField("arrival_time", StringType(), True),
                            StructField("destination_city", StringType(), True), StructField("class", StringType(), True),
                            StructField("duration", IntegerType(), True), StructField("days_left", IntegerType(), True),
                            StructField("price", IntegerType(), True)])

df = spark.read.format("csv").option("header", "true").schema(flight_schema).load("../../SampleData/flight_data.csv")

# Group By Example
df_group = df.groupBy("airline").agg(fun.sum(fun.col("price")).alias("total_price"))
df_group.show()

# Group By Using Window Function Example
win_fun = Window.partitionBy("airline")
df_group_win = df.withColumn("total_price", fun.sum("price").over(win_fun).alias("total_price"))
df_group_win = df_group_win.select("airline", "total_price").drop_duplicates()
df_group_win.show()

df.createOrReplaceTempView("flight_dt")

# Group By SQL Example
df_group = spark.sql("SELECT airline, sum(price) AS total_price FROM flight_dt GROUP BY airline")
df_group.show()

# Group By Using Window Function SQL Example
df_group_win = spark.sql("SELECT DISTINCT(airline), SUM(price) OVER (PARTITION BY airline) as total_price FROM flight_dt")
df_group_win.show()
