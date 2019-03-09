from __future__ import print_function
import sys
import time
from pyspark.sql import SparkSession
from pyspark.sql.types import LongType, StringType, StructField, StructType, TimestampType, ArrayType, IntegerType, DoubleType
from pyspark.sql.functions import col, regexp_replace, to_timestamp


input_path = '/Users/ekeddy/Downloads/weather_no_headers'

#=== Read data
df = spark.read.csv(input_path)


#==== Read data using schema
customSchema = StructType([
        StructField("DateTime", StringType(), True),        
        StructField("Year", IntegerType(), True),
        StructField("Month", IntegerType(), True),
        StructField("Day", IntegerType(), True),
        StructField("Time", StringType(), True),
        StructField("Temp", DoubleType(), True),
        StructField("TempFlag", StringType(), True),
        StructField("DewPointTemp", DoubleType(), True),
        StructField("DewPointTempFlag", StringType(), True),
        StructField("RelativeHumidity", IntegerType(), True),
        StructField("RelativeHumidityFlag", StringType(), True),
        StructField("WindDirection", IntegerType(), True),
        StructField("WindDirectionFlag", StringType(), True)
    ])
df = spark.read.csv(input_path, schema=customSchema)


df.createOrReplaceTempView("weather")
spark.sql('select * from weather').show()
spark.sql('select min(Temp),max(Temp) from weather').show()
spark.sql('select * from weather where Temp = (select min(Temp) from weather)').show()