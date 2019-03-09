from __future__ import print_function
import sys
import time
from pyspark.sql import SparkSession
from pyspark.sql.types import LongType, StringType, StructField, StructType, TimestampType, ArrayType, IntegerType, DoubleType
from pyspark.sql.functions import col, regexp_replace, to_timestamp

# A spark application that reads weather files and cleans bad temperature data.
# In this case, 'bad temperature' is any value that is null.
# It is cleaned by replacing the null value with the average value for that day of year.
#
# Usage: bin/spark-submit --conf spark.driver.host=localhost ~/Documents/meetup/clean_temp.py ~/Downloads/weather_no_headers ~/Downloads/weather_cleaned

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: demo.py <input_path> <output_path>", file=sys.stderr)
        sys.exit(-1)

    spark = SparkSession\
        .builder\
        .appName("Clean Data")\
        .getOrCreate()

    input_path = sys.argv[1]
    output_path = sys.argv[2]

    #"Date/Time","Year","Month","Day","Time","Temp","Temp Flag","Dew Point Temp","Dew Point Temp Flag","Rel Hum (%)","Rel Hum Flag","Wind Dir (10s deg)","Wind Dir Flag","Wind Spd (km/h)","Wind Spd Flag","Visibility (km)","Visibility Flag","Stn Press (kPa)","Stn Press Flag","Hmdx","Hmdx Flag","Wind Chill","Wind Chill Flag","Weather"
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
        StructField("WindDirection", IntegerType(), True)
    ])

    # Create a dataframe from text files
    df = spark.read.csv(\
        input_path, \
        header=False, \
        schema=customSchema)
    # Convert the DateTime field
    df = df.withColumn('DateTime',to_timestamp(col('DateTime')))

    
    # Create view on the dataframe so SQL can be done
    df.createOrReplaceTempView("weather")


    # Create a view of the average temperature per day of year
    avg_temp_per_day = spark.sql("""
        select dayofyear(DateTime) as DayOfYear, 
        avg(Temp) as AvgTemp
        from weather
        group by DayOfYear
    """)


    # Create a new table by joining the weather data with the average daily temp
    avg_temp_per_day.createOrReplaceTempView("daily_averages")
    joined_with_daily_avg = spark.sql("""
        select *
        from weather
        join daily_averages ON dayofyear(DateTime)=daily_averages.DayOfYear
     """)
    joined_with_daily_avg.createOrReplaceTempView("weather")

    # Replace null temperature with average daily value
    cleaned = spark.sql("""
        select DateTime, 
        case when Temp is null then AvgTemp else Temp end as Temp,
        DewPointTemp,
        RelativeHumidity,
        WindDirection
        from weather
    """)
    cleaned.createOrReplaceTempView("weather")

    cleaned.write.mode('overwrite').csv(output_path)
    time.sleep(3600)
