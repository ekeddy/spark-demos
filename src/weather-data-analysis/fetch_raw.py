from __future__ import print_function
import requests
import datetime
import sys
from dateutil.relativedelta import relativedelta
from pyspark.sql import SparkSession

def fetch_data(idx, iterator):
    url_template = 'http://climate.weather.gc.ca/climate_data/bulk_data_e.html?format=csv&stationID={}&Year={}&Month={}&Day={}&timeframe=1&submit=Download+Data'
    contents = []

    for (stn,date) in iterator:
        # Fetch the data
        url = url_template.format(stn, date.year, date.month, date.day)
        content = requests.get(url).content
        contents.append(content)
    return contents


if __name__ == "__main__":

    if len(sys.argv) != 4:
        print("Usage: fetch_raw output_path start_date max_months", file=sys.stderr)
        sys.exit(-1)
    

    output_path = sys.argv[1]
    start_date = datetime.datetime.strptime(sys.argv[2],"%Y-%m-%d")
    end_date = start_date + relativedelta(months=+int(sys.argv[3]))
    if end_date > datetime.datetime.now():
        end_date = datetime.datetime.now()

    station_id = "49568"
    dates = []
    date = start_date
    while date < end_date:
        dates.append((station_id,date))
        date += relativedelta(months=+1)

    spark = SparkSession\
        .builder\
        .appName("Fetch Data")\
        .getOrCreate()
    
    res = spark.sparkContext.parallelize(dates,len(dates))\
        .mapPartitionsWithIndex(fetch_data)

    res.saveAsTextFile(output_path)
