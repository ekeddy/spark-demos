from __future__ import print_function
from pyspark.sql import SparkSession
import sys
import re


# A simple spark app to cleanup raw weather files by removing all
# the header lines.
# Usage: bin/spark-submit  --conf spark.driver.host=localhost ~/Documents/meetup/cleanup_csv.py ~/Downloads/weather ~/Downloads/weather_no_headers
#
start_of_data_line_pattern = re.compile('.[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}')

def is_data_line(line):
    return False if start_of_data_line_pattern.match(line) is None else True

if __name__ == "__main__":

    if len(sys.argv) != 3:
        print("Usage: cleanup_csv <input_path> <output_path>", file=sys.stderr)
        sys.exit(-1)
    
    input_path = sys.argv[1]
    output_path = sys.argv[2]

    spark = SparkSession\
        .builder\
        .appName("Cleanup CSV")\
        .getOrCreate()

    # Create an RDD that reads all files from the input directory
    # and then excludes any lines that don't match the criteria
    rdd = spark.sparkContext\
        .textFile(input_path)\
        .filter(is_data_line)

    # Save the results to text files.
    rdd.saveAsTextFile(output_path)

