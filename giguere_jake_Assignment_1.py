from __future__ import print_function

import os
import sys
import requests
from operator import add

from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark import StorageLevel

from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

from pyspark.sql.types import *
from pyspark.sql import functions as func
from pyspark.sql.functions import *


#Exception Handling and removing wrong datalines
def isfloat(value):
    try:
        float(value)
        return True
 
    except:
         return False

#Function - Cleaning
#For example, remove lines if they don’t have 16 values and 
# checking if the trip distance and fare amount is a float number
# checking if the trip duration is more than a minute, trip distance is more than 0.1 miles, 
# fare amount and total amount are more than 0.1 dollars
def correctRows(p):
    if(len(p)==17):
        if(isfloat(p[5]) and isfloat(p[11])):
            if(float(p[4])> 60 and float(p[5])>0 and float(p[11])> 0 and float(p[16])> 0):
                return p

#Main
if __name__ == "__main__":
    #if len(sys.argv) != 4:
    #    print("Usage: main_task1 <file> <output> ", file=sys.stderr)
    #    exit(-1)
    

    sc = SparkContext(appName="Assignment-1")
    
    spark = SparkSession.builder.appName("assignment-1").getOrCreate()
    # convert file into RDD
    # rdd = sc.parallelize(sys.argv[1])
    
    """
    Task 1:
    Determine top 10 distinct drivers
    output a set of pairs consisting of medallion number and the corresponding count of drivers
    E.g Taxi #1: 10 distinct drivers
    """
    df = spark.read.format('csv').options(header='false', inferSchema='true',  sep =",").load(sys.argv[1])
    # df.show()
    
    # convert dataframe to RDD
    testRDD = df.rdd.map(tuple)
    # clean rows without values
    filteredRDD = testRDD.filter(correctRows)
    
    df_filtered = spark.createDataFrame(filteredRDD)
    
    # Persist the DataFrame to avoid recomputation
    df_filtered.persist(StorageLevel.MEMORY_AND_DISK)

    # Group by the taxi medallion (_1 is the first column) and count distinct values in the second column (_2 for hack_license)
    df_grouped = df_filtered.groupBy("_1").agg(countDistinct("_2").alias("distinct_driver_count"))
    
    df_grouped.persist(StorageLevel.MEMORY_AND_DISK)

    # Sort by the count of distinct drivers in descending order
    df_sorted = df_grouped.orderBy("distinct_driver_count", ascending=False, mode="overwrite")

    # Take the top 10 taxis
    top_10_taxis = df_sorted.limit(10)

    # Show the result
    # top_10_taxis.show()
    
    top_10_taxis.coalesce(1).write.csv(sys.argv[2], header=True, mode="overwrite")


    """
    Task 2:
    Determine top 10 Best Drivers
    output a set of pairs consisting of driver and the money/minute 
    E.g Driver1: $4/min
    """

    # Calculate money per minute for each trip: total_amount / (trip_time_in_secs / 60)
    df_filtered = df_filtered.withColumn("money_per_minute", df_filtered["_16"] / (df_filtered["_5"] / 60))

    # Group by driver (_2), and calculate the average money per minute
    df_grouped2 = df_filtered.groupBy("_2").agg(func.avg("money_per_minute").alias("avg_money_per_minute"))

    # Sort by the average money per minute in descending order
    df_sorted2 = df_grouped2.orderBy("avg_money_per_minute", ascending=False)

    # Take the top 10 drivers
    top_10_drivers = df_sorted2.limit(10)

    # Save the result with overwrite mode
    top_10_drivers.coalesce(1).write.csv(sys.argv[3], header=True, mode='overwrite')

    sc.stop()


    #Task 3 - Optional 
    #Your code goes here

    #Task 4 - Optional 
    #Your code goes here


    sc.stop()