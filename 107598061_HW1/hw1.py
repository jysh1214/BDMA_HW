from __future__ import division
from pyspark import SparkConf, SparkContext, SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as f

conf = SparkConf().setAppName("hw1").setMaster("local")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

"""upload the input file to the HDFS first
bin/hadoop fs -mkdir /hw1/
bin/hadoop fs -put /home/jyx_master/Documents/hw1/household_power_consumption.txt /hw1/
"""

# read data
ss = SparkSession.builder.appName("hw1").config("spark.some.config.option", "some-value").getOrCreate()
df = ss.read.csv(
        '/hw1/household_power_consumption.txt',
        header=True,
        sep=";",
        nullValue='?',
        ignoreLeadingWhiteSpace=True,
        ignoreTrailingWhiteSpace=True,
        inferSchema=True,
        )

df.show()

# Out the min, max, and count of the columns: 'global active power', 'global reactive power', 'voltage', and 'global intensity'

# min
min_global_active_power = df.agg({"Global_active_power": "min"}).collect()[0][0]
print("min Global_active_power: ", min_global_active_power)

min_global_reactive_power = df.agg({"Global_reactive_power": "min"}).collect()[0][0]
print("min Global_reactive_power: ", min_global_reactive_power)

min_voltage = df.agg({"Voltage": "min"}).collect()[0][0]
print("min Voltage: ", min_voltage)

min_global_intensity = df.agg({"Global_intensity": "min"}).collect()[0][0]
print("min Global_intensity:" , min_global_intensity)

# max
max_global_active_power = df.agg({"Global_active_power": "max"}).collect()[0][0]
print("max Global_active_power: ", max_global_active_power)

max_global_reactive_power = df.agg({"Global_reactive_power": "max"}).collect()[0][0]
print("max Global_reactive_power: ", max_global_reactive_power)

max_voltage = df.agg({"Voltage": "max"}).collect()[0][0]
print("max Voltage: ", max_voltage)

max_global_intensity = df.agg({"Global_intensity": "max"}).collect()[0][0]
print("max Global_intensity:" , max_global_intensity)

print("count of the columns: ", df.count())

# mean
mean_global_active_power = df.agg({"Global_active_power": "mean"}).collect()[0][0]
print("mean of Global_active_power: ", mean_global_active_power)

mean_global_reactive_power = df.agg({"Global_reactive_power": "mean"}).collect()[0][0]
print("mean of Global_reactive_power: ", mean_global_reactive_power)

mean_voltage = df.agg({"Voltage": "mean"}).collect()[0][0]
print("mean of Voltage: ", mean_voltage)

mean_global_intensity = df.agg({"Global_intensity": "mean"}).collect()[0][0]
print("mean Global_intensity:" , mean_global_intensity)

# standard deviation
stddev_global_active_power = df.agg({"Global_active_power": "stddev"}).collect()[0][0]
print("standard deviation of Global_active_power: ", stddev_global_active_power)

stddev_global_reactive_power = df.agg({"Global_reactive_power": "stddev"}).collect()[0][0]
print("standard deviation of Global_reactive_power: ", stddev_global_reactive_power)

stddev_voltage = df.agg({"Voltage": "stddev"}).collect()[0][0]
print("standard deviation of Voltage: ", stddev_voltage)

stddev_global_intensity = df.agg({"Global_intensity": "stddev"}).collect()[0][0]
print("standard deviation of Global_intensity:" , stddev_global_intensity)

df = df.withColumn("normalized_Global_active_power", 
        (f.col("Global_active_power")-min_global_active_power)/(max_global_active_power-min_global_active_power))

df = df.withColumn("normalized_Global_reactive_power",
        (f.col("Global_reactive_power")-min_global_reactive_power)/(max_global_reactive_power-min_global_reactive_power))

df = df.withColumn("normalized_Voltage",
        (f.col("Voltage")-min_voltage)/(max_voltage-min_voltage))

df = df.withColumn("normalized_Global_intensity",
        (f.col("Global_intensity")-min_global_intensity)/(max_global_intensity-min_global_intensity))

columns_to_drop = ["Date", "Time", "Global_active_power", "Global_reactive_power", "Voltage", "Global_intensity", 
        "Sub_metering_1", "Sub_metering_2", "Sub_metering_3"]
df = df.drop(*columns_to_drop)
df.show()

df.write.csv("hw1.csv")
print("saved: hw1.csv")

