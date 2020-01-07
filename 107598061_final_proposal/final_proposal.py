from __future__ import division
from pyspark import SparkConf, SparkContext, SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as f
import sys
import os.path
import os
import string
import re
import random
import time
import binascii

conf = SparkConf().setAppName("final_proposal").setMaster("local")
# sc = SparkContext(conf=conf)
sc = SparkContext.getOrCreate()
sqlContext = SQLContext(sc)

"""upload the input file to the HDFS first
./hadoop fs -mkdir /final_proposal/
./hadoop fs -put /home/jyx_master/Documents/final_proposal/* /final_proposal/
"""

# source: https://data.gov.tw/dataset/6252

# read data
ss = SparkSession.builder.appName("final_proposal").config("spark.some.config.option", "some-value").getOrCreate()

df = ss.read.csv(
    '/final_proposal/107598061_final_proposal/106年計程車營運狀況調查原始資料new.csv',
    header=True,
    sep=",",
    nullValue="NULL",
    ignoreLeadingWhiteSpace=True,
    ignoreTrailingWhiteSpace=True,
    inferSchema=True
    )

# B4_1:晚上12時~凌晨4時
# B4_2:凌晨4時~上午6時
# B4_3:上午6時~早上8時
# B4_4:早上8時~早上10時
# B4_5:早上10時~中午12時
# B4_6:中午12時~下午2時
# B4_7:下午2時~下午4時
# B4_8:下午4時~晚上6時
# B4_9:晚上6時~晚上8時
# B4_10:晚上8時~晚上10時
# B4_11:晚上10時~晚上12時

time_dic = {"00-04":"晚上12時~凌晨4時", 
            "04-06":"凌晨4時~上午6時", 
            "06-08":"上午6時~早上8時", 
            "08-10":"早上10時~中午12時",
            "10-12":"早上8時~早上10時",
            "12-14":"中午12時~下午2時",
            "14-16":"下午2時~下午4時",
            "16-18":"下午4時~晚上6時",
            "18-20":"晚上6時~晚上8時",
            "20-22":"晚上8時~晚上10時",
            "22-00":"晚上10時~晚上12時"}

# E7a:最主要營業縣市
# E7b:次要營業縣市

city_dic = {1:"新北市", 
            2:"臺北市",
            3:"桃園市",
            4:"臺中市",
            5:"臺南市",
            6:"高雄市",
            7:"宜蘭縣",
            8:"新竹縣",
            9:"苗栗縣",
            10:"彰化縣",
            11:"南投縣",
            12:"雲林縣",
            13:"嘉義縣",
            14:"屏東縣",
            15:"臺東縣",
            16:"花蓮縣",
            17:"澎湖縣",
            18:"基隆市",
            19:"新竹市",
            20:"嘉義市",
            21:"金門縣",
            22:"連江縣"}

my_df = df.select(df.B4_1.alias("00-04"), 
                  df.B4_2.alias("04-06"), 
                  df.B4_3.alias("06-08"), 
                  df.B4_4.alias("08-10"), 
                  df.B4_5.alias("10-12"), 
                  df.B4_6.alias("12-14"), 
                  df.B4_7.alias("14-16"), 
                  df.B4_8.alias("16-18"), 
                  df.B4_9.alias("18-20"), 
                  df.B4_10.alias("20-22"), 
                  df.B4_11.alias("22-00"), 
                  df.E7a.alias("主要營業城市"), 
                  df.E7b.alias("次要營業城市"))
my_df.show()

def addTimeToDic(item, dic):
    if (item in dic):
        dic[item] += 1
    else:
        dic[item] = 1

def addCityToDic(city, dic):
    if (city_dic[city] in dic):
        dic[city_dic[city]] += 1
    else:
        dic[city_dic[city]] = 1

time_list = ["00-04", "04-06", "06-08", "08-10", "10-12", "12-14",
             "14-16", "16-18", "18-20", "20-22", "22-00"]

count_time = {}
count_main_city = {}
count_sec_city = {}
for row in my_df.toLocalIterator():
    for time in time_list:
        if (row[time]):
            addTimeToDic(time, count_time)

    if (row["主要營業城市"]):
        addCityToDic(row["主要營業城市"], count_main_city)
    if (row["次要營業城市"]):
        addCityToDic(row["次要營業城市"], count_sec_city)


sorted_time_dic = sorted(count_time.items(), key=lambda x: x[1], reverse=True)
print("最容易搭到車的前三時段:")
for time in range(0, 3):
    print(time_dic[sorted_time_dic[time][0]])

# 綜合主要營業和次要營業縣市評分表: 主要營業權重10 次要6
count_city = {}
for city in count_main_city:
    if (city in count_city):
        count_city[city] += 10
    else:
        count_city[city] = 10

for city in count_sec_city:
    if (city in count_city):
        count_city[city] += 6
    else:
        count_city[city] = 6

sorted_count_city = sorted(count_city.items(), key=lambda x: x[1], reverse=True)
print("最容易搭到車的前三縣市:")
for city in range(0, 3):
    print(sorted_count_city[city][0])