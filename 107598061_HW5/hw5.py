from __future__ import division
from pyspark import SparkConf, SparkContext, SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as f
# import html
# from bs4 import BeautifulSoup
import sys
import os.path
import os
import string
import re
import random
import time
import binascii

from os import environ
environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.databricks:spark-xml_2.10:0.4.1 pyspark-shell' 

conf = SparkConf().setAppName("hw5").setMaster("local")
# sc = SparkContext(conf=conf)
sc = SparkContext.getOrCreate()
sqlContext = SQLContext(sc)

# read data
ss = SparkSession.builder.appName("hw5").config("spark.some.config.option", "some-value").getOrCreate()

df = ss.read.csv(
    'file:/media/jyx/Data/Big Data Mining and Applications/HW5/hw5.csv',
    header=True,
    sep='\t',
    nullValue='?',
    ignoreLeadingWhiteSpace=True,
    ignoreTrailingWhiteSpace=True,
    inferSchema=True,
    )

df.show()

outlink_dict = {}
for row in df.toLocalIterator():
    if row["FromNodeId"] not in outlink_dict:
        outlink_dict[row["FromNodeId"]] = 1
    else:
        outlink_dict[row["FromNodeId"]] += 1

order_outlink_list = [(outlink, outlink_dict[outlink]) for outlink in sorted(outlink_dict, key=outlink_dict.get, reverse=False)]
print(order_outlink_list)

# task1 = open("task1.csv", "w")
# for item in order_outlink_list:
#     task1.write(str(item[0]) + ";" + str(item[1]) + '\n')
# task1.close()
    
# task1_df = ss.read.csv(
#     'file:/media/jyx/Data/Big Data Mining and Applications/HW5/task1.csv',
#     header=False,
#     sep=';',
#     nullValue='?',
#     ignoreLeadingWhiteSpace=True,
#     ignoreTrailingWhiteSpace=True,
#     inferSchema=True,
#     )

# task1_df.show()

inlink_dict = {}
for row in df.toLocalIterator():
    if row["ToNodeId"] not in inlink_dict:
        inlink_dict[row["ToNodeId"]] = 1
    else:
        inlink_dict[row["ToNodeId"]] += 1

order_inlink_dict = [(inlink, inlink_dict[inlink]) for inlink in sorted(inlink_dict, key=inlink_dict.get, reverse=False)]
print(order_inlink_dict)

# task2 = open("task2.csv", "w")
# for item in order_inlink_dict:
#     task2.write(str(item[0]) + ";" + str(item[1]) + '\n')
# task2.close()
    
# task2_df = ss.read.csv(
#     'file:/media/jyx/Data/Big Data Mining and Applications/HW5/task2.csv',
#     header=False,
#     sep=';',
#     nullValue='?',
#     ignoreLeadingWhiteSpace=True,
#     ignoreTrailingWhiteSpace=True,
#     inferSchema=True,
#     )

# task2_df.show()

nodeID = int(input("請輸入Node ID: "))

order_outlink_dict_to_node = {}
for row in df.toLocalIterator():
    if(row["FromNodeId"] == nodeID):
        if(row["ToNodeId"] not in order_outlink_dict_to_node):
            order_outlink_dict_to_node[row["ToNodeId"]] = 1
        else:
            order_outlink_dict_to_node[row["ToNodeId"]] += 1

order_inlink_dict = [(inlink, order_outlink_dict_to_node[inlink])\
for inlink in sorted(order_outlink_dict_to_node, key=order_outlink_dict_to_node.get, reverse=True)]

print("Outlink: ")
for item in order_inlink_dict:
    print(item[0])

order_inlink_dict_to_node = {}
for row in df.toLocalIterator():
    if(row["ToNodeId"] == nodeID):
        if(row["FromNodeId"] not in order_inlink_dict_to_node):
            order_inlink_dict_to_node[row["FromNodeId"]] = 1
        else:
            order_inlink_dict_to_node[row["FromNodeId"]] += 1

order_outlink_dict = [(outlink, order_inlink_dict_to_node[outlink])\
for outlink in sorted(order_inlink_dict_to_node, key=order_inlink_dict_to_node.get, reverse=True)]

print("Inlink: ")
for item in order_outlink_dict:
    print(item[0])