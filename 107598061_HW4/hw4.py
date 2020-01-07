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

conf = SparkConf().setAppName("hw4").setMaster("local")
# sc = SparkContext(conf=conf)
sc = SparkContext.getOrCreate()
sqlContext = SQLContext(sc)

"""upload the input file to the HDFS first
./hadoop fs -mkdir /hw4/
./hadoop fs -put /home/jyx_master/Documents/hw3/* /hw4/
"""

# read data
ss = SparkSession.builder.appName("hw4").config("spark.some.config.option", "some-value").getOrCreate()

movies_df = ss.read.csv(
    'file:/media/jyx/Data/Big Data Mining and Applications/HW4/movies.csv',
    header=True,
    sep=';',
    nullValue='?',
    ignoreLeadingWhiteSpace=True,
    ignoreTrailingWhiteSpace=True,
    inferSchema=True,
    )

ratings_df = ss.read.csv(
    'file:/media/jyx/Data/Big Data Mining and Applications/HW4/ratings.csv',
    header=False,
    sep=';',
    nullValue='?',
    ignoreLeadingWhiteSpace=True,
    ignoreTrailingWhiteSpace=True,
    inferSchema=True,
    )

# users_df = ss.read.csv(
#     'file:/media/jyx/Data/Big Data Mining and Applications/HW4/users.csv',
#     header=True,
#     sep=';',
#     nullValue='?',
#     ignoreLeadingWhiteSpace=True,
#     ignoreTrailingWhiteSpace=True,
#     inferSchema=True,
#     )

rating_users = set()
for row in ratings_df.toLocalIterator():
    rating_users.add(row["_c0"])

movies_dic = {}
for row in movies_df.toLocalIterator():
    movies_dic[row["MovieID"]] = row["Title"]

### task 1 ###

# movies_score = {}
# for ID, Title in movies_dic.items():
#     total_vote = 0
#     sum_score = 0
#     for row in ratings_df.toLocalIterator():
#         if (ID == row["MovieID"]):
#             total_vote += 1
#             sum_score += row["Rating"]

#     if (total_vote > 0):
#         movies_score[ID] = sum_score / total_vote
#         print(Title, movies_score[ID])


# movies_score_order_list = [(avg, movies_score[avg]) for avg in sorted(movies_score, key=movies_score.get, reverse=True)]

# task1 = open("task1.csv","w")
# for item in movies_score_order_list:
#     task1.write(str(movies_dic[item[0]]) + ";" + str(item[1]) + '\n')

# task1.close()

task1_df = ss.read.csv(
    'file:/media/jyx/Data/Big Data Mining and Applications/HW4/task1.csv',
    header=False,
    sep=';',
    nullValue='?',
    ignoreLeadingWhiteSpace=True,
    ignoreTrailingWhiteSpace=True,
    inferSchema=True,
    )
task1_df.show()

### task 3 ###

movies_genres_df = movies_df.select("MovieID", "Genres")

while True:
    mvID = int(input("請輸入電影(ID)："))
    if mvID in movies_dic:
        break
    else:
        print("此電影不存在，請輸入其他ID")

movies_genres_dic = {}
movies_cosine_sim = {}

import math 
def cosine_sim(list1, list2):
    inter = list(set(list1).intersection(set(list2)))
    norn1 = math.sqrt(len(list1))
    norn2 = math.sqrt(len(list2))

    return len(inter) / (norn1*norn2)

for row in movies_df.toLocalIterator():
    movies_genres_dic[row["MovieID"]] = row["Genres"].split("|")

for row in movies_df.toLocalIterator():
    if (row["MovieID"] != mvID):
        movies_cosine_sim[row["MovieID"]] = cosine_sim(movies_genres_dic[mvID], movies_genres_dic[row["MovieID"]])

movies_cosine_order_list = [(cos, movies_cosine_sim[cos]) for cos in sorted(movies_cosine_sim, key=movies_cosine_sim.get, reverse=True)]

for movie in movies_cosine_order_list:
    print(str(movies_dic[movie[0]]) + ", cosine similarity:" + str(movie[1]))

### task 4 ###

print("collaborative filtering: item-based")
while True:
    userID = int(input("請輸入推薦給哪位使用者(ID)："))
    if userID in rating_users:
        break
    else:
        print("此使用者不存在，請輸入其他ID")

num = int(input("要推薦幾部電影："))

# 看該使用者最喜愛哪一個電影
score = 0
fav_movie = 1
for row in ratings_df.toLocalIterator():
    if (row["_c0"] == userID and row["_c2"] >= score):
        score = row["_c2"]
        fav_movie = row["_c1"]

# print(movies_dic[fav_movie], score)

# 找出該電影相似度接近的進行推薦
movies_genres_dic = {}
movies_cosine_sim = {}
for row in movies_df.toLocalIterator():
    movies_genres_dic[row["MovieID"]] = row["Genres"].split("|")

for row in movies_df.toLocalIterator():
    if (row["MovieID"] != fav_movie):
        movies_cosine_sim[row["MovieID"]] = cosine_sim(movies_genres_dic[mvID], movies_genres_dic[row["MovieID"]])

movies_cosine_order_list = [(cos, movies_cosine_sim[cos]) for cos in sorted(movies_cosine_sim, key=movies_cosine_sim.get, reverse=True)]

count = 0
for movie in movies_cosine_order_list:
    count += 1
    print(str(movies_dic[movie[0]]) + ", 推薦分數： " + str(10*movie[1]))
    if (count == num):
        break

#################################################################################################

from pyspark.mllib.recommendation import Rating
rawUserData = sc.textFile('file:/media/jyx/Data/Big Data Mining and Applications/HW4/ratings.csv')
rawRatings = rawUserData.map(lambda line: line.split(";")[:3])
ratingsRDD = rawRatings.map(lambda x: (x[0], x[1], x[2]))

from pyspark.mllib.recommendation import ALS
model = ALS.train(ratingsRDD, 10, 10, 0.01)

print("collaborative filtering: user-based")
while True:
    userID = int(input("請輸入推薦給哪位使用者(ID)："))
    if userID in rating_users:
        break
    else:
        print("此使用者不存在，請輸入其他ID")

num = int(input("要推薦幾部電影："))

# item[0]: user; item[1]: movie; item[2]: rating
for item in model.recommendProducts(userID, num):
    print(movies_dic[item[1]] + ", 推薦分數： " + str(item[2]))
    