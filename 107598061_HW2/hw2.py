from __future__ import division
from pyspark import SparkConf, SparkContext, SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as f
import sys
import os
sys.path.append(os.path.abspath("/home/jyx_master/Documents/hw2"))
#from co_list import *

conf = SparkConf().setAppName("hw2").setMaster("local")
sc = SparkContext.getOrCreate()
sqlContext = SQLContext(sc)

"""upload the input file to the HDFS first
./bin/hadoop fs -mkdir /hw2/
./bin/hadoop fs -put /home/jyx_master/Documents/hw2/*.csv /hw2/
"""

def record_word_freq(df, col):
    words_dict = {}
    def record_word(sentence):
        words_list = sentence.split(" ")
        for word in words_list:
            if not (word == "..." or word == " " or word == ","):
                if word in words_dict:
                    words_dict[word] += 1
                else:
                    words_dict[word] = 1
    for item in df.toLocalIterator():
        sentence = item[col]
        record_word(sentence)
    return words_dict


def show_result(words_dict, total_count = 11):
    words_dict_order_list = [(word, words_dict[word]) for word in sorted(words_dict, key=words_dict.get, reverse=True)]
    count = 1
    for key, freq in words_dict_order_list:
        print("出現頻率 第",count,"名: ", key, ", 出現", freq, "次數")
        count += 1
        if count == total_count: break


def is_date(test_string):
    try:
        int(str(test_string[0:4]))
        return True
    except:
        return False


def same_day(current_day, new_day):
    if current_day[0:10] == new_day[0:10]:
        return True
    else:
        return False


def record_each_day_word_freq(df, col, date_col="PublishDate"):
    words_dict = {}
    def record_word(sentence):
        words_list = sentence.split(" ")
        for word in words_list:
            if not (word == "..." or word == " " or word == ","):
                if word in words_dict:
                    words_dict[word] += 1
                else:
                    words_dict[word] = 1

    # record first date
    current_day = df.collect()[0][date_col]
    for row in df.toLocalIterator():
        if is_date(row[col][0:10]):
            if same_day(row[date_col] ,current_day):
                record_word(row[col])
            else:
                print("at ", current_day[0:10], ":")
                show_result(words_dict)
                words_dict.clear()
                record_word(row[col])
                current_day = row[date_col]


def record_each_topic_word_freq(df, topic, col):
    words_dict = {}
    def record_word(sentence):
        words_list = sentence.split(" ")
        for word in words_list:
            if not (word == "..." or word == " " or word == ","):
                if word in words_dict:
                    words_dict[word] += 1
                else:
                    words_dict[word] = 1
    # record first topic
    for row in df.toLocalIterator():
        if row["Topic"] == topic:
            record_word(row[col])
    print("topic", topic, " at ", col,":")
    show_result(words_dict)


def sum_avg_score_of_topic(df, topic, sentimen_col):
    length = 0
    sum_score = 0
    for row in df.toLocalIterator():
        if row["Topic"] == topic:
            sum_score += float(row[sentimen_col])
            length += 1
    avg_score = sum_score / length
    print(topic, ": ", sentimen_col,",sum score: ", sum_score, ", avg score: ", avg_score)


def get_top_100_words_in_topic(df, topic, col_1, col_2):
    words_dict = {}
    def record_word(sentence):
        words_list = sentence.split(" ")
        for word in words_list:
            if not (word == "..." or word == " " or word == "," or "'" in word or '"' in word):
                if word in words_dict:
                    words_dict[word] += 1
                else:
                    words_dict[word] = 1
    for row in df.toLocalIterator():
        if row["Topic"] == topic:
            record_word(row[col_1])
            record_word(row[col_2])
    words_dict_order_list = [(word, words_dict[word]) for word in sorted(words_dict, key=words_dict.get, reverse=True)]
    top_100_words = words_dict_order_list[0:100]
    return top_100_words


def get_co_ocurrence_list(df, top_100_words_list):
    co_occurence_list = []
    for top_word in top_100_words_list:
        word_string = top_word
        for row in df.toLocalIterator():
            title_list = row["Title"].split()
            headline_list = row["Headline"].split()
            join_list = title_list + headline_list
            for word in join_list:
                if (top_word in join_list) and (word != top_word) and (word in top_100_words_list) and (not("'" in word)) and (not(' ' in word)):
                    word_string += ","
                    word_string += word
            co_occurence_list.append(str(word_string))
            if (len(co_occurence_list) == 100): # just top 100 data
                #print(co_occurence_list)
                return co_occurence_list
            word_string = top_word
    return co_occurence_list

    # read data
ss = SparkSession.builder.appName("hw2")\
    .config("spark.some.config.option", "some-value")\
    .getOrCreate()
df = ss.read.csv(
    'file:/media/jyx/Data/Big Data Mining and Applications/107598061_HW2/News_Final.csv',
    header=True,
    sep=",",
    nullValue='?',
    ignoreLeadingWhiteSpace=True,
    ignoreTrailingWhiteSpace=True,
    inferSchema=True,
    )

"""Subtask 1"""
print("in total:")
print("in Title:")
title_df = df.select("Title")
words_dict = record_word_freq(title_df, "Title")
show_result(words_dict)

print("in Headline:")
headline_df = df.select("Headline")
words_dict = record_word_freq(headline_df, "Headline")
show_result(words_dict)

print("perday")
# sort dataframe by date => let same day be close
date_words_df = df.orderBy(f.from_unixtime(f.col("PublishDate"), 'yyyy-MM-dd HH:mm:ss.')
        .cast("timestamp")).select("Title", "Headline", "PublishDate")

record_each_day_word_freq(date_words_df, "Title")
record_each_day_word_freq(date_words_df, "Headline")  

print("topic")
topic_words_df = df.select("Title", "Headline", "Topic")

record_each_topic_word_freq(topic_words_df, "obama", "Title")
record_each_topic_word_freq(topic_words_df, "obama", "Headline")

record_each_topic_word_freq(topic_words_df, "economy", "Title")
record_each_topic_word_freq(topic_words_df, "economy", "Headline")

record_each_topic_word_freq(topic_words_df, "microsoft", "Title")
record_each_topic_word_freq(topic_words_df, "microsoft", "Headline")

record_each_topic_word_freq(topic_words_df, "palestine", "Title")
record_each_topic_word_freq(topic_words_df, "palestine", "Headline")

"""subtask 3"""

score_df = df.select("Topic", "SentimentTitle", "SentimentHeadline")

sum_avg_score_of_topic(score_df, "obama", "SentimentTitle")
sum_avg_score_of_topic(score_df, "obama", "SentimentHeadline")

sum_avg_score_of_topic(score_df, "economy", "SentimentTitle")
sum_avg_score_of_topic(score_df, "economy", "SentimentHeadline")

sum_avg_score_of_topic(score_df, "microsoft", "SentimentTitle")
sum_avg_score_of_topic(score_df, "microsoft", "SentimentHeadline")

sum_avg_score_of_topic(score_df, "palestine", "SentimentTitle")
sum_avg_score_of_topic(score_df, "palestine", "SentimentHeadline")

"""subtask 4"""
# record top-100 frequent words per topic in titles and headlines
words_df = df.select("Title", "Headline", "Topic")
# topic: obama
obama_top_100_words_list = get_top_100_words_in_topic(words_df, "obama", "Title", "Headline")

# create a list to record each words co-occurence; ex: ["apple,book", "book,car", ...]
for i in range(0, 100): obama_top_100_words_list[i] = obama_top_100_words_list[i][0]

co_occurence_list = get_co_ocurrence_list(words_df, obama_top_100_words_list)    

co_df = ss.createDataFrame(
    co_occurence_list , "string"
).toDF("basket")

co_matrix = (co_df
    .withColumn("id", f.monotonically_increasing_id())
    .select("id", f.explode(f.split("basket", ","))))

co_matrix.withColumnRenamed("col", "col_").join(co_matrix, ["id"]).stat.crosstab("col", "col_").show()

# topic: economy
economy_top_100_words_list = get_top_100_words_in_topic(words_df, "economy", "Title", "Headline")
for i in range(0, 100): economy_top_100_words_list[i] = economy_top_100_words_list[i][0].lower()

co_occurence_list = get_co_ocurrence_list(words_df, economy_top_100_words_list)

co_df = ss.createDataFrame(
    co_occurence_list , "string"
).toDF("basket")

co_matrix = (co_df
    .withColumn("id", f.monotonically_increasing_id())
    .select("id", f.explode(f.split("basket", ","))))

co_matrix.withColumnRenamed("col", "col_").join(co_matrix, ["id"]).stat.crosstab("col", "col_").show()

# topic: microsoft
microsoft_top_100_words_list = get_top_100_words_in_topic(words_df, "microsoft", "Title", "Headline")
for i in range(0, 100): microsoft_top_100_words_list[i] = microsoft_top_100_words_list[i][0].lower()

co_occurence_list = get_co_ocurrence_list(words_df, economy_top_100_words_list)

co_df = ss.createDataFrame(
    co_occurence_list , "string"
).toDF("basket")

co_matrix = (co_df
    .withColumn("id", f.monotonically_increasing_id())
    .select("id", f.explode(f.split("basket", ","))))

co_matrix.withColumnRenamed("col", "col_").join(co_matrix, ["id"]).stat.crosstab("col", "col_").show()

# topic: palestine
palestine_top_100_words_list = get_top_100_words_in_topic(words_df, "palestine", "Title", "Headline")
for i in range(0, 100): palestine_top_100_words_list[i] = palestine_top_100_words_list[i][0].lower()

co_occurence_list = get_co_ocurrence_list(words_df, palestine_top_100_words_list)

co_df = ss.createDataFrame(
    co_occurence_list , "string"
).toDF("basket")

co_matrix = (co_df
    .withColumn("id", f.monotonically_increasing_id())
    .select("id", f.explode(f.split("basket", ","))))

co_matrix.withColumnRenamed("col", "col_").join(co_matrix, ["id"]).stat.crosstab("col", "col_").show()


ss.stop()

    
