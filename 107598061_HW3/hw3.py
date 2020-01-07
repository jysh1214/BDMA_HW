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

from os import environ
environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.databricks:spark-xml_2.10:0.4.1 pyspark-shell' 

conf = SparkConf().setAppName("hw3").setMaster("local")
# sc = SparkContext(conf=conf)
sc = SparkContext.getOrCreate()
sqlContext = SQLContext(sc)

"""upload the input file to the HDFS first
./hadoop fs -mkdir /hw3/
./hadoop fs -put /home/jyx_master/Documents/hw3/* /hw3/
"""

# read data
ss = SparkSession.builder.appName("hw3").config("spark.some.config.option", "some-value").getOrCreate()

df = ss.read \
    .format('com.databricks.spark.xml') \
    .options(rowTag='REUTERS') \
    .load('file:/media/jyx/Data/Big Data Mining and Applications/HW3/final.xml')

df.show()

documents = []
for row in df.toLocalIterator():
    documents.append(str(row["BODY"]).lower())

print('The number of documents read was: ' + str(len(documents)))

i = 0
d = {}
t = {}
t0 = time.time()
for value in documents:
    d[i] = value
    d[i] = re.sub("[^\w]", " ", d[i]).split()
    if d[i]:
        i = i + 1

docsAsShingleSets = {}
docNames = []
totalShingles = 0
shingleNo = 0

while True:
    try:
        shingle_size = int(input("Please input k value for k-shingles: "))
    except ValueError:
        print("ERROR: k must > 0")
        continue
    if shingle_size <= 0:
        continue
    else:
        break

shinglesInDocWords = set()
# loop through all the documents
for i in range(0, len(d)):
    words = d[i]
    docID = i
    docNames.append(docID)

    shinglesInDocWords = set()
    shinglesInDocInts = set()

    shingle = []
    for index in range(len(words) - shingle_size + 1):
        shingle = words[index:index + shingle_size]
        shingle = " ".join(shingle)
        crc = binascii.crc32(shingle.encode()) & 0xffffffff

        if shingle not in shinglesInDocWords:
            shinglesInDocWords.add(shingle)

        if crc not in shinglesInDocInts:
            shinglesInDocInts.add(crc)
            shingleNo = shingleNo + 1
        else:
            del shingle
            index = index - 1
    docsAsShingleSets[docID] = shinglesInDocInts

print("總共" + str(shingleNo) + "個shingles")

# task 1
# task1 = open("task1.csv","w")
for row in df.toLocalIterator():
    for shingles in shinglesInDocWords:
        if (shingles in row["BODY"]):
            # task1.write(shingles + "," + str(row["_NEWID"]) + '\n')
            print(shingles + "存在於第" + str(row["_NEWID"]) + "篇文章")

# task1.close()

# task 2
while True:
    try:
        numHashes = int(input("要使用多少個hash function: "))
    except ValueError:
        print("Your input is not valid.")
        continue
    if numHashes <= 0:
        continue
    else:
        break

def MillerRabinPrimalityTest(number):
    if number == 2:
        return True
    elif number == 1 or number % 2 == 0:
        return False

    oddPartOfNumber = number - 1

    timesTwoDividNumber = 0

    while oddPartOfNumber % 2 == 0:
        oddPartOfNumber = oddPartOfNumber / 2
        timesTwoDividNumber = timesTwoDividNumber + 1

    for time in range(3):
        while True:
            randomNumber = random.randint(2, number) - 1
            if randomNumber != 0 and randomNumber != 1:
                break

        randomNumberWithPower = pow(int(randomNumber), int(oddPartOfNumber), int(number))
        if (randomNumberWithPower != 1) and (randomNumberWithPower != number - 1):
            iterationNumber = 1
            while (iterationNumber <= timesTwoDividNumber - 1) and (randomNumberWithPower != number - 1):
                randomNumberWithPower = pow(randomNumberWithPower, 2, number)
                iterationNumber = iterationNumber + 1
            if (randomNumberWithPower != (number - 1)):
                return False

    return True

i = 1
while not MillerRabinPrimalityTest(shingleNo + i):
    i = i + 1

maxShingleID = shingleNo
nextPrime = shingleNo + i
# h(x) = (a*x + b) % c
def pickRandomCoeffs(k):
    randList = []
    while k > 0:
        randIndex = random.randint(0, maxShingleID)
        while randIndex in randList:
            randIndex = random.randint(0, maxShingleID)

        randList.append(randIndex)
        k = k - 1
    return randList

coeffA = pickRandomCoeffs(numHashes)
coeffB = pickRandomCoeffs(numHashes)
signatures = []

# task2 = open("task2.csv","w")
for docID in docNames:
    shingleIDSet = docsAsShingleSets[docID]
    signature = []

    # For each of the random hash functions...
    for i in range(0, numHashes):
        minHashCode = nextPrime + 1
        for shingleID in shingleIDSet:
            hashCode = (coeffA[i] * shingleID + coeffB[i]) % nextPrime
            if hashCode < minHashCode:
                minHashCode = hashCode

        signature.append(minHashCode)

    # Store the MinHash signature for this document.
    signatures.append(signature)
    # print(signatures)
    # task2.write(str(docID) + "," + str(signature[0]) + "," + str(signature[1]) + '\n')
    print("第" + str(docID) + "文章的MinHash signature:", end='')
    for h in range(0, len(signature)):
        print(str(signature[h]) + " ", end='')
    print()

# task2.close()
numDocs = len(signatures)

# task 3
numElems = int(len(docsAsShingleSets) * (len(docsAsShingleSets) - 1) / 2)
JSim = [0 for x in range(numElems)]
estJSim = [0 for x in range(numElems)]

def getTriangleIndex(i, j):
    if i == j:
        sys.stderr.write("Can't access triangle matrix with i == j")
        sys.exit(1)
    if j < i:
        temp = i
        i = j
        j = temp

    k = int(i * (len(docsAsShingleSets) - (i + 1) / 2.0) + j - i) - 1
    return k

# task3 = open("task3.csv", "w")
neighbors = 1
for docID in docNames:
    s0 = len(docsAsShingleSets[0])
    i = docID

    s1 = docsAsShingleSets[docNames[i]]
    fp = []
    tp = []
    closeDoc = docID
    minDis = 0
    for j in range(0, len(docsAsShingleSets)):
        if j != i:
            s2 = docsAsShingleSets[docNames[j]]

            JSim[getTriangleIndex(i, j)] = (len(s1.intersection(s2)) / float(len(s1.union(s2))))
            percsimilarity = JSim[getTriangleIndex(i, j)] * 100
            if (percsimilarity > 0):
                if (percsimilarity >= minDis):
                    minDis = percsimilarity
                    closeDoc = j

    print("與第" + str(docID) + "篇文章最相似為: " + str(closeDoc))
    # task3.write(str(docID) + "," + str(closeDoc) + '\n')

# task3.close()
