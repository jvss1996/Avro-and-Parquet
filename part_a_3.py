from pyspark import SparkConf,SparkContext
from operator import add
from pyspark.sql import SQLContext
import string
import nltk
from nltk.corpus import stopwords
import re
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession

conf=SparkConf()
conf.setAppName("Similarity Matrix")
conf.set("spark.executor.memory","2g")
conf.set("spark.ui.port","4098")
sc=SparkContext(conf=conf)
sqlContext = SQLContext(sc)
spark = SparkSession(sc)

inverted8 = sc.textFile("/bigd45/out307/").map(lambda x: eval(x))
inverted7 = inverted8.map(lambda (x,y):(x,y))

#similarity matrix
def func_similarity(inverted7):
        matrix=list()
        inverted7 = inverted7[1]
        if(len(inverted7) != 1):
                for a in range(len(inverted7)):
                        for b in range(a+1,len(inverted7)):
                                doc1_fraction=inverted7[a][1]
                                doc2_fraction=inverted7[b][1]
                                multiplication = ((inverted7[a][0], inverted7[b][0]), doc1_fraction*doc2_fraction)
                                matrix.append(multiplication)
        return matrix

sim_rdd = inverted7.map(func_similarity)
sim_rdd2 = sim_rdd.flatMap(lambda x:x)
sim_rdd3 = sim_rdd2.reduceByKey(add,numPartitions=4)
sim_rdd4 = sim_rdd3.map(lambda (x,y):(x,y))
sim_rdd4.saveAsTextFile("/bigd45/out332")