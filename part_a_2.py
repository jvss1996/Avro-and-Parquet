from pyspark import SparkConf, SparkContext
from operator import add
from pyspark.sql import SQLContext
from nltk.corpus import stopwords
import re
import string
import nltk
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession

conf = SparkConf()
conf.setAppName("Inverted Index")
conf.set("spark.ui.port", "4091")
conf.set("spark.executor.memory", "2g")
sc = SparkContext(conf = conf)
sqlContext = SQLContext(sc)
spark = SparkSession(sc)

path1=spark.read.load("/bigd45/out301/part-00000-a582686d-0690-4324-9aef-2d608120b7e8-c000.snappy.parquet")

removePunct=(lambda x:x not in string.punctuation)
path = "/cosc6339_hw2/gutenberg-500/"
finalWords=[]
out=path1.collect()
for(count,word) in out:
        out1 = word
        finalWords.append(out1)

rdd=sc.wholeTextFiles(path)
inverted1=rdd.map(lambda(x,y):(y,x))
inverted2=inverted1.map(lambda (x,y):(filter(removePunct,x),y))

def checkWords(c):
        if c in finalWords:
                return True
        else:
                return False
inverted3=inverted2.flatMap(lambda (x,y):(((i,y),float(1.0/(float(len(x.split()))))) for i in x.lower().split() if checkWords(i)))
inverted4=inverted3.reduceByKey(add,numPartitions=4)
inverted5=inverted4.map(lambda ((x,y),z):(x,(y,z)))
inverted6=inverted5.groupByKey()
inverted7=inverted6.mapValues(list)
inverted8 = inverted7.map(lambda (x,y):(x,y))
inverted8.saveAsTextFile("/bigd45/out401")