import sys
import json

from random import random
from operator import add

from pyspark.sql import SparkSession


if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("GitHub parse")\
        .getOrCreate()

    sc = spark.sparkContext

    path = "data/2017-01-01-*.json"

    events =  sc.textFile(path)
    
    def toJSON(line):
        return json.loads(line)
    
    events = events.map(toJSON)
    events = events.map(lambda table: return [table['repo']['id'], 1])
    """ Comptage du nombre de clé """
    events = events.reduceByKey(lambda a, b: a + b)
    events = events.sortBy(False, lambda x: x[1])
    top20 = events.collect()[20][1]
    events = events.filter(lambda x: x[1] > top20) 
    events.collect()
    
    spark.stop()
