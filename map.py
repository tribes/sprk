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

    path = "data/*.json"

    events =  sc.textFile(path)
    
    def toJSON(line):
        return json.loads(line)
    
    events = events.map(toJSON).filter(lambda event: 'repo' in event)
    events = events.map(lambda table: (table['repo']['id'], 1))
    # Comptage du nombre de cle
    events = events.reduceByKey(lambda a, b: a + b)
    events = events.sortBy(ascending=False, keyfunc=lambda x: x[1])
    print("Top repositories:")
    print("Repository ID | Occurences")
    for event in events.collect()[:20]:
        print("%13d | %.10d" % event)
    
    spark.stop()
