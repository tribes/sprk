import sys
from random import random
from operator import add

from pyspark.sql import SparkSession


if __name__ == "__main__":
    """
        Usage: pi [partitions]
    """
    spark = SparkSession\
        .builder\
        .appName("GitHub parse")\
        .getOrCreate()

    sc = spark.sparkContext

    path = "data/2017-01-01-*.json"

    eventDF = spark.read.json(path)

    eventDF.createOrReplaceTempView("repo")

    countEventsByRepoDF = spark.sql("select repo.id, repo.name, count(*) as count from repo where repo.id is not null group by repo.id,repo.name order by count desc")

    countEventsByRepoDF.show()
    
    spark.stop()
