import sys
import pandas as pd
from pyspark.sql.functions import lit, split, col
assert sys.version_info >= (3, 8) # make sure we have Python 3.8+
from pyspark.sql import SparkSession, functions, types, Row
import string, re

wordbreak = r'[%s\s]+' % (re.escape(string.punctuation),)  # regex that matches spaces and/or punctuation

in_directory = 'wordcount-1'
spark = SparkSession.builder.appName('word count').getOrCreate()
assert spark.version >= '3.2' # make sure we have Spark 3.2+
spark.sparkContext.setLogLevel('WARN')


def main(in_directory, out_directory):
    lines = spark.read.text(in_directory)

    lines = lines.select(functions.split(lines['value'], wordbreak).alias('words'))
    lines = lines.select(functions.explode(lines['words']).alias('words'))
    lines = lines.select(functions.lower(lines['words']).alias('words'))
    lines = lines.groupBy('words').count()
    lines = lines.sort('count', 'words', ascending=[False, True])
    lines = lines.filter(lines['count']!=17881) 

    lines.write.csv(out_directory, compression=None, mode='overwrite')


if __name__=='__main__':
    in_directory = sys.argv[1]
    out_directory = sys.argv[2]
    spark = SparkSession.builder.appName('word count').getOrCreate()
    assert spark.version >= '3.2' # make sure we have Spark 3.2+
    spark.sparkContext.setLogLevel('WARN')

    main(in_directory, out_directory)