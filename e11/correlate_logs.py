import sys
import pandas as pd
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import lit
import math
assert sys.version_info >= (3, 8) # make sure we have Python 3.8+
from pyspark.sql import SparkSession, functions, types, Row
import re


line_re = re.compile(r"^(\S+) - - \[\S+ [+-]\d+\] \"[A-Z]+ \S+ HTTP/\d\.\d\" \d+ (\d+)$")


def line_to_row(line):
    """
    Take a logfile line and return a Row object with hostname and bytes transferred.
    Return None if regex doesn't match.
    """
    m = line_re.match(line)
    if m:
        # TODO
        return Row(hostname=m.group(1), bytes=m.group(2))

    else:
        return None


def not_none(row):
    """
    Is this None? Hint: .filter() with it.
    """
    return row is not None


def create_row_rdd(in_directory):
    log_lines = spark.sparkContext.textFile(in_directory + "/*")
    
    # TODO: return an RDD of Row() objects
    rows = log_lines.map(lambda x: line_to_row(x))
    rows = rows.filter(lambda x: not_none(x))
    return rows


def main(in_directory):
    logs = spark.createDataFrame(create_row_rdd(in_directory))

    # TODO: calculate r.

    logs = logs.withColumn('bytes', logs['bytes'].cast(IntegerType()))
    logs = logs.groupby('hostname').agg(functions.count('hostname').alias('requests'),
                                        functions.sum('bytes').alias('total_bytes')).cache()
    logs = logs.withColumn("requests^2", logs['requests']*logs['requests'])
    logs = logs.withColumn("total_bytes^2", logs['total_bytes']*logs['total_bytes'])
    logs = logs.withColumn("requests*total_bytes", logs['requests']*logs['total_bytes'])
    logs = logs.withColumn("datapoints", lit(1))

    sums = logs.groupBy().sum().cache()

    n = sums.first()['sum(datapoints)']
    x = sums.first()['sum(requests)']
    y = sums.first()['sum(total_bytes)']
    x2 = sums.first()['sum(requests^2)']
    y2 = sums.first()['sum(total_bytes^2)']
    xy = sums.first()['sum(requests*total_bytes)']


    r = (n*xy-x*y)/(math.sqrt(n*x2-x*x)*math.sqrt(n*y2-y*y)) # TODO: it isn't zero.
    print(f"r = {r}\nr^2 = {r*r}")
    # Built-in function should get the same results.
    #print(totals.corr('count', 'bytes'))


if __name__=='__main__':
    in_directory = sys.argv[1]
    spark = SparkSession.builder.appName('correlate logs').getOrCreate()
    assert spark.version >= '3.2' # make sure we have Spark 3.2+
    spark.sparkContext.setLogLevel('WARN')

    main(in_directory)
