import sys
from pyspark.sql import SparkSession, functions, types

spark = SparkSession.builder.appName('reddit averages').getOrCreate()
spark.sparkContext.setLogLevel('WARN')

assert sys.version_info >= (3, 8) # make sure we have Python 3.8+
assert spark.version >= '3.2' # make sure we have Spark 3.2+


pages_schema = types.StructType([
    types.StructField('language', types.StringType()),
    types.StructField('title', types.StringType()),
    types.StructField('requests', types.LongType()),
    types.StructField('bytes', types.LongType())
])

def filter_path(path):
    path = path.removesuffix('.gz')
    path = path[-15:]
    path = path[0:-4]
    return path

def main(in_directory, out_directory):
    comments = spark.read.json(in_directory, schema=comments_schema)


def main(in_directory, out_directory):
    pages = spark.read.csv(in_directory, sep=' ', schema=pages_schema).withColumn('filename', functions.input_file_name())

    # filter
    pages = pages.filter(pages.language=='en')
    pages = pages.filter(pages.title!='Main_Page')
    pages = pages.filter(~pages.title.contains('Special:'))

    pages = pages.cache()

    path_to_hour = functions.udf(filter_path, returnType = types.StringType())
    pages = pages.withColumn('hour', path_to_hour(pages.filename))  

    max_requests = pages.groupBy(pages['hour']).max('requests')

    pages = pages.join(max_requests, 'hour')
    pages = pages.filter(pages['requests'] == pages['max(requests)'])
    pages = pages.orderBy(functions.col('hour'), functions.col('title'), ascending=True)

    result = pages.select(
        pages['hour'],
        pages['title'],
        pages['max(requests)']
    )
    result.write.csv(out_directory, compression=None, mode='overwrite')



  


if __name__=='__main__':
    in_directory = sys.argv[1]
    out_directory = sys.argv[2]
    main(in_directory, out_directory)
