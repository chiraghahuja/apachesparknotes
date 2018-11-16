import sys
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, functions, types
from pyspark.sql import types
import datetime as dt
from pyspark.sql import functions

assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

conf = SparkConf().setAppName('example code')
sc = SparkContext(conf=conf)
spark = SparkSession.builder.appName('example code').getOrCreate()
assert spark.version >= '2.3' # make sure we have Spark 2.3+


#######################aggregate transofrmation : reduce scalar in rdd

schema = [
      ('Date', types.StringType())
    , ('Region', types.StringType())
    , ('Rep',  types.StringType())
    , ('Item', types.StringType())
    , ('Units', types.IntegerType())
    , ('Unit Cost', types.DoubleType())
    , ('total', types.DoubleType())
]

schema = types.StructType([types.StructField(e[0],e[1], False) for e in schema])

sales_df = spark.read.csv('sales.csv',header=True)

sales_df.agg({'total':'avg'})#.show()

sales_df.agg({'total':'avg','Units': 'sum'}) #.show()
sales_df.show()

aggregations = [functions.min('Total').alias('Min Total'), functions.max('Total').alias('Max Total'),functions.avg('Total').alias('Average Total'), functions.stddev('Total').alias('Stddev Total')]

sales_df.agg(*aggregations).show()

sales_df.agg(functions.count("*")).show()