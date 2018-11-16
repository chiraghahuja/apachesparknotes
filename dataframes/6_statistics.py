from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, functions, types
from pyspark.sql import types
import sys
import datetime as dt
from pyspark.sql import functions

assert sys.version_info >= (3, 5)   # make sure we have Python 3.5+

conf = SparkConf().setAppName('example code')
sc = SparkContext(conf=conf)
spark = SparkSession.builder.appName('example code').getOrCreate()
assert spark.version >= '2.3'     # make sure we have Spark 2.3+


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

schema_sales = types.StructType([types.StructField(e[0],e[1], False) for e in schema])

sales_df = spark.read.csv('sales.csv',header=True,schema=schema_sales)

numeric_columns = [e[0]
         for e in sales_df.dtypes
         if e[1] in ('int', 'double')
        ]


sales_df.select(numeric_columns).describe().show()