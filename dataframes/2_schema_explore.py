import sys
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, functions, types
from pyspark.sql import types
import datetime as dt

assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

conf = SparkConf().setAppName('example code')
sc = SparkContext(conf=conf)
spark = SparkSession.builder.appName('example code').getOrCreate()
assert spark.version >= '2.3' # make sure we have Spark 2.3+


####################################### Specifying schema of dataframe

#### Infer schema

simple_rdd = sc.parallelize([
      ['2017-02-01','Rachel', 19, 156, 'Sydney']
    , ['2018-01-01','Albert',  3,  45, 'New York']
    , ['2018-03-02','Jack',   61, 190, 'Krakow']
    , ['2017-12-31','Skye',    8,  82, 'Harbin']
])

simple_df = spark.createDataFrame(simple_rdd)                       # optionally can give colum names
#simple_df.printSchema()                                             # will print inferred schema. date is wrongly shown as string






#### Specify schema : faster

schema = [
      ('Date', types.DateType())
    , ('Name', types.StringType())
    , ('Age',  types.IntegerType())
    , ('Weight', types.IntegerType())
    , ('Location', types.StringType())
]

schema = types.StructType([types.StructField(e[0],e[1], False) for e in schema])

simple_df_schema = spark.createDataFrame(
      simple_rdd
        .map(lambda row:
             [dt.datetime.strptime(row[0], '%Y-%m-%d')] + row[1:]
            )
    , schema=schema
)

#simple_df_schema = spark.createDataFrame(simple_rdd,schema=schema)

#simple_df_schema.show(n=5)

# specifying schema for weather
schema_weather = [
    ('station',types.StringType())
    , ('date',types.StringType())
    , ('observation',types.StringType())
    , ('value',types.IntegerType())
    , ('mflag',types.StringType())
    , ('qflag',types.StringType())
    , ('sflag',types.StringType())
    , ('obstime',types.StringType())]

schema_weather = types.StructType([types.StructField(e[0],e[1], False) for e in schema_weather])

weather = spark.read.csv('weather.csv', schema=schema_weather)

weather.show(n=10)
