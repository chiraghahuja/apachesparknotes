import sys
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, functions, types

assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

conf = SparkConf().setAppName('example code')
sc = SparkContext(conf=conf)
spark = SparkSession.builder.appName('example code').getOrCreate()
assert spark.version >= '2.3' # make sure we have Spark 2.3+

############################################1. from rdd. give rdd and column names as list

simple_rdd = sc.parallelize([
      ['2017-02-01','Rachel', 19, 156, 'Sydney']
    , ['2018-01-01','Albert',  3,  45, 'New York']
    , ['2018-03-02','Jack',   61, 190, 'Krakow']
    , ['2017-12-31','Skye',    8,  82, 'Harbin']
])

simple_df = spark.createDataFrame(simple_rdd,['Date','Name','Age','Weight','Location'])      #spark.createdataFrame(rdd,columnListName)

#simple_df.show()                                                                            # show is printing the dataframe in tabular format

#print(simple_df.take(2))                                                                     # take will return df as python list of rows


##############################2. from list of json_strings : use spark.read.json

json_stringlist = [
    '{"Date":"2017-02-01","Name":"Rachel","Age":19,"Weight":156,"Location":"Sydney"}',
    '{"Date":"2018-01-01","Name":"Albert","Age":3 ,"Weight":45 ,"Location":"New York"}',
    '{"Date":"2018-03-02","Name":"Jack"  ,"Age":61,"Weight":190,"Location":"Krakow"}',
    '{"Date":"2017-12-31","Name":"Skye"  ,"Age":8 ,"Weight":82 ,"Location":"Harbin"}'
]

simple_df_json = spark.read.json(sc.parallelize(json_stringlist))        # read rdd where each element is a json string
#simple_df_json.show()                                                   # show in tabular form. order columns alphabetically (can modify order also)

##########################3. from csv

simple_df_csv = spark.read.csv('sample_data.csv',header=True)           # give path to csv file. header for reading first row as column names
simple_df_csv.show(n=5)                                                 # show in tabluar form

###################4. parquet : just give path to data in the function spark.read.parquet()