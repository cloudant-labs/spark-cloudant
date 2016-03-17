#*******************************************************************************
# Copyright (c) 2015 IBM Corp.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#******************************************************************************/
import pprint
from pyspark.sql import SQLContext
from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("Cloudant Spark SQL External Datasource in Python")
# define cloudant related configuration:
# set protocol to http if needed, default value = https
# conf.set("cloudant.protocol","http")
conf.set("cloudant.host","ACCOUNT.cloudant.com")
conf.set("cloudant.username", "USERNAME")
conf.set("cloudant.password", "PASSWORD")
conf.set("jsonstore.rdd.maxInPartition",1000)
conf.set("schemaSampleSize", 10)

sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

df = sqlContext.load("n_airportcodemapping", "com.cloudant.spark")

# In case of doing multiple operations on a dataframe (select, filter etc.)
# you should persist the dataframe.
# Othewise, every operation on the dataframe will load the same data from Cloudant again.
# Persisting will also speed up computation.
df.cache() # persisting in memory
# alternatively for large dbs to persist in memory & disk:
# from pyspark import StorageLevel
# df.persist(storageLevel = StorageLevel(True, True, False, True, 1)) 


df.printSchema()

df.filter(df.airportName >= 'Moscow').select("_id",'airportName').show()
df.filter(df._id >= 'CAA').select("_id",'airportName').show()
df.filter(df._id >= 'CAA').select("_id",'airportName').save("airportcodemapping_df", "com.cloudant.spark", bulkSize = "100")

df = sqlContext.load(source="com.cloudant.spark", database="n_flight")
df.printSchema()

total = df.filter(df.flightSegmentId >'AA9').select("flightSegmentId", "scheduledDepartureTime").orderBy(df.flightSegmentId).count()
print "Total", total, "flights from table"

df = sqlContext.load(source="com.cloudant.spark", database="n_flight", index="_design/view/_search/n_flights")
df.printSchema()

total = df.filter(df.flightSegmentId >'AA9').select("flightSegmentId", "scheduledDepartureTime").orderBy(df.flightSegmentId).count()
print "Total", total, "flights from index"

# Loading data from views
df = sqlContext.load(source="com.cloudant.spark", path="movies-glynn", view="_design/view1/_view/titleyear2")
df.printSchema()
df.filter(df.value.year >= 1950).select(df.value.title.alias("title"), df.value.year.alias("year")).show()

sc.stop()
