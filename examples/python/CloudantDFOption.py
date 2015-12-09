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
# define coudant related configuration
conf.set("jsonstore.rdd.maxInPartition",1000)
conf.set("jsonstore.rdd.concurrentSave",2)
conf.set("jsonstore.rdd.bulkSize",10)

sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

cloudant_host = "ACCOUNT.cloudant.com"
cloudant_username = "USERNAME"
cloudant_password = "PASSWORD"

df = sqlContext.read.format("com.cloudant.spark").option("cloudant.host",cloudant_host).option("cloudant.username",cloudant_username).option("cloudant.password",cloudant_password).load("n_airportcodemapping")
df.printSchema()

df.filter(df._id >= 'CAA').select("_id",'airportName').show()
df.filter(df._id >= 'CAA').select("_id",'airportName').write.format("com.cloudant.spark").option("cloudant.host",cloudant_host).option("cloudant.username",cloudant_username).option("cloudant.password",cloudant_password).save("airportcodemapping_df")

df = sqlContext.read.format("com.cloudant.spark").option("cloudant.host",cloudant_host).option("cloudant.username",cloudant_username).option("cloudant.password",cloudant_password).load("n_flight")
df.printSchema()

total = df.filter(df.flightSegmentId >'AA9').select("flightSegmentId", "scheduledDepartureTime").orderBy(df.flightSegmentId).count()
print "Total", total, "flights from table"

df = sqlContext.read.format("com.cloudant.spark").option("cloudant.host",cloudant_host).option("cloudant.username",cloudant_username).option("cloudant.password",cloudant_password).option("index","_design/view/_search/n_flights").load("n_flight")
df.printSchema()

total = df.filter(df.flightSegmentId >'AA9').select("flightSegmentId", "scheduledDepartureTime").orderBy(df.flightSegmentId).count()
print "Total", total, "flights from index"
