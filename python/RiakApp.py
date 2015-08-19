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

conf = SparkConf().setAppName("Riak Spark SQL External Datasource in Python")
# define Riak related configuration
conf.set("riak.host","your host")
conf.set("riak.port", "your port")

sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

print 'About to test com.cloudant.spark.RiakPartitionedPrunedFilteredRP'
# Create a temp table 
sqlContext.sql("CREATE TEMPORARY TABLE animalTable USING com.cloudant.spark.riak.RiakPartitionedPrunedFilteredRP OPTIONS ( path 'famous')")
      
#data = sqlContext.sql("SELECT name_s , age_i FROM animalTable WHERE age_i>30") # hit class cast error between Integer and String
data = sqlContext.sql("SELECT name_s FROM animalTable WHERE name_s>'L'")
print '...before print schema 1'
data.printSchema()
print '...post print schema 1'
for code in data.collect():
	print code.name_s

