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
from pyspark.sql import SQLContext
from pyspark import SparkContext, SparkConf
from os.path import dirname as dirname
import sys
import requests
# add /test to pythonpath so utils can be imported when running from spark
sys.path.append(dirname(dirname(dirname(__file__))))
import helpers.utils as utils

conf = utils.createSparkConf()
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

def verify():
	customerData = sqlContext.sql("SELECT miles_ytd, total_miles FROM customerTable")
	customerData.printSchema()
	customerData.show(5)
	print(customerData.count())
	assert customerData.count() == doc_count

# query the index using Cloudant API to get expected count
test_properties = utils.get_test_properties()
url = url = "https://{}/{}".format(
			test_properties["cloudanthost"], 'n_customer')
print(url)
response = requests.get(url, auth=(test_properties["cloudantusername"], test_properties["cloudantpassword"]))
assert response.status_code == 200
doc_count = response.json().get("doc_count")

print ('About to test com.cloudant.spark.CloudantPartitionedPrunedFilteredRP for n_customer with setting schemaSampleSize to -1')
sqlContext.sql(" CREATE TEMPORARY TABLE customerTable USING com.cloudant.spark.CloudantPartitionedPrunedFilteredRP OPTIONS ( schemaSampleSize '-1',database 'n_customer')")
verify()

print ('About to test com.cloudant.spark.CloudantPrunedFilteredRP for n_customer with setting schemaSampleSize to -1')
sqlContext.sql(" CREATE TEMPORARY TABLE customerTable USING com.cloudant.spark.CloudantPrunedFilteredRP OPTIONS ( schemaSampleSize '-1',database 'n_customer')")
verify()

print ('About to test com.cloudant.spark for n_customer with setting schemaSampleSize to -1')
sqlContext.sql(" CREATE TEMPORARY TABLE customerTable USING com.cloudant.spark OPTIONS ( schemaSampleSize '-1',database 'n_customer')")
verify()

print ('About to test com.cloudant.spark.CloudantRP for n_customer with setting schemaSampleSize to -1')
sqlContext.sql(" CREATE TEMPORARY TABLE customerTable USING com.cloudant.spark.CloudantRP OPTIONS ( schemaSampleSize '-1',database 'n_customer')")
verify()

	


	
	

