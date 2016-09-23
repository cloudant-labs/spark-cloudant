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
from pyspark.sql import SparkSession
import requests
import sys
from os.path import dirname as dirname
# add /test to pythonpath so utils can be imported when running from spark
sys.path.append(dirname(dirname(dirname(__file__))))
import helpers.utils as utils

conf = utils.createSparkConf()
spark = SparkSession\
    .builder\
    .appName("Cloudant Spark SQL Example in Python using dataframes")\
    .config(conf=conf)\
    .getOrCreate()

def verifyViewOption():
	flightData = spark.sql("SELECT key, value FROM flightTable1 WHERE key = 'AA0'")
	flightData.printSchema()

	# verify expected count
	print ("flightData.count() = ", flightData.count())
	assert flightData.count() == total_rows

	# verify key = "AA0'
	for flight in flightData.collect():
		print ('Flight {0} on {1}'.format(flight.key, flight.value))
		assert flight.key =='AA0'


# query the index using Cloudant API to get expected count
test_properties = utils.get_test_properties()
url = "https://"+test_properties["cloudanthost"]+"/n_flight/_design/view/_view/AA0"
response = requests.get(url, auth=(test_properties["cloudantusername"], test_properties["cloudantpassword"]))
assert response.status_code == 200
total_rows = response.json().get("total_rows")


print ('About to test com.cloudant.spark for n_flight with view')
spark.sql(" CREATE TEMPORARY TABLE flightTable1 USING com.cloudant.spark OPTIONS ( database 'n_flight', view '_design/view/_view/AA0')")
verifyViewOption()


	
	

