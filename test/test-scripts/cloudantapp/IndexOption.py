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
import requests
import sys
from os.path import dirname as dirname
# add /test to pythonpath so utils can be imported when running from spark
sys.path.append(dirname(dirname(dirname(__file__))))
import helpers.utils as utils

conf = utils.createSparkConf()
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

def verifyIndexOption():
	flightData = sqlContext.sql("SELECT flightSegmentId, scheduledDepartureTime FROM flightTable1 WHERE flightSegmentId >'AA9' AND flightSegmentId < 'AA95'")
	flightData.printSchema()

	# verify expected count
	print ("flightData.count() = ", flightData.count())
	assert flightData.count() == total_rows

	# verify flightSegmentId >'AA9' AND flightSegmentId<'AA95'
	for flight in flightData.collect():
		print ('Flight {0} on {1}'.format(flight.flightSegmentId, flight.scheduledDepartureTime))
		assert flight.flightSegmentId >'AA9' and flight.flightSegmentId <'AA95' 


# query the index using Cloudant API to get expected count
test_properties = utils.get_test_properties()
url = "https://"+test_properties["cloudanthost"]+"/n_flight/_design/view/_search/n_flights?q=flightSegmentId:{AA9%20TO%20AA95}"
response = requests.get(url, auth=(test_properties["cloudantusername"], test_properties["cloudantpassword"]))
assert response.status_code == 200
total_rows = response.json().get("total_rows")


print ('About to test com.cloudant.spark for n_flight with index')
sqlContext.sql(" CREATE TEMPORARY TABLE flightTable1 USING com.cloudant.spark OPTIONS ( database 'n_flight', index '_design/view/_search/n_flights')")
verifyIndexOption()
