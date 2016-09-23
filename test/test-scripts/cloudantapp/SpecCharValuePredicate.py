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
import warnings
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

def verifySpecCharValuePredicate():
	bookingData = spark.sql("SELECT customerId, dateOfBooking FROM bookingTable1 WHERE customerId = 'uid0@email.com'")
	bookingData.printSchema()

	# verify expected count
	print ("bookingData.count() = ", bookingData.count())
	assert bookingData.count() == total_rows

	# verify customerId = 'uid0@email.com'
	for booking in bookingData.collect():
		assert booking.customerId == 'uid0@email.com'


# query the index using Cloudant API to get expected count
test_properties = utils.get_test_properties()
url = "https://"+test_properties["cloudanthost"]+"/n_booking/_design/view/_search/n_bookings?q=customerId:uid0@email.com"
response = requests.get(url, auth=(test_properties["cloudantusername"], test_properties["cloudantpassword"]))
assert response.status_code == 200
total_rows = response.json().get("total_rows")

# record a warning if there is no data to test, will check for 0 doc anyway
if total_rows == 0:
	warnings.warn("No data for uid0@email.com in the n_booking database!")


print ('About to test com.cloudant.spark for n_booking')
spark.sql(" CREATE TEMPORARY TABLE bookingTable1 USING com.cloudant.spark OPTIONS ( database 'n_booking')")
verifySpecCharValuePredicate()