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
# set protocol to http if needed, default value=https
# conf.set("cloudant.protocol","http")
conf.set("cloudant.host","ACCOUNT.cloudant.com")
conf.set("cloudant.username", "USERNAME")
conf.set("cloudant.password","PASSWORD")

sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

print 'About to test com.cloudant.spark for n_airportcodemapping'
sqlContext.sql("CREATE TEMPORARY TABLE airportTable USING com.cloudant.spark OPTIONS ( database 'n_airportcodemapping')")
      
airportData = sqlContext.sql("SELECT _id, airportName FROM airportTable WHERE _id >= 'CAA' ORDER BY _id")
airportData.printSchema()
print 'Total # of rows in airportData: ' + str(airportData.count())
for code in airportData.collect():
	print code._id

print 'About to test com.cloudant.spark for n_booking'
sqlContext.sql(" CREATE TEMPORARY TABLE bookingTable USING com.cloudant.spark OPTIONS ( database 'n_booking' , jsonstore.rdd.schemaSampleSize '-1' )")
      
bookingData = sqlContext.sql("SELECT customerId, dateOfBooking FROM bookingTable WHERE customerId = 'uid0@email.com'") 
bookingData.printSchema()
for code in bookingData.collect():
	print code.customerId
	print code.dateOfBooking


print 'About to test com.cloudant.spark for n_airportcodemapping'
sqlContext.sql(" CREATE TEMPORARY TABLE airportTable1 USING com.cloudant.spark OPTIONS ( database 'n_airportcodemapping')")
      
airportData = sqlContext.sql("SELECT _id, airportName FROM airportTable1 WHERE _id >= 'CAA' AND _id <= 'GAA' ORDER BY _id")
airportData.printSchema()
print 'Total # of rows in airportData: ' + str(airportData.count())
for code in airportData.collect():
	print code._id

print 'About to test com.cloudant.spark for n_booking'
sqlContext.sql(" CREATE TEMPORARY TABLE bookingTable1 USING com.cloudant.spark OPTIONS ( database 'n_booking')")
      
bookingData = sqlContext.sql("SELECT customerId, dateOfBooking FROM bookingTable1 WHERE customerId = 'uid0@email.com'")
bookingData.printSchema()
for code in bookingData.collect():
	print code.customerId
	print code.dateOfBooking

print 'About to test com.cloudant.spark for flight with index'
sqlContext.sql(" CREATE TEMPORARY TABLE flightTable1 USING com.cloudant.spark OPTIONS ( database 'n_flight', index '_design/view/_search/n_flights')")
      
flightData = sqlContext.sql("SELECT flightSegmentId, scheduledDepartureTime FROM flightTable1 WHERE flightSegmentId >'AA9' AND flightSegmentId<'AA95'")
flightData.printSchema()
for code in flightData.collect():
	print 'Flight {0} on {1}'.format(code.flightSegmentId, code.scheduledDepartureTime)
		

print 'About to test com.cloudant.spark for n_airportcodemapping'
sqlContext.sql(" CREATE TEMPORARY TABLE airportTable2 USING com.cloudant.spark OPTIONS ( database 'n_airportcodemapping')")
      
airportData = sqlContext.sql("SELECT _id, airportName FROM airportTable2 WHERE _id >= 'CAA' AND _id <= 'GAA' ORDER BY _id")
airportData.printSchema()
for code in airportData.collect():
	print code._id

print 'About to test com.cloudant.spark for n_booking'
sqlContext.sql(" CREATE TEMPORARY TABLE bookingTable2 USING com.cloudant.spark OPTIONS ( database 'n_booking')")
      
bookingData = sqlContext.sql("SELECT customerId, dateOfBooking FROM bookingTable2 WHERE customerId = 'uid0@email.com'")
bookingData.printSchema()
for code in bookingData.collect():
	print 'Booking for {0} on {1}'.format(code.customerId,code.dateOfBooking)

print 'About to test com.cloudant.spark for flight with index'
sqlContext.sql(" CREATE TEMPORARY TABLE flightTable2 USING com.cloudant.spark OPTIONS ( database 'n_flight', index '_design/view/_search/n_flights')")
      
flightData = sqlContext.sql("SELECT flightSegmentId, scheduledDepartureTime FROM flightTable2 WHERE flightSegmentId >'AA9' AND flightSegmentId<'AA95'")
flightData.printSchema()
for code in flightData.collect():
	print 'Flight {0} on {1}'.format(code.flightSegmentId, code.scheduledDepartureTime)

sc.stop()
