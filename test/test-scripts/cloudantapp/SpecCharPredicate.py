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
import sys
from os.path import dirname as dirname
# add /test to pythonpath so utils can be imported when running from spark
sys.path.append(dirname(dirname(dirname(__file__))))
import helpers.utils as utils

conf = utils.createSparkConf()
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

def verifySpecCharPredicate():
	airportData = sqlContext.sql("SELECT _id, airportName FROM airportTable WHERE _id >= 'CAA' ORDER BY _id")
	airportData.printSchema()
	print ('Total # of rows in airportData: ' + str(airportData.count()))

	# verify expected count
	assert airportData.count() == 13

	# verify >= 'CAA' ORDER BY _id
	previous_id = ''
	for code in airportData.collect():
		print (code._id)
		assert code._id >= 'CAA'
		assert code._id >= previous_id
		previous_id = code._id

			
print ('About to test com.cloudant.spark for n_airportcodemapping')
sqlContext.sql("CREATE TEMPORARY TABLE airportTable USING com.cloudant.spark OPTIONS ( database 'n_airportcodemapping')")
verifySpecCharPredicate()
