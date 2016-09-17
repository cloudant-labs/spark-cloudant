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

def verifyRegCharPredicate():
	airportData = spark.sql("SELECT _id, airportName FROM airportTable WHERE airportName >= 'Paris' ORDER BY airportName")
	airportData.printSchema()
	print ('Total # of rows in airportData: ' + str(airportData.count()))

	# verify expected count
	assert airportData.count() == 6

	# verify >= 'Paris' ORDER BY airportName
	previous_name = ''
	for name in airportData.collect():
		print (name.airportName)
		assert name.airportName >= 'Paris'
		assert name.airportName >= previous_name
		previous_name = name.airportName

			
print ('About to test com.cloudant.spark for n_airportcodemapping')
spark.sql("CREATE TEMPORARY TABLE airportTable USING com.cloudant.spark OPTIONS ( database 'n_airportcodemapping')")
verifyRegCharPredicate()