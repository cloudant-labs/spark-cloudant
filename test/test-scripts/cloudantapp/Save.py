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
import time

conf = utils.createSparkConf()
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

def verifySave():
    airportData = sqlContext.sql("SELECT _id, airportName FROM airportTable")
    airportData.write.format("com.cloudant.spark") \
            .option("createDBOnSave", "false") \
            .save("n_airportcodemapping2")
	
		
    time.sleep(5)
    sqlContext.sql("CREATE TEMPORARY TABLE airportTable2 USING com.cloudant.spark OPTIONS ( database 'n_airportcodemapping2')")

    # verify that database was created, save and have the same count of data
    airportData2 = sqlContext.sql("SELECT _id, airportName FROM airportTable2")
    assert airportData2.count() == airportData.count()

			
print ('About to save test com.cloudant.spark for n_airportcodemapping')
sqlContext.sql("CREATE TEMPORARY TABLE airportTable USING com.cloudant.spark OPTIONS ( database 'n_airportcodemapping')")
verifySave()

sc.stop()
