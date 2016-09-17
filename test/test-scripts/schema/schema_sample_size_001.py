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
from os.path import dirname as dirname
import sys
# add /test to pythonpath so utils can be imported when running from spark
sys.path.append(dirname(dirname(dirname(__file__))))
import helpers.utils as utils

conf = utils.createSparkConf()
spark = SparkSession\
    .builder\
    .appName("Cloudant Spark SQL Example in Python using dataframes")\
    .config(conf=conf)\
    .getOrCreate()

print ('About to test com.cloudant.spark for n_customer with setting schemaSampleSize to 0')
spark.sql(" CREATE TEMPORARY TABLE customerTable USING com.cloudant.spark OPTIONS ( schemaSampleSize '0',database 'n_customer')")
customerData = spark.sql("SELECT * FROM customerTable")
customerData.printSchema()

	


	
	

