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
import pytest
import requests
import sys
import os


@pytest.fixture(scope="session", autouse=True)
def test_properties():
	properties = {
	'cloudanthost':'ACCOUNT.cloudant.com', 
	'cloudantusername':'USERNAME', 
	'cloudantpassword':'PASSWORD'}
	
	return properties
	

@pytest.fixture(scope="session", autouse=True)
def setup_test_databases(test_properties):
	from helpers.dbutils import CloudantDbUtils
	
	cloudantUtils = CloudantDbUtils(test_properties)
	cloudantUtils.check_databases()


@pytest.fixture(scope="session", autouse=True)
def check_connector_jar():
	try:
		jarpath = os.environ["CONNECTOR_JAR"]
		if os.path.isfile(jarpath):
			print ("Cloudant-Spark Connector path: ", jarpath)
		else:
			raise RuntimeError("Invalid Cloudant-Spark Connector path:", jarpath)
				
	except KeyError:
		raise RuntimeError("Environment variable CONNECTOR_JAR not set")


@pytest.fixture
def sparksubmit():
	try:
		sys.path.append(os.path.join(os.environ["SPARK_HOME"], "python"))
		sys.path.append(os.path.join(os.environ["SPARK_HOME"], "python", "lib", "py4j-0.8.2.1-src.zip"))
		
	except KeyError:
		raise RuntimeError("Environment variable SPARK_HOME not set")
		
	return os.path.join(os.environ.get("SPARK_HOME"), "bin", "spark-submit")




	
