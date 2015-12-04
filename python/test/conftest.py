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
sys.path.append(os.path.join(os.path.dirname(__file__), "helpers"))


@pytest.fixture(scope="session", autouse=True)
def test_properties():
	properties = {
	'cloudanthost':'281053d5-a6e9-4c13-943e-05f70f25bf70-bluemix.cloudant.com', 
	'cloudantusername':'stromentemonstallostitya', 
	'cloudantpassword':'6a9c346d6aea58ddd4f9ee92bb9758491c44d54f'}
	
# 	properties = {
# 	'cloudanthost':'ACCOUNT.cloudant.com', 
# 	'cloudantusername':'USERNAME', 
# 	'cloudantpassword':'PASSWORD'}
	
	return properties
	

@pytest.fixture(scope="session", autouse=True)
def check_db_availability(test_properties):
	test_dbs = [
		"n_airportcodemapping",
		"n_booking",
		"n_customer",
		"n_customersession",
		"n_flight",
		"n_flightsegment",
		"airportcodemapping_df"
	]
	for db in test_dbs:
		print ("Checking connection to cloudant databases: {}".format(db))
		url = "https://{}/{}".format(
				test_properties["cloudanthost"], db)
		response = requests.get(url, auth=(test_properties["cloudantusername"], test_properties["cloudantpassword"]))
		assert response.status_code == 200


@pytest.fixture(scope="session", autouse=True)
def check_connector_jar():
	try:
		jarpath = os.environ["CONNECTOR_JAR"]
		if os.path.isfile(jarpath):
			print ("Cloudant-Spark Connector path: ", jarpath)
		else:
			print("Invalid Cloudant-Spark Connector path:", jarpath)
			sys.exit(1)			
	except KeyError:
		print("Environment variable CONNECTOR_JAR not set")
		sys.exit(1)


@pytest.fixture
def sparksubmit():
	try:
		sys.path.append(os.path.join(os.environ["SPARK_HOME"], "python"))
		sys.path.append(os.path.join(os.environ["SPARK_HOME"], "python", "lib", "py4j-0.8.2.1-src.zip"))
	except KeyError:
		print("Environment variable SPARK_HOME not set")
		sys.exit(1)
		
	return os.path.join(os.environ.get("SPARK_HOME"), "bin", "spark-submit")

	


	
