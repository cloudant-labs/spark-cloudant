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
import os
import sys
import requests
import subprocess
import signal
import time
import conftest
from helpers.dbutils import CloudantDbUtils

class AcmeAirUtils:
	"""
	Test AcmeAir app related functions
	"""

	_api_context_root = "/rest/api"
	_app_host = "http://localhost:9080"
	_test_properties = None
	FORM_CONTENT_TYPE = "application/x-www-form-urlencoded; charset=UTF-8"
	test_properties = conftest.test_properties()

	def __init__(self):
		try:
			acmeair_path = os.environ["ACMEAIR_HOME"]
			if os.path.exists(acmeair_path):
				print ("\nAcmeAir Nodejs app home: ", acmeair_path)
				self.acmehome = acmeair_path
			else:
				raise RuntimeError("Invalid AcmeAir Nodejs app home:", jarpath)
		except KeyError:
			raise RuntimeError("Environment variable ACMEAIR_HOME not set")
			
		if not all(x in ["cloudantusername", "cloudantpassword", "cloudanthost"] for x in self.test_properties):
			raise RuntimeError("test_properties does not contain all required cloudant properties")		
						
					
	def start_acmeair(self):
		"""
		Set the required env vars for cloudant and start the AcmeAir app locally
		If app is already running, check if it's functioning
		"""		
		app_status = self.is_acmeair_running();
		if app_status == -1:
			raise RuntimeError("AcmeAir is already running but malfunctioning.  Please shut it down.")
		
		elif app_status == 1:
			print ("AcmeAir app is already running, will not attempt to start\n")
			return
			
		cloudantUtils = CloudantDbUtils(self.test_properties)
		cloudantUtils.check_databases()
		
		# set the env vars required to start the app
		new_env = os.environ.copy()
		new_env["dbtype"] = "cloudant"
		new_env["CLOUDANT_URL"] = "https://{}:{}@{}".format(
			self.test_properties["cloudantusername"], 
			self.test_properties["cloudantpassword"],
			self.test_properties["cloudanthost"])
			
		# start the acmeair app
		os.chdir(self.acmehome)
		command = ["node", "app.js"]
		self.proc = subprocess.Popen(command, env=new_env, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
		
		# wait at most 10 sec for app to come up
		timeout = time.time() + 10
		while (True):		
			if not self.is_acmeair_running() == 1:
				time.sleep(1)
			else:
				print ("\nAcemAir started!")
				break
				
			if time.time() > timeout:
				raise RuntimeError("Cannot connect to AcmeAir!")
						
	
	def stop_acmeair(self):
		"""
		Stop the AcmeAir App that was started by this util
		"""	
		if hasattr(self, "proc"):
			os.kill(self.proc.pid, signal.SIGTERM)
			print ("AcmeAir is shutdown")
		
				
	def is_acmeair_running(self):
		"""
		Check if AcmeAir app is running
		Return 0: app is not running
		Return 1: app is running
		Return -1: app is running but malfunctioning, possibly because db were rebuilt
		when app was running 
		"""	
		r = requests.Session()
		url = "{}{}/{}".format(
				self._app_host, self._api_context_root, "config/countCustomers")
		try:
			response = r.get(url)
			if response.status_code == 200:
				status = 1
			else:
				# happens when db were rebuilt while app is running
				status = -1
		except:
			# happens when app is not running
			status = 0
				
		return status
		
	
	def load_data(self, num_of_customers):
		"""
		Call the AcmeAir app REST API to populate with the given # of users
		"""
		cloudantUtils = CloudantDbUtils(self.test_properties)
		if cloudantUtils.get_doc_count("n_customer") > 0:
			raise RuntimeException("Test databases already contains unknown data so AcmeAir data loader will not run!")
	
		r = requests.Session()
		url = "{}{}/{}".format(
				self._app_host, self._api_context_root, "loader/load")

		if isinstance(num_of_customers, int):
			num_of_customers = str(num_of_customers)
		param = {"numCustomers" : num_of_customers}
		headers = { "Content-Type" : self.FORM_CONTENT_TYPE}
		
		print ("Start AcmeAir database loader with num_of_customers = ", num_of_customers)
		start_time = time.time()
		try:
			r.get(url, headers=headers, params=param)
			
		except requests.exceptions.ConnectionError:
			# the request aborts after 2 mins due to server timeout, wait until expected # of rows is reached
			cloudantUtils.wait_for_doc_count("n_customer", num_of_customers, int(num_of_customers) / 500)
			cloudantUtils.wait_for_doc_count("n_airportcodemapping", 14, 5)			
			cloudantUtils.wait_for_doc_count("n_flightsegment", 395, 5)
			cloudantUtils.wait_for_doc_count("n_flight", 1971, 20)
			print ("Database load completed after {} mins\n".format(int(round((time.time() - start_time) / 60))))
			
			
	def login(self, user):
		"""
		Create a user session for the given user. It's needed in order to book a flight.
		@return session ID
		"""
		r = requests.Session()		
		url = "{}{}/{}".format(
				self._app_host, self._api_context_root, "login")
		payload = {"login" : user, "password" : "password"}
		headers = { "Content-Type" : self.FORM_CONTENT_TYPE}		
						
		print ("Login as user: ", user)
		response = r.post(url, headers=headers, data=payload)
		if response.status_code == 200:
			# get session ID
			return response.cookies["sessionid"]
		else:
			raise RuntimeError(response.text)
	

	def book_flights(self, user, toFlightId, retFlightId):
		"""
		Login as the given user and booking flights.
		Set retFlightId=None if booking one way.
		"""		
		r = requests.Session()		
		url = "{}{}/{}".format(
				self._app_host, self._api_context_root, "bookings/bookflights")
		headers = { "Content-Type" : self.FORM_CONTENT_TYPE }
		payload = {"userid" : user, "toFlightId" : toFlightId}
		
		# see if it's round trip
		if retFlightId is None:
			payload["oneWayFlight"] = "true"
		else:
			payload["oneWayFlight"] = "false"
			payload["retFlightId"] = retFlightId
	
		# the request must include the cookie retrieved from login
		sessionId = self.login(user)	
		cookies = { "sessionid" : sessionId,
					"loggedinuser" : user } 
			
		print ("Book flight(s) " + str(payload))
		response = r.post(url, headers=headers, data=payload, cookies=cookies)
		if response.status_code == 200:
			print ("\nFlight(s) booked: {}\n".format(response.text))
		else:
			raise RuntimeError(response.text)
			
			
	def get_flightId_by_number(self, flightNum):
		"""
		Get the generated flight ID for the given flight number
		"""
		cloudantUtils = CloudantDbUtils(self.test_properties)
		
		url = "https://{}/{}".format(
				self.test_properties["cloudanthost"], "n_flight/_design/view/_search/n_flights?q=flightSegmentId:" + flightNum)
		param = {"q" : "flightSegmentId:" + flightNum}		
				
		response = cloudantUtils.r.get(url, params=param)
		data = response.json()
		if int(data["total_rows"]) > 0:
			# just get one from the dict
			return data["rows"][0]["id"]
		else:
			raise RuntimeError("n_flights has no data for ", flightNum)	
			
			
		
	