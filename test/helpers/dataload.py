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
import requests
import sys
import os
import json

from helpers.dbutils import CloudantDbUtils
from helpers.acmeair_utils import AcmeAirUtils
import conftest

# get the cloudant credentials from pytest config file
test_properties = conftest.test_properties()


class DataLoader:
	"""
	Test data loader related functions
	"""
	
	def load_AcmeData(self, num_of_cust):
		"""
		Reset databases and use the AcmeAir database loader to populate initial customer,
		flight and airportmapping data. Does NOT generate user data like bookings.
		"""	
		print ("num_of_cust: ", num_of_cust)
			
		acmeair = AcmeAirUtils()	
		try:
			if acmeair.is_acmeair_running() != 0:
				raise RuntimeError("""
				AcmeAir is already running which may cause unexpected results when
				resetting databases.  Please shut down the app and try again.
				""")
			else:
				cloudantUtils = CloudantDbUtils(test_properties)
				cloudantUtils.reset_databases()
				acmeair.start_acmeair()
				acmeair.load_data(num_of_cust)
		
		finally:
			acmeair.stop_acmeair()
			
			
	def remove_AcmeDb(self, num_of_cust):
		"""
		Drop all AcmeAir databases
		"""
		acmeair = AcmeAirUtils()
		if acmeair.is_acmeair_running() != 0:
			acmeair.stop_acmeair()
			
		cloudantUtils = CloudantDbUtils(test_properties)
		cloudantUtils.drop_all_databases()		
	
	
	def load_SpecCharValuePredicateData(self):
		"""
		Create booking data needed to test SpecCharValuePredicate
		"""
		try:
			acmeair = AcmeAirUtils()
			acmeair.start_acmeair()
			
			# book flights AA93 and AA330
			flight1 = "AA93"
			flight2 = "AA330"
			
			# Step#1 - need to find the flights generated _id required for booking
			flight1_id = acmeair.get_flightId_by_number(flight1)
			print ("{} id = {}".format(flight1, flight1_id))
			
			flight2_id = acmeair.get_flightId_by_number(flight2)
			print ("{} id = {}".format(flight2, flight2_id))
			
			# Step#2 - add the boooking
			acmeair.book_flights("uid0@email.com", flight1, flight2)

		finally:
			acmeair.stop_acmeair()
	
	
if  __name__ =='__main__':
	"""
	Utility to create test databases and load data
	"""
	import argparse
	parser = argparse.ArgumentParser(description="Utility to load AcmeAir data required for python spark-cloudant tests")
	group = parser.add_mutually_exclusive_group(required=True)
	group.add_argument('-cleanup', action='store_true', help='Drop all test databases')
	group.add_argument('-load', help='Reset and Load databases with the given # of users. -load 0 to just recreate databases and indexes.', type=int)
	args = parser.parse_args()

	dataloader = DataLoader()
	
	if args.load is not None:
		if args.load == 0:
			cloudantUtils = CloudantDbUtils(test_properties)
			cloudantUtils.reset_databases()
		else:			
			dataloader.load_AcmeData(args.load)
			dataloader.load_SpecCharValuePredicateData()
		
	elif args.cleanup:
		cloudantUtils = CloudantDbUtils(test_properties)
		cloudantUtils.drop_all_databases()

