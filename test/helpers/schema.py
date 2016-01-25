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
import os
import json

from helpers.dbutils import CloudantDbUtils
import conftest

# get the cloudant credentials from pytest config file
test_properties = conftest.test_properties()


class DataLoader:
	"""
	Test data loader related functions
	"""

	def getInputJsonFilepath(self, db_name):
		return os.path.join("resources", "schema_data", "{}.json".format(db_name))
	
	def loadData(self):
		cloudantUtils = CloudantDbUtils(test_properties)
		for db in cloudantUtils.test_dbs:
			inputJsonFilePath = self.getInputJsonFilepath(db)
			if os.path.exists(inputJsonFilePath):
					if cloudantUtils.db_exists(db):
						with open(inputJsonFilePath, "r") as fh:
							bulkDocsJson = json.loads(fh.read())
							if cloudantUtils.bulk_insert(db, json.dumps(bulkDocsJson)):
								print("Successfully inserted the docs into {} from ./{}".format(db, inputJsonFilePath))
							else:
								print("Failed to insert the docs into {}".format(db))
					else:
						print("{} doesn't exist".format(db))

if  __name__ =='__main__':
	"""
	Utility for python spark-cloudant schema tests
	"""
	import argparse
	parser = argparse.ArgumentParser(description="Utility for python spark-cloudant schema tests")
	group = parser.add_mutually_exclusive_group(required=True)
	group.add_argument('-addDocs', action='store_true', help='Add required docs for spark-cloudant schema tests')
	
	args = parser.parse_args()

	dataloader = DataLoader()

	if args.addDocs:
		dataloader.loadData()

