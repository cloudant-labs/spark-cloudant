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
import utils
import os

class TestCloudantSparkConnector:

	script_dir = "test-scripts/cloudantapp"
	
	def test_SpecCharPredicate(self, sparksubmit, tmpdir, test_properties):
		script_name = "SpecCharPredicate.py"
		utils.run_test(self.get_script_path(script_name), script_name, tmpdir, sparksubmit, test_properties)
		
	def test_RegCharPredicate(self, sparksubmit, tmpdir, test_properties):
		script_name = "RegCharPredicate.py"
		utils.run_test(self.get_script_path(script_name), script_name, tmpdir, sparksubmit, test_properties)
		
	def test_RangePredicate(self, sparksubmit, tmpdir, test_properties):
		script_name = "RangePredicate.py"
		utils.run_test(self.get_script_path(script_name), script_name, tmpdir, sparksubmit, test_properties)	

	def test_SpecCharValuePredicate(self, sparksubmit, tmpdir, test_properties):
		script_name = "SpecCharValuePredicate.py"
		utils.run_test(self.get_script_path(script_name), script_name, tmpdir, sparksubmit, test_properties)

	def test_IndexOption(self, sparksubmit, tmpdir, test_properties):
		script_name = "IndexOption.py"
		utils.run_test(self.get_script_path(script_name), script_name, tmpdir, sparksubmit, test_properties)
		
		
		
	def get_script_path(self, script_name):
		return os.path.join(os.path.dirname(__file__), self.script_dir, script_name)
			

		

