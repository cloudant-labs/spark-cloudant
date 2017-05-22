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

from helpers import utils

util = utils.Utils(os.path.dirname(__file__))

class TestCloudantSparkConnector:

    def test_SpecCharPredicate(self, sparksubmit):
        script_name = "SpecCharPredicate.py"
        util.run_test(util.get_script_path(script_name), sparksubmit)
        
    def test_RegCharPredicate(self, sparksubmit):
        script_name = "RegCharPredicate.py"
        util.run_test(util.get_script_path(script_name), sparksubmit)
        
    def test_RangePredicate(self, sparksubmit):
        script_name = "RangePredicate.py"
        util.run_test(util.get_script_path(script_name), sparksubmit)

    def test_SpecCharValuePredicate(self, sparksubmit):
        script_name = "SpecCharValuePredicate.py"
        util.run_test(util.get_script_path(script_name), sparksubmit)

    def test_IndexOption(self, sparksubmit):
        script_name = "IndexOption.py"
        util.run_test(util.get_script_path(script_name), sparksubmit)
    
    def test_ViewOption(self, sparksubmit):
        script_name = "ViewOption.py"
        util.run_test(util.get_script_path(script_name), sparksubmit)
    
    def test_Save(self, sparksubmit):
        script_name = "Save.py"
        util.run_test(util.get_script_path(script_name), sparksubmit)
            

        

