# *******************************************************************************
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
# ******************************************************************************/
import os
from helpers.dbutils import CloudantDbUtils
import conftest
from helpers import utils

# get the cloudant credentials from pytest config file
test_properties = conftest.test_properties()

# util class with schema tests turned on
util = utils.Utils(os.path.dirname(__file__), True)

class TestCloudantSparkConnector:


    def test_001(self, sparksubmit):
        script_name = "schema_sample_size_001.py"
        returncode, out, err = util.run_test(util.get_script_path(script_name), sparksubmit)
        assert returncode == 1
        print(err)
        err = err[err.index("java.lang.RuntimeException"):]
        err = err[:err.index("\n")]
        assert err == "java.lang.RuntimeException: Database n_customer schema sample size is 0!"

    def test_002(self, sparksubmit):
        script_name = "schema_sample_size_002.py"
        returncode, out, err = util.run_test(util.get_script_path(script_name), sparksubmit)
        assert returncode == 1
        print(err)
        err = err[err.index("java.lang.NumberFormatException"):]
        err = err[:err.index("\n")]
        assert err == "java.lang.NumberFormatException: For input string: \"str\""

    def test_003(self, sparksubmit):
        script_name = "schema_sample_size_003.py"
        returncode, out, err = util.run_test(util.get_script_path(script_name), sparksubmit)
        cloudantUtils = CloudantDbUtils(test_properties)
        if cloudantUtils.check_if_doc_exists("n_customer", "zzzzzzzzzzzzzz"):
            print(err)
            assert err.index("org.apache.spark.SparkException") > 0
            assert returncode == 1
        else:
            assert returncode == 0

    def test_004(self, sparksubmit):
        script_name = "schema_sample_size_004.py"
        returncode, out, err = util.run_test(util.get_script_path(script_name), sparksubmit)
        assert returncode == 0

    def test_005(self, sparksubmit):
        script_name = "schema_sample_size_005.py"
        returncode, out, err = util.run_test(util.get_script_path(script_name), sparksubmit)
        cloudantUtils = CloudantDbUtils(test_properties)
        if cloudantUtils.check_if_doc_exists("n_customer", "zzzzzzzzzzzzzz"):
            print(err)
            assert err.index("org.apache.spark.SparkException") > 0
            assert returncode == 1
        else:
            assert returncode == 0
