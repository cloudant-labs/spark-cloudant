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
import pytest
import subprocess
import os
import conftest

app_script_dir = 'test-scripts/cloudantapp'
schema_script_dir = 'test-scripts/schema'
script_dir = None


class Utils(object):
    def __init__(self, test_scripts_path=None, run_schema_tests=False):
        self.scripts_path = test_scripts_path
        self.schema_tests = run_schema_tests
        super(Utils, self).__init__()
        if self.schema_tests:
            self.script_dir = schema_script_dir
        else:
            self.script_dir = app_script_dir

    def get_script_path(self, script_name):
        print(os.path.dirname(__file__))
        print(self.script_dir)

        return os.path.join(self.scripts_path, self.script_dir, script_name)

    def run_test(self, in_script, sparksubmit):
        __tracebackhide__ = True

        # spark-submit the script
        import os, sys
        command = [sparksubmit]
        command.extend(["--master", "local[4]", "--jars", os.environ["CONNECTOR_JAR"], str(in_script)])
        proc = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        err, out = proc.communicate()
        if self.schema_tests:
            return proc.returncode, out.decode(encoding='UTF-8'), err.decode(encoding='UTF-8')
        else:
            # print spark log and stdout (when  py.test -s is used)
            # spark log is in stdout while test output is in stderr
            print(out.decode(encoding='UTF-8'))
            print(err.decode(encoding='UTF-8'))

            if proc.returncode != 0:
                pytest.fail(err.decode(encoding='UTF-8'))


def createSparkConf():
    from pyspark import SparkConf
    test_properties = conftest.test_properties()

    conf = SparkConf()
    conf.set("cloudant.host", test_properties["cloudanthost"])
    conf.set("cloudant.username", test_properties["cloudantusername"])
    conf.set("cloudant.password", test_properties["cloudantpassword"])

    return conf


def get_test_properties():
    return conftest.test_properties()


def run_sample_test(in_script, sparksubmit):
    __tracebackhide__ = True

    # spark-submit the script
    import os, sys
    command = [sparksubmit]
    command.extend(["--master", "local[4]", "--jars", os.environ["CONNECTOR_JAR"], str(in_script)])
    proc = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    err, out = proc.communicate()
    # print spark log and stdout (when  py.test -s is used)
    # spark log is in stdout while test output is in stderr
    print(out.decode(encoding='UTF-8'))
    print(err.decode(encoding='UTF-8'))

    if proc.returncode != 0:
        pytest.fail(err.decode(encoding='UTF-8'))
