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
import subprocess

def create_test_py(in_script, out_script, test_properties):
	"""
	Given a master test script file, insert the necessary test configurations (eg. Cloudant credentials),
	write it to a temp file then submit a spark job
	"""

	replacements = {
	"<cloudanthost>":test_properties["cloudanthost"],
	"<cloudantusername>":test_properties["cloudantusername"],
	"<cloudantpassword>":test_properties["cloudantpassword"]}
	
	lines = []
	with open(in_script) as infile:
		for line in infile:
			for src, target in replacements.items():
				line = line.replace(src, target)
			lines.append(line)
	infile.close()
	
	with open(str(out_script), "w") as outfile:
		for line in lines:
			outfile.write(line)
	outfile.close()

	print ("Master Script File = ", in_script)
	print ("Temp Script File = ", out_script)
	
	
def run_test(in_script, out_script_name, out_script_dir, sparksubmit, test_properties):
	__tracebackhide__ = True
	
	# write the test script to a temp file with proper configuration
	# note: pytest keeps 5 temp dirs created for tests and will clean up itself
	out_script = out_script_dir.join(out_script_name)
	create_test_py(in_script, out_script, test_properties)
	
	# spark-submit the script
	import os, sys
	command = [sparksubmit]
	command.extend(["--master", "local[4]", "--jars", os.environ["CONNECTOR_JAR"], str(out_script)])
	proc = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
	err, out = proc.communicate()
	# print spark log and stdout (when  py.test -s is used)
	# spark log is in stdout while test output is in stderr
	print(out.decode(encoding='UTF-8'))
	print(err.decode(encoding='UTF-8'))
	
	if proc.returncode != 0:
		pytest.fail(err.decode(encoding='UTF-8'))
	



	
	