## Pytest test scripts

### Prerequisites
- python 3  (or use [virtualenv](https://virtualenv.readthedocs.org/en/latest/) to create an isolated python 3 environment)
- python [Requests module] (http://docs.python-requests.org/en/latest/user/install/)
- [pytest](http://pytest.org/latest/)
- spark 1.4 or 1.5 (http://spark.apache.org/docs/latest/spark-standalone.html) up and running 
- cloudant databases with test data populated by the [acmeair-nodejs app](https://github.com/acmeair/acmeair-nodejs)

### Test Setup:
- Start a python 3 virtualenv
- Export environment variables

     `CONNECTOR_JAR`  (path to the spark-cloudant connector jar file)

     `SPARK_HOME`        (path to the local spark home)

    ```
    export CONNECTOR_JAR=/mypath/spark-cloudant/cloudant-spark-sql/target/scala-2.10/cloudant-spark.jar
    export SPARK_HOME=/Applications/spark-1.4.1-bin-hadoop2.6/
    ```
- Edit spark-cloudant/python/test/conftest.py, update the test_properties fixture (eg. cloudant credentials) 
    
### Run Tests:
- Go to the spark-cloudant/python/test
- Run `py.test`   (or `py.test -s` to disable output capturing for debug purposes)

### Add More Tests:
- Add your test script to test/test-scripts dir
- Add the test to test/test_cloudantapp.py or create a new file prefixed with test_


		    
