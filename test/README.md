## Pytest test scripts

### Prerequisites
- python 3  (or use [virtualenv](https://virtualenv.readthedocs.org/en/latest/) to create an isolated python 3 environment)
- python [Requests module] (http://docs.python-requests.org/en/latest/user/install/)
- [pytest](http://pytest.org/latest/)
- spark 1.4 or 1.5 (http://spark.apache.org/docs/latest/spark-standalone.html) up and running 
- cloudant databases with test data populated by the [acmeair-nodejs app](https://github.com/acmeair/acmeair-nodejs).  See [Data Loader](https://github.com/cloudant-labs/spark-cloudant/tree/master/test#data-loader).

### Test Setup:
- Start a python 3 virtualenv
- Export environment variables:

  - `CONNECTOR_JAR`  (path to the spark-cloudant connector jar file)
  - `SPARK_HOME`        (path to the local spark home)

  ```
     export CONNECTOR_JAR=/mypath/spark-cloudant/cloudant-spark-sql/target/scala-2.10/cloudant-spark.jar
     export SPARK_HOME=/Applications/spark-1.4.1-bin-hadoop2.6/
  ```
- Edit spark-cloudant/test/conftest.py, update the test_properties fixture (eg. cloudant credentials) 

### Data Loader:
A utility is included to create test data to the cloudant database.
- Download [acmeair-nodejs app](https://github.com/acmeair/acmeair-nodejs).  Follow the project README.md to install the required dependencies.
- Export environment variable:
  - `ACMEAIR_HOME`  (path to the acmeair-nodejs project)

  ```
     export ACMEAIR_HOME=/mypath/acmeair-nodejs-master
  ```
- Edit spark-cloudant/test/conftest.py, update the test_properties fixture.  Note the cloudant credentials need to have rights to **create** and **delete** databases.
- Go to spark-cloudant/test
- Run `python -m helpers.dataload [options]` with one of the following arguments:
  - `-cleanup`  (Drop all test databases)
  - `-load num_of_customers`  (Reset and Load databases with the given # of users. -load 0 to just recreate databases and indexes.)
  ```
     python -m helpers.dataload -load 200
  ```
- Run `python -m helpers.schema -addDocs` to add docs for schema test

  _Note_: To load custom documents, put them in folder `test/resources/<database>.json` where `<database>` represents the database to load the document into. An invalid database name will be ignored. Documents are supplied in a `docs` array for bulk loading, e.g.:

  `test/resources/n_customer.json`
  ```
  {
      "docs":[
          {
              "_id": "1",
              "name":{
                  "firstname":"Jim",
                  "lastname":"Yal"
              },
              "age":20,
              "total_miles":true
          },
       ...
      ]
  }
  ```


### Run Tests:
- Go to spark-cloudant/test
- Run `py.test`   (or `py.test -s` to disable output capturing for debug purposes)

### Add More Tests:
- Add your test script to test/test-scripts dir
- Add the test to test/test_cloudantapp.py or create a new file prefixed with test_
