## Pytest test scripts

### Setup testing environment

1. Clone the repo into a folder, set up a Python 3 [virtual environment](https://virtualenv.pypa.io/en/latest/),
install the requirements:
    ```
    $ git clone git clone git@github.com:cloudant-labs/spark-cloudant.git
    $ cd spark-cloudant
    $ virtualenv .
    $ ./bin/activate
    $ pip install -r requirements.txt
    ```
2. Download the pre-built [Spark 2.1 for Hadoop 2.7 and later](http://d3kbcqa49mib13.cloudfront.net/spark-2.1.0-bin-hadoop2.7.tgz)
   and run the standalone master server:  ` ./sbin/start-master.sh`
   [Additional Spark standalone instructions](http://spark.apache.org/docs/latest/spark-standalone.html).
3. Export environment variables within the Python 3 virtual environment:

  - `CONNECTOR_JAR`      (path to the spark-cloudant connector jar file)
  - `SPARK_HOME`         (path to the local spark home)
  - `CLOUDANT_HOST`      (Cloudant account with read/write access)
  - `CLOUDANT_USERNAME`  (Cloudant account username)
  - `CLOUDANT_PASSWORD`  (Cloudant account password)
  ```
     export CONNECTOR_JAR=/PATH/spark-cloudant/cloudant-spark-sql/target/scala-2.11/cloudant-spark.jar
     export SPARK_HOME=/PATH/spark-2.1.0-bin-hadoop2.7/
     export CLOUDANT_HOST=ACCOUNT.cloudant.com
     export CLOUDANT_USERNAME=username
     export CLOUDANT_PASSWORD=password
  ```

4. Populate a Cloudant test database with [acmeair-nodejs app](https://github.com/acmeair/acmeair-nodejs) and
   [Data Loader](#data-loader).

### Data Loader:
A utility is included to create test data to the cloudant database.
- Download [acmeair-nodejs app](https://github.com/acmeair/acmeair-nodejs).  Follow the project README.md to install the required dependencies.
- Export environment variable:
  - `ACMEAIR_HOME`  (path to the acmeair-nodejs project)

  ```
     export ACMEAIR_HOME=/mypath/acmeair-nodejs-master
  ```
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
