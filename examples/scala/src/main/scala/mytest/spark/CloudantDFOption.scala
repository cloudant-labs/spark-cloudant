/*******************************************************************************
* Copyright (c) 2015 IBM Corp.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*******************************************************************************/
package mytest.spark

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel

/**
 * @author yanglei
 */
object CloudantDFOption{
  
      def main(args: Array[String]) {

        val conf = new SparkConf().setAppName("Cloudant Spark SQL External Datasource with DataFrame using Option")
        val sc = new SparkContext(conf)
        
        val sqlContext = new SQLContext(sc)
        import sqlContext._
        

        val cloudantHost = "ACCOUNT.cloudant.com"
        val cloudantUser = "USERNAME"
        val cloudantPassword = "PASSWORD"
        val df = sqlContext.read.format("com.cloudant.spark").option("cloudant.host",cloudantHost).option("cloudant.username", cloudantUser).option("cloudant.password",cloudantPassword).load("n_airportcodemapping")
        
        // In case of doing multiple operations on a dataframe (select, filter etc.)
        // you should persist the dataframe.
        // Othewise, every operation on the dataframe will load the same data from Cloudant again.
        // Persisting will also speed up computation.
        df.cache() //persisting in memory
        //  alternatively for large dbs to persist in memory & disk:
        // df.persist(StorageLevel.MEMORY_AND_DISK)
        
        df.printSchema()

        df.filter(df("_id") >= "CAA").select("_id","airportName").show()
        df.filter(df("_id") >= "CAA").select("_id","airportName").write.format("com.cloudant.spark").option("cloudant.host",cloudantHost).option("cloudant.username", cloudantUser).option("cloudant.password",cloudantPassword).save("airportcodemapping_df")

        val df2 = sqlContext.read.format("com.cloudant.spark").option("cloudant.host",cloudantHost).option("cloudant.username", cloudantUser).option("cloudant.password",cloudantPassword).load("n_flight")
        val total = df2.filter(df2("flightSegmentId") >"AA9").select("flightSegmentId", "scheduledDepartureTime").orderBy(df2("flightSegmentId")).count()
        println(s"Total $total flights from table scan")

        val df3 = sqlContext.read.format("com.cloudant.spark").option("index", "_design/view/_search/n_flights").option("cloudant.host",cloudantHost).option("cloudant.username", cloudantUser).option("cloudant.password",cloudantPassword).load("n_flight")
        val total2 = df3.filter(df3("flightSegmentId") >"AA9").select("flightSegmentId", "scheduledDepartureTime").orderBy(df3("flightSegmentId")).count()
        println(s"Total $total2 flights from index")
}
}
