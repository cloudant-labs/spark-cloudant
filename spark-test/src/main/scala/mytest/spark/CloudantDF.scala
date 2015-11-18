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

/**
 * @author yanglei
 */
object CloudantDF{
  
      def main(args: Array[String]) {

        val conf = new SparkConf().setAppName("Cloudant Spark SQL External Datasource with DataFrame")
        conf.set("cloudant.host","ACCOUNT.cloudant.com")
        conf.set("cloudant.username", "USERNAME")
        conf.set("cloudant.password","PASSWORD")
        val sc = new SparkContext(conf)
        
        val sqlContext = new SQLContext(sc)
        import sqlContext._
        

        val df = sqlContext.read.format("com.cloudant.spark").load("n_airportcodemapping")
        df.printSchema()
        df.filter(df("airportName") >= "Moscow").select("_id","airportName").show()
        df.filter(df("_id") >= "CAA").select("_id","airportName").show()
        df.filter(df("_id") >= "CAA").select("_id","airportName").write.format("com.cloudant.spark").save("airportcodemapping_df")

        val df2 = sqlContext.read.format("com.cloudant.spark").load("n_flight")
        val total = df2.filter(df2("flightSegmentId") >"AA9").select("flightSegmentId", "scheduledDepartureTime").orderBy(df2("flightSegmentId")).count()
        println(s"Total $total flights from table scan")

        val df3 = sqlContext.read.format("com.cloudant.spark").option("index", "_design/view/_search/n_flights").load("n_flight")
        val total2 = df3.filter(df3("flightSegmentId") >"AA9").select("flightSegmentId", "scheduledDepartureTime").orderBy(df3("flightSegmentId")).count()
        println(s"Total $total2 flights from index")
}
}