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
object CloudantApp {
  
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Cloudant Spark SQL External Datasource")
    // set protocol to http if needed, default value=https
    // conf.set("cloudant.protocol","http")
    conf.set("cloudant.host","ACCOUNT.cloudant.com")
    conf.set("cloudant.username", "USERNAME")
    conf.set("cloudant.password","PASSWORD")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext._

    println("About to test com.cloudant.spark for n_airportcodemapping")
    sqlContext.sql(
        s"""
        |CREATE TEMPORARY TABLE airportTable
        |USING com.cloudant.spark
        |OPTIONS ( database 'n_airportcodemapping')
        """.stripMargin)
    val airportData = sqlContext.sql("SELECT _id, airportName FROM airportTable WHERE _id >= 'CAA' AND _id <= 'GAA' ORDER BY _id")
    airportData.printSchema()
    println(s"Total # of rows in airportData: " + airportData.count())
    airportData.map(t => "code: " + t(0) + ",name:" + t(1)).collect().foreach(println) 


    println("About to test com.cloudant.spark for n_booking") 
    sqlContext.sql(
        s"""
        |CREATE TEMPORARY TABLE bookingTable
        |USING com.cloudant.spark
        |OPTIONS (database 'n_booking')
        """.stripMargin)
    val bookingData = sqlContext.sql("SELECT customerId, dateOfBooking FROM bookingTable WHERE customerId = 'uid0@email.com'")
    bookingData.printSchema()
    bookingData.map(t => "customer: " + t(0) + ", dateOfBooking: " + t(1)).collect().foreach(println) 

    println("About to test com.cloudant.spark for flight with index")
    sqlContext.sql(
        s"""
        |CREATE TEMPORARY TABLE flightTable
        |USING com.cloudant.spark
        |OPTIONS (database 'n_flight', index '_design/view/_search/n_flights')
        """.stripMargin)
    val flightData = sqlContext.sql("SELECT flightSegmentId, scheduledDepartureTime FROM flightTable WHERE flightSegmentId >'AA9' AND flightSegmentId<'AA95'")
    flightData.printSchema()
    flightData.map(t => "flightSegmentId: " + t(0) + ", scheduledDepartureTime: " + t(1)).collect().foreach(println) 
  
  }
}
