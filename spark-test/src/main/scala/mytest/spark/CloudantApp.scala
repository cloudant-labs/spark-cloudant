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
        conf.set("cloudant.host","ACCOUNT.cloudant.com")
        conf.set("cloudant.username", "USERNAME")
        conf.set("cloudant.password","PASSWORD")
        val sc = new SparkContext(conf)
        
        val sqlContext = new SQLContext(sc)
        import sqlContext._
        
       println("About to test com.cloudant.spark.CloudantRP for n_airportcodemapping")
        sqlContext.sql(
      s"""
        |CREATE TEMPORARY TABLE airportTable
        |USING com.cloudant.spark.CloudantRP
        |OPTIONS ( database 'n_airportcodemapping')
      """.stripMargin)
      
      val airportData = sqlContext.sql("SELECT _id, airportName FROM airportTable WHERE _id >= 'CAA' ORDER BY _id")
      airportData.printSchema()
      println(s"Total # of rows in airportData: " + airportData.count())
      airportData.map(t => "code: " + t(0) + ",name:" + t(1)).collect().foreach(println) 

//       println("About to test com.cloudant.spark.CloudantRP for booking") -- ArrayIndexOutOfBoundsException on 1.4.1
//        sqlContext.sql(
//      s"""
//        |CREATE TEMPORARY TABLE bookingTable
//        |USING com.cloudant.spark.CloudantRP
//        |OPTIONS (database 'booking')
//      """.stripMargin)
//      
//      val bookingData = sqlContext.sql("SELECT customerId, dateOfBooking FROM bookingTable WHERE customerId = 'uid0@email.com'")
//      bookingData.printSchema()
//
//      bookingData.map(t => "customer: " + t(0) + ", dateOfBooking: " + t(1)).collect().foreach(println) 

      // defect 56458 - 0 row in DF
      println("About to test com.cloudant.spark.CloudantPrunedFilteredRP for n_airportcodemapping")
        sqlContext.sql(
      s"""
        |CREATE TEMPORARY TABLE airportTable2
        |USING com.cloudant.spark.CloudantPrunedFilteredRP
        |OPTIONS ( database 'n_airportcodemapping')
      """.stripMargin)
      
      val airportData2 = sqlContext.sql("SELECT _id, airportName FROM airportTable2 WHERE _id >= 'CAA' AND _id <= 'GAA' ORDER BY _id")
      airportData2.printSchema()
      println(s"Total # of rows in airportData2: " + airportData2.count())
      airportData2.map(t => "code: " + t(0) + ",name:" + t(1)).collect().foreach(println) 

       println("About to test com.cloudant.spark.CloudantPrunedFilteredRP for n_booking") 
        sqlContext.sql(
      s"""
        |CREATE TEMPORARY TABLE bookingTable2
        |USING com.cloudant.spark.CloudantPrunedFilteredRP
        |OPTIONS (database 'n_booking')
      """.stripMargin)

      val bookingData2 = sqlContext.sql("SELECT customerId, dateOfBooking FROM bookingTable2 WHERE customerId = 'uid0@email.com'")
      bookingData2.printSchema()

      bookingData2.map(t => "customer: " + t(0) + ", dateOfBooking: " + t(1)).collect().foreach(println) 

       println("About to test com.cloudant.spark.CloudantPrunedFilteredRP for flight with index")
        sqlContext.sql(
      s"""
        |CREATE TEMPORARY TABLE flightTable
        |USING com.cloudant.spark.CloudantPrunedFilteredRP
        |OPTIONS (database 'n_flight', index '_design/view/_search/n_flights')
      """.stripMargin)

      val flightData = sqlContext.sql("SELECT flightSegmentId, scheduledDepartureTime FROM flightTable WHERE flightSegmentId >'AA9' AND flightSegmentId<'AA95'")
      flightData.printSchema()

      flightData.map(t => "flightSegmentId: " + t(0) + ", scheduledDepartureTime: " + t(1)).collect().foreach(println) 


      // defect 56458 - exception thrown, commenting out so remaining tests will run
//       println("About to test com.cloudant.spark.CloudantPartitionedPrunedFilteredRP for n_airportcodemapping")  
//        sqlContext.sql(
//      s"""
//        |CREATE TEMPORARY TABLE airportTable3
//        |USING com.cloudant.spark.CloudantPartitionedPrunedFilteredRP
//        |OPTIONS ( database 'n_airportcodemapping')
//      """.stripMargin)
//      
//      val airportData3 = sqlContext.sql("SELECT _id, airportName FROM airportTable3 WHERE _id >= 'CAA' AND _id <= 'GAA' ORDER BY _id")
//      airportData3.printSchema()
//      airportData3.map(t => "code: " + t(0) + ",name:" + t(1)).collect().foreach(println) 

       println("About to test com.cloudant.spark.CloudantPartitionedPrunedFilteredRP for n_booking")
        sqlContext.sql(
      s"""
        |CREATE TEMPORARY TABLE bookingTable3
        |USING com.cloudant.spark.CloudantPartitionedPrunedFilteredRP
        |OPTIONS (database 'n_booking')
      """.stripMargin)
      
      val bookingData3 = sqlContext.sql("SELECT customerId, dateOfBooking FROM bookingTable3 WHERE customerId = 'uid0@email.com'")
      bookingData3.printSchema()

      bookingData3.map(t => "customer: " + t(0) + ", dateOfBooking: " + t(1)).collect().foreach(println) 

      println("About to test com.cloudant.spark.CloudantPartitionedPrunedFilteredRP for flight with index")
        sqlContext.sql(
      s"""
        |CREATE TEMPORARY TABLE flightTable2
        |USING com.cloudant.spark.CloudantPartitionedPrunedFilteredRP
        |OPTIONS (database 'n_flight', path '_design/view/_search/n_flights')
      """.stripMargin)

      val flightData2 = sqlContext.sql("SELECT flightSegmentId, scheduledDepartureTime FROM flightTable2 WHERE flightSegmentId >'AA9' AND flightSegmentId<'AA95'")
      flightData2.printSchema()

      flightData2.map(t => "flightSegmentId: " + t(0) + ", scheduledDepartureTime: " + t(1)).collect().foreach(println) 


      }

      

}