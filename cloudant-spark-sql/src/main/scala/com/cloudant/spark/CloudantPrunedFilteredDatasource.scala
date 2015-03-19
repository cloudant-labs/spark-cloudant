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
package com.cloudant.spark

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.expressions.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.sources._
import com.cloudant.spark.common._

/**
 * @author yanglei
 */

case class CloudantPrunedFilteredScan (dbName: String, indexName: String)
                      (@transient val sqlContext: SQLContext) 
  extends BaseRelation with PrunedFilteredScan 
{
  lazy val dataAccess = {
      new JsonStoreDataAccess(JsonStoreConfigManager.getConfig(sqlContext, dbName, indexName))
  }
  
  lazy val config: CloudantConfig = {
    JsonStoreConfigManager.getConfig(sqlContext, dbName, indexName).asInstanceOf[CloudantConfig]
  }

  val schema: StructType = {
      val aRDD = sqlContext.sparkContext.parallelize(dataAccess.getOne())
      sqlContext.jsonRDD(aRDD).schema
  }

    def buildScan(requiredColumns: Array[String], 
                filters: Array[Filter]): RDD[Row] = {
      val filterInterpreter = new FilterInterpreter(filters)
      var searchField:String = {
        if (filterInterpreter.containsFiltersFor(config.pkField)) config.pkField
        else  filterInterpreter.firstField
      }  
      val (min, minInclusive, max, maxInclusive) = filterInterpreter.getInfo(searchField)
      implicit val columns = requiredColumns
      val (url: String, pusheddown: Boolean) =  config.getRangeUrl(searchField, min,minInclusive, max,maxInclusive)
      if (!pusheddown) searchField = null
      implicit val attrToFilters = filterInterpreter.getFiltersForPostProcess(searchField)

      val rows = dataAccess.getAll(url) 
      val sRDD = sqlContext.sparkContext.parallelize(rows)
      sqlContext.jsonRDD(sRDD).rdd
  }

}

class CloudantPrunedFilteredRP extends RelationProvider {

  def createRelation(sqlContext: SQLContext, parameters: Map[String, String]) = {
    CloudantPrunedFilteredScan(
      parameters("database"),parameters.getOrElse("index",null))(sqlContext)
  }
  
}