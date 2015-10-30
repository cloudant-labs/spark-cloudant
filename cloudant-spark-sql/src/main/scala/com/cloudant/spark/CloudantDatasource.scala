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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.{TableScan, RelationProvider, BaseRelation}
import com.cloudant.spark.common._

/**
 * @author yanglei
 */
case class CloudantTableScan (dbName: String)
                      (@transient val sqlContext: SQLContext) 
  extends BaseRelation with TableScan 
{
  lazy val dataAccess = {
      new JsonStoreDataAccess(JsonStoreConfigManager.getConfig(sqlContext, dbName)) }
  
  lazy val config: CloudantConfig = {
    JsonStoreConfigManager.getConfig(sqlContext, dbName).asInstanceOf[CloudantConfig]
  }

  val schema: StructType = {
      val aRDD = sqlContext.sparkContext.parallelize(dataAccess.getOne())
      sqlContext.read.json(aRDD).schema
  }

  def buildScan: RDD[Row] = {
      val (url, _) = config.getRangeUrl()
      val aRDD = sqlContext.sparkContext.parallelize(dataAccess.getAll(url))
      sqlContext.read.json(aRDD).rdd
  }
  
}

class CloudantRP extends RelationProvider {

  def createRelation(sqlContext: SQLContext, parameters: Map[String, String]) = {
    CloudantTableScan(
      parameters("database"))(sqlContext)
  }
  
}