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
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.sources._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SaveMode
import org.apache.spark.SparkEnv
import akka.event.Logging
import com.cloudant.spark.common._



/**
 * @author yanglei
 */
case class CloudantReadWriteRelation (config:CloudantConfig, schema: StructType, allDocsDF: DataFrame = null)
                      (@transient val sqlContext: SQLContext) 
  extends BaseRelation with PrunedFilteredScan  with InsertableRelation 
{
   @transient lazy val dataAccess = {new JsonStoreDataAccess(config)}

    implicit lazy val system = config.getSystem()
    lazy val logger = {Logging(system, getClass)}

    def buildScan(requiredColumns: Array[String], 
                filters: Array[Filter]): RDD[Row] = {
      val colsLength = requiredColumns.length

      if (allDocsDF != null) {
        if (colsLength == 0) {
          allDocsDF.select().rdd
        } else if (colsLength == 1) {
          allDocsDF.select(requiredColumns(0)).rdd
        } else {
          val colsExceptCol0 = for (i <- 1 until colsLength) yield requiredColumns(i)
          allDocsDF.select(requiredColumns(0), colsExceptCol0: _*).rdd
        }
      } else {
        val filterInterpreter = new FilterInterpreter(filters)
        var searchField:String = {
          if (filterInterpreter.containsFiltersFor(config.pkField)) config.pkField
          else  filterInterpreter.firstField
        }  
        
        val (min, minInclusive, max, maxInclusive) = filterInterpreter.getInfo(searchField)
        implicit val columns = requiredColumns
        val (url: String, pusheddown: Boolean) =  config.getRangeUrl(searchField,
            min, minInclusive, max, maxInclusive, false)
        if (!pusheddown) searchField = null
        implicit val attrToFilters = filterInterpreter.getFiltersForPostProcess(searchField)
        
        val cloudantRDD  = new JsonStoreRDD(sqlContext.sparkContext,config,url)
        val df = sqlContext.read.json(cloudantRDD)
        if (colsLength > 1) {
          val colsExceptCol0 = for (i <- 1 until colsLength) yield requiredColumns(i)
          df.select(requiredColumns(0), colsExceptCol0: _*).rdd
        } else {
          df.rdd
        }
      }
    }


  def insert(data:DataFrame, overwrite: Boolean) = {
    val ifIgnore = dataAccess.maybeCreateDB();
    if (ifIgnore){
      logger.warning(("Database " + config.getDbname() +
        "exists, and saveMode=Ignore; nothing was saved."))
    } else if (data.count() == 0){
      logger.warning(("Database " + config.getDbname() +
        ": nothing was saved because the number of records was 0!"))
    } else {
      val result = data.toJSON.foreachPartition { x =>
        val list = x.toList // Has to pass as List, Iterator results in 0 data
        dataAccess.saveAll(list)
      }
    }
  }
}

/**
 * @author yanglei
 */
class DefaultSource extends RelationProvider with CreatableRelationProvider with SchemaRelationProvider{

      implicit val system = SparkEnv.get.actorSystem
      val logger = Logging(system, getClass)

    def createRelation(sqlContext: SQLContext, parameters: Map[String, String]) = {
      create(sqlContext, parameters, null)
    }
      
    private def create(sqlContext: SQLContext, parameters: Map[String, String], inSchema: StructType) = {
    
      val config: CloudantConfig = JsonStoreConfigManager.getConfig(sqlContext, parameters)

      var allDocsDF: DataFrame = null
      
      val schema: StructType = {
        if (inSchema!=null)
          inSchema
        else{
          val df = if (config.getSchemaSampleSize() == JsonStoreConfigManager.SCHEMA_FOR_ALL_DOCS_NUM
              && config.viewName == null && config.indexName == null) {
            val filterInterpreter = new FilterInterpreter(null)
            var searchField = null
            val (min, minInclusive, max, maxInclusive) =
                filterInterpreter.getInfo(searchField)
            val (url: String, pusheddown: Boolean) = config.getRangeUrl(searchField,
                min, minInclusive, max, maxInclusive, false)
            val cloudantRDD  = new JsonStoreRDD(sqlContext.sparkContext,config, url)
            allDocsDF = sqlContext.read.json(cloudantRDD)
            allDocsDF
          } else {
            val dataAccess = new JsonStoreDataAccess(config)
            val aRDD = sqlContext.sparkContext.parallelize(
                dataAccess.getMany(config.getSchemaSampleSize()))
            sqlContext.read.json(aRDD)
          }
          df.schema
        }
      }
      CloudantReadWriteRelation(config, schema, allDocsDF)(sqlContext)
    }
  
    def createRelation(sqlContext:SQLContext,mode:SaveMode, parameters:Map[String,String], data:DataFrame) =
    {
      val relation =create(sqlContext, parameters, data.schema)
      relation.insert(data, mode==SaveMode.Overwrite)
      relation
    }
  
    def createRelation(sqlContext:SQLContext, parameters:Map[String,String], schema: StructType) ={
      create(sqlContext, parameters, schema)
    }
  
}