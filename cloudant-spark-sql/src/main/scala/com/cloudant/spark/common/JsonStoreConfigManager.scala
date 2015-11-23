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
package com.cloudant.spark.common

import org.apache.spark.sql.SQLContext
import com.typesafe.config.ConfigFactory
import com.cloudant.spark.CloudantConfig

 object JsonStoreConfigManager
{
  val CLOUDANT_HOST_CONFIG = "cloudant.host"
  val CLOUDANT_USERNAME_CONFIG = "cloudant.username"
  val CLOUDANT_PASSWORD_CONFIG = "cloudant.password"
  
  val PARTITION_CONFIG = "jsonstore.rdd.partitions"
  val MAX_IN_PARTITION_CONFIG = "jsonstore.rdd.maxInPartition"
  val MIN_IN_PARTITION_CONFIG = "jsonstore.rdd.minInPartition"
  val REQUEST_TIMEOUT_CONFIG = "jsonstore.rdd.requestTimeout"
  val CONCURRENT_SAVE_CONFIG = "jsonstore.rdd.concurrentSave"
  val BULK_SIZE_CONFIG = "jsonstore.rdd.bulkSize"
  
  val configFactory = ConfigFactory.load()

  val ROOT_CONFIG_NAME = "spark-sql"
  val rootConfig = configFactory.getConfig(ROOT_CONFIG_NAME) 
  val defaultPartitions = rootConfig.getInt(PARTITION_CONFIG)
  val defaultMaxInPartition = rootConfig.getInt(MAX_IN_PARTITION_CONFIG)
  val defaultMinInPartition = rootConfig.getInt(MIN_IN_PARTITION_CONFIG)
  val defaultRequestTimeout = rootConfig.getLong(REQUEST_TIMEOUT_CONFIG)
  val defaultConcurrentSave = rootConfig.getInt(CONCURRENT_SAVE_CONFIG)
  val defaultBulkSize = rootConfig.getInt(BULK_SIZE_CONFIG)
  
  def getConfig(context: SQLContext, dbName: String, indexName:String = null): CloudantConfig = {
      val sparkConf = context.sparkContext.getConf
      implicit val total = sparkConf.getInt(PARTITION_CONFIG,defaultPartitions)
      implicit val max = sparkConf.getInt(MAX_IN_PARTITION_CONFIG,defaultMaxInPartition)
      implicit val min =sparkConf.getInt(MIN_IN_PARTITION_CONFIG,defaultMinInPartition)
      implicit val requestTimeout =sparkConf.getLong(REQUEST_TIMEOUT_CONFIG,defaultRequestTimeout)
      implicit val concurrentSave =sparkConf.getInt(CONCURRENT_SAVE_CONFIG,defaultConcurrentSave)
      implicit val bulkSize =sparkConf.getInt(BULK_SIZE_CONFIG,defaultBulkSize)

      if (sparkConf.contains(CLOUDANT_HOST_CONFIG))
      {
        val host = sparkConf.get(CLOUDANT_HOST_CONFIG)
        val user = sparkConf.get(CLOUDANT_USERNAME_CONFIG)
        val passwd = sparkConf.get(CLOUDANT_PASSWORD_CONFIG)
        return new CloudantConfig(host,  dbName, indexName)(user, passwd, total, max, min,requestTimeout,concurrentSave, bulkSize)
      }
      null
  }
  
  /**
   * The configuration sequence: DataFrame option override SparkConf override defaults
   */
  
  def getConfig(context: SQLContext,  parameters: Map[String, String]): CloudantConfig = {
    
      val sparkConf = context.sparkContext.getConf
      
      val totalS = parameters.getOrElse(PARTITION_CONFIG,null)
      implicit val total = if (totalS ==null) sparkConf.getInt(PARTITION_CONFIG,defaultPartitions) else totalS.toInt
      val maxS = parameters.getOrElse(MAX_IN_PARTITION_CONFIG,null)
      implicit val max = if (maxS ==null) sparkConf.getInt(MAX_IN_PARTITION_CONFIG,defaultMaxInPartition) else maxS.toInt
      val minS = parameters.getOrElse(MIN_IN_PARTITION_CONFIG,null)
      implicit val min = if (minS ==null) sparkConf.getInt(MIN_IN_PARTITION_CONFIG,defaultMinInPartition) else minS.toInt
      
      implicit val requestTimeout =sparkConf.getLong(REQUEST_TIMEOUT_CONFIG,defaultRequestTimeout)
      implicit val concurrentSave =sparkConf.getInt(CONCURRENT_SAVE_CONFIG,defaultConcurrentSave)
      implicit val bulkSize =sparkConf.getInt(BULK_SIZE_CONFIG,defaultBulkSize)

      val dbName = parameters.getOrElse("database", parameters.getOrElse("path",null))
      val indexName = parameters.getOrElse("index",null)
      
      println(s"Use dbName=$dbName, indexName=$indexName, $PARTITION_CONFIG=$total, $MAX_IN_PARTITION_CONFIG=$max, $MIN_IN_PARTITION_CONFIG=$min, $REQUEST_TIMEOUT_CONFIG=$requestTimeout,$CONCURRENT_SAVE_CONFIG=$concurrentSave,$BULK_SIZE_CONFIG=$bulkSize")
      

      if (sparkConf.contains(CLOUDANT_HOST_CONFIG) || parameters.contains(CLOUDANT_HOST_CONFIG) ) 
      {
        val host = parameters.getOrElse(CLOUDANT_HOST_CONFIG,sparkConf.get(CLOUDANT_HOST_CONFIG))
        val user = parameters.getOrElse(CLOUDANT_USERNAME_CONFIG,sparkConf.get(CLOUDANT_USERNAME_CONFIG))
        val passwd = parameters.getOrElse(CLOUDANT_PASSWORD_CONFIG,sparkConf.get(CLOUDANT_PASSWORD_CONFIG))
        return new CloudantConfig(host,  dbName, indexName)(user, passwd, total, max, min,requestTimeout,concurrentSave,bulkSize)
      }
      null
  }

}