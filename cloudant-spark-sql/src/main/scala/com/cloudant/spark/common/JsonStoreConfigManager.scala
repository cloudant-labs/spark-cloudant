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
  val CLOUDANT_CONNECTOR_VERSION = "1.6.2"

  val SCHEMA_FOR_ALL_DOCS_NUM = -1
  
  val SPARK_CLOUDANT_PROTOCOL_CONFIG = "spark.cloudant.protocol"
  val SPARK_CLOUDANT_HOST_CONFIG = "spark.cloudant.host"
  val SPARK_CLOUDANT_USERNAME_CONFIG = "spark.cloudant.username"
  val SPARK_CLOUDANT_PASSWORD_CONFIG = "spark.cloudant.password"
  val SPARK_PARTITION_CONFIG = "spark.jsonstore.rdd.partitions"
  val SPARK_MAX_IN_PARTITION_CONFIG = "spark.jsonstore.rdd.maxInPartition"
  val SPARK_MIN_IN_PARTITION_CONFIG = "spark.jsonstore.rdd.minInPartition"
  val SPARK_REQUEST_TIMEOUT_CONFIG = "spark.jsonstore.rdd.requestTimeout"
  val SPARK_CONCURRENT_SAVE_CONFIG = "spark.jsonstore.rdd.concurrentSave"
  val SPARK_BULK_SIZE_CONFIG = "spark.jsonstore.rdd.bulkSize"
  val SPARK_SCHEMA_SAMPLE_SIZE_CONFIG = "spark.jsonstore.rdd.schemaSampleSize"

  val CLOUDANT_HOST_CONFIG = "cloudant.host"
  val CLOUDANT_USERNAME_CONFIG = "cloudant.username"
  val CLOUDANT_PASSWORD_CONFIG = "cloudant.password"
  val CLOUDANT_PROTOCOL_CONFIG = "cloudant.protocol"


  val PARTITION_CONFIG = "jsonstore.rdd.partitions"
  val MAX_IN_PARTITION_CONFIG = "jsonstore.rdd.maxInPartition"
  val MIN_IN_PARTITION_CONFIG = "jsonstore.rdd.minInPartition"
  val REQUEST_TIMEOUT_CONFIG = "jsonstore.rdd.requestTimeout"
  val CONCURRENT_SAVE_CONFIG = "jsonstore.rdd.concurrentSave"
  val BULK_SIZE_CONFIG = "jsonstore.rdd.bulkSize"
  val SCHEMA_SAMPLE_SIZE_CONFIG = "jsonstore.rdd.schemaSampleSize"
  val PARAM_SCHEMA_SAMPLE_SIZE_CONFIG = "schemaSampleSize"
  val PARAM_BULK_SIZE_CONFIG = "bulkSize"
  
  val configFactory = ConfigFactory.load()

  val ROOT_CONFIG_NAME = "spark-sql"
  val rootConfig = configFactory.getConfig(ROOT_CONFIG_NAME) 
  val defaultPartitions = rootConfig.getInt(PARTITION_CONFIG)
  val defaultMaxInPartition = rootConfig.getInt(MAX_IN_PARTITION_CONFIG)
  val defaultMinInPartition = rootConfig.getInt(MIN_IN_PARTITION_CONFIG)
  val defaultRequestTimeout = rootConfig.getLong(REQUEST_TIMEOUT_CONFIG)
  val defaultConcurrentSave = rootConfig.getInt(CONCURRENT_SAVE_CONFIG)
  val defaultBulkSize = rootConfig.getInt(BULK_SIZE_CONFIG)
  val defaultSchemaSampleSize = rootConfig.getInt(SCHEMA_SAMPLE_SIZE_CONFIG)

  def calculateSchemaSampleSize(schemaSampleSize: String): Int = {
    if (schemaSampleSize != null) {
      schemaSampleSize.toInt;
    } else {
      defaultSchemaSampleSize
    }
  }
  
  def getConfig(context: SQLContext, dbName: String, indexName:String = null, viewName:String = null, schemaSampleSize: String): CloudantConfig = {
      val sparkConf = context.sparkContext.getConf
      implicit val total = sparkConf.getInt(SPARK_PARTITION_CONFIG, sparkConf.getInt(PARTITION_CONFIG, defaultPartitions))
      implicit val max = sparkConf.getInt(SPARK_MAX_IN_PARTITION_CONFIG, sparkConf.getInt(MAX_IN_PARTITION_CONFIG, defaultMaxInPartition))
      implicit val min = sparkConf.getInt(SPARK_MIN_IN_PARTITION_CONFIG, sparkConf.getInt(MIN_IN_PARTITION_CONFIG, defaultMinInPartition))
      implicit val requestTimeout = sparkConf.getLong(SPARK_REQUEST_TIMEOUT_CONFIG, sparkConf.getLong(REQUEST_TIMEOUT_CONFIG, defaultRequestTimeout))
      implicit val concurrentSave = sparkConf.getInt(SPARK_CONCURRENT_SAVE_CONFIG, sparkConf.getInt(CONCURRENT_SAVE_CONFIG, defaultConcurrentSave))
      implicit val bulkSize = sparkConf.getInt(SPARK_BULK_SIZE_CONFIG, sparkConf.getInt(BULK_SIZE_CONFIG, defaultBulkSize))

      var varSchemaSampleSize = sparkConf.get(SPARK_SCHEMA_SAMPLE_SIZE_CONFIG, schemaSampleSize)
      if (varSchemaSampleSize == null && sparkConf.contains(SCHEMA_SAMPLE_SIZE_CONFIG)){
        varSchemaSampleSize = sparkConf.get(SCHEMA_SAMPLE_SIZE_CONFIG)
      }
      
      val intSchemaSampleSize = calculateSchemaSampleSize(varSchemaSampleSize);

      println(s"Use connectorVersion=$CLOUDANT_CONNECTOR_VERSION, dbName=$dbName, " +
          s"indexName=$indexName, viewName=$viewName," +
          s"$PARTITION_CONFIG=$total, + $MAX_IN_PARTITION_CONFIG=$max," +
          s"$MIN_IN_PARTITION_CONFIG=$min, $REQUEST_TIMEOUT_CONFIG=$requestTimeout," +
          s"$CONCURRENT_SAVE_CONFIG=$concurrentSave, $BULK_SIZE_CONFIG=$bulkSize," +
          s"$SCHEMA_SAMPLE_SIZE_CONFIG=$intSchemaSampleSize")

      val protocol = sparkConf.get(SPARK_CLOUDANT_PROTOCOL_CONFIG, sparkConf.get(CLOUDANT_PROTOCOL_CONFIG, "https"))
      val host = sparkConf.get(SPARK_CLOUDANT_HOST_CONFIG, sparkConf.get(CLOUDANT_HOST_CONFIG, null))
      val user = sparkConf.get(SPARK_CLOUDANT_USERNAME_CONFIG, sparkConf.get(CLOUDANT_USERNAME_CONFIG, null))
      val passwd = sparkConf.get(SPARK_CLOUDANT_PASSWORD_CONFIG, sparkConf.get(CLOUDANT_PASSWORD_CONFIG, null))
      
      if (host != null) {
        return new CloudantConfig(protocol, host,  dbName, indexName, viewName, intSchemaSampleSize)(user, passwd, total, max, min,requestTimeout,concurrentSave, bulkSize)
      }else{
        throw new RuntimeException("Spark configuration is invalid! Please make sure to supply required values for cloudant.host.")
      }
      null
  }
  
  /**
   * The configuration sequence: DataFrame option override SparkConf override defaults
   */
  
  def getConfig(context: SQLContext,  parameters: Map[String, String]): CloudantConfig = {
    
      val sparkConf = context.sparkContext.getConf

      val totalS = sparkConf.get(SPARK_PARTITION_CONFIG, parameters.getOrElse(PARTITION_CONFIG, null))
      implicit val total = if (totalS ==null) sparkConf.getInt(PARTITION_CONFIG,defaultPartitions) else totalS.toInt
      val maxS = sparkConf.get(SPARK_MAX_IN_PARTITION_CONFIG, parameters.getOrElse(MAX_IN_PARTITION_CONFIG, null))
      implicit val max = if (maxS ==null) sparkConf.getInt(MAX_IN_PARTITION_CONFIG,defaultMaxInPartition) else maxS.toInt
      val minS = sparkConf.get(SPARK_MIN_IN_PARTITION_CONFIG, parameters.getOrElse(MIN_IN_PARTITION_CONFIG, null))
      implicit val min = if (minS ==null) sparkConf.getInt(MIN_IN_PARTITION_CONFIG,defaultMinInPartition) else minS.toInt

      val requestTimeoutS = sparkConf.get(SPARK_REQUEST_TIMEOUT_CONFIG, parameters.getOrElse(REQUEST_TIMEOUT_CONFIG, null))
      implicit val requestTimeout = if (requestTimeoutS == null) sparkConf.getLong(REQUEST_TIMEOUT_CONFIG, defaultRequestTimeout) else requestTimeoutS.toLong
  
      val concurrentSaveS = sparkConf.get(SPARK_CONCURRENT_SAVE_CONFIG, parameters.getOrElse(CONCURRENT_SAVE_CONFIG, null))
      implicit val concurrentSave = if (concurrentSaveS == null) sparkConf.getInt(CONCURRENT_SAVE_CONFIG, defaultConcurrentSave) else concurrentSaveS.toInt
      val bulkSizeS = sparkConf.get(SPARK_BULK_SIZE_CONFIG,parameters.getOrElse(PARAM_BULK_SIZE_CONFIG, null))
      implicit val bulkSize = if (bulkSizeS == null) sparkConf.getInt(BULK_SIZE_CONFIG, defaultBulkSize) else bulkSizeS.toInt

      val dbName = parameters.getOrElse("database", parameters.getOrElse("path",null))
      val indexName = parameters.getOrElse("index",null)
      val viewName = parameters.getOrElse("view", null)

      var schemaSampleSize = sparkConf.get(SPARK_SCHEMA_SAMPLE_SIZE_CONFIG, parameters.getOrElse(PARAM_SCHEMA_SAMPLE_SIZE_CONFIG, null))
      if (schemaSampleSize == null && sparkConf.contains(SCHEMA_SAMPLE_SIZE_CONFIG) ) {
        schemaSampleSize = sparkConf.get(SCHEMA_SAMPLE_SIZE_CONFIG)
      }
      
      val intSchemaSampleSize = calculateSchemaSampleSize(schemaSampleSize);
      
      println(s"Use connectorVersion=$CLOUDANT_CONNECTOR_VERSION, dbName=$dbName, " +
          s"indexName=$indexName, viewName=$viewName," +
          s"$PARTITION_CONFIG=$total, + $MAX_IN_PARTITION_CONFIG=$max," +
          s"$MIN_IN_PARTITION_CONFIG=$min, $REQUEST_TIMEOUT_CONFIG=$requestTimeout," +
          s"$CONCURRENT_SAVE_CONFIG=$concurrentSave, $BULK_SIZE_CONFIG=$bulkSize," +
          s"$SCHEMA_SAMPLE_SIZE_CONFIG=$intSchemaSampleSize")

      val protocolParam = sparkConf.get(SPARK_CLOUDANT_PROTOCOL_CONFIG, parameters.getOrElse(CLOUDANT_PROTOCOL_CONFIG, null))
      val hostParam = sparkConf.get(SPARK_CLOUDANT_HOST_CONFIG, parameters.getOrElse(CLOUDANT_HOST_CONFIG, null))
      val userParam = sparkConf.get(SPARK_CLOUDANT_USERNAME_CONFIG, parameters.getOrElse(CLOUDANT_USERNAME_CONFIG, null))
      val passwdParam = sparkConf.get(SPARK_CLOUDANT_PASSWORD_CONFIG, parameters.getOrElse(CLOUDANT_PASSWORD_CONFIG, null))
      val protocol = if (protocolParam == null) sparkConf.get(CLOUDANT_PROTOCOL_CONFIG, "https") else protocolParam
      val host = if (hostParam == null) sparkConf.get(CLOUDANT_HOST_CONFIG, null) else hostParam
      val user = if (userParam == null) sparkConf.get(CLOUDANT_USERNAME_CONFIG, null) else userParam
      val passwd = if (passwdParam == null) sparkConf.get(CLOUDANT_PASSWORD_CONFIG, null) else passwdParam
      if (host != null) {
        return new CloudantConfig(protocol, host, dbName, indexName, viewName, intSchemaSampleSize)(user, passwd, total, max, min, requestTimeout, concurrentSave, bulkSize)
      } else {
        throw new RuntimeException("Spark configuration is invalid! Please make sure to supply required values for cloudant.host.")
      }
  }

  def getConfig(parameters: Map[String, String]): CloudantConfig = {

    val requestTimeoutS = parameters.getOrElse(REQUEST_TIMEOUT_CONFIG, null)
    implicit val requestTimeout = if (requestTimeoutS == null) defaultRequestTimeout else requestTimeoutS.toLong
    val dbName = parameters.getOrElse("database", null)

    println(s"Use connectorVersion=$CLOUDANT_CONNECTOR_VERSION, dbName=$dbName, " +
      s"$REQUEST_TIMEOUT_CONFIG=$requestTimeout")
    val protocol = parameters.getOrElse(CLOUDANT_PROTOCOL_CONFIG, "https")
    val host = parameters.getOrElse(CLOUDANT_HOST_CONFIG, null)
    val user = parameters.getOrElse(CLOUDANT_USERNAME_CONFIG, null)
    val passwd = parameters.getOrElse(CLOUDANT_PASSWORD_CONFIG, null)
    if (host != null) {
      new CloudantConfig(protocol, host, dbName)(user, passwd, defaultPartitions, defaultMaxInPartition, defaultMinInPartition, requestTimeout, defaultConcurrentSave, defaultBulkSize)
    } else {
      throw new RuntimeException("Cloudant parameters are invalid! Please make sure to supply required values for cloudant.host.")
    }
  }

}
