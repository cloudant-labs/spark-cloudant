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
import play.api.libs.json.JsValue
import play.api.libs.json.JsSuccess
import play.api.libs.json.JsError
import play.api.libs.json.Json
import scala.util.control.Breaks._
import play.api.libs.json.JsUndefined
import java.net.URLEncoder
import com.typesafe.config.ConfigFactory
import scala.concurrent.duration.Duration
import com.cloudant.spark.riak.RiakConfig
import com.cloudant.spark.CloudantConfig

 object JsonStoreConfigManager
{
  val CLOUDANT_HOST_CONFIG = "cloudant.host"
  val CLOUDANT_USERNAME_CONFIG = "cloudant.username"
  val CLOUDANT_PASSWORD_CONFIG = "cloudant.password"
  val RIAK_HOST_CONFIG = "riak.host"
  val RIAK_PORT_CONFIG = "riak.port"
  
  val PARTITION_CONFIG = "jsonstore.rdd.partitions"
  val MAX_IN_PARTITION_CONFIG = "jsonstore.rdd.maxInPartition"
  val MIN_IN_PARTITION_CONFIG = "jsonstore.rdd.minInPartition"
  
  val configFactory = ConfigFactory.load()
  import java.util.concurrent.TimeUnit._

  val timeoutInMillis = Duration(configFactory.getDuration("spray.can.server.request-timeout", SECONDS),SECONDS).toMillis
  
  val ROOT_CONFIG_NAME = "spark-sql"
  val rootConfig = configFactory.getConfig(ROOT_CONFIG_NAME) 
  val defaultPartitions = rootConfig.getInt(PARTITION_CONFIG)
  val defaultMaxInPartition = rootConfig.getInt(MAX_IN_PARTITION_CONFIG)
  val defaultMinInPartition = rootConfig.getInt(MIN_IN_PARTITION_CONFIG)
  
  def getConfig(context: SQLContext, dbName: String, indexName:String = null): JsonStoreConfig = {
      val sparkConf = context.sparkContext.getConf
      implicit val total = sparkConf.getInt(PARTITION_CONFIG,defaultPartitions)
      implicit val max = sparkConf.getInt(MAX_IN_PARTITION_CONFIG,defaultMaxInPartition)
      implicit val min =sparkConf.getInt(MIN_IN_PARTITION_CONFIG,defaultMinInPartition)
      if (sparkConf.contains(CLOUDANT_HOST_CONFIG))
      {
        val host = sparkConf.get(CLOUDANT_HOST_CONFIG)
        val user = sparkConf.get(CLOUDANT_USERNAME_CONFIG)
        val passwd = sparkConf.get(CLOUDANT_PASSWORD_CONFIG)
        return CloudantConfig(host,  dbName, indexName)(user, passwd, total, max, min)
      }
      if (sparkConf.contains(RIAK_HOST_CONFIG))
      {
        val host = sparkConf.get(RIAK_HOST_CONFIG)
        val port = sparkConf.get(RIAK_PORT_CONFIG)
        return RiakConfig(host, port, dbName)(partitions=total, maxInPartition=max, minInPartition=min)
      }
      null
  }
  

}