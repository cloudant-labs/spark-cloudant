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

import com.cloudant.spark.CloudantConfig
import org.apache.spark.SparkContext
import org.apache.spark.Partition
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import play.api.libs.json.JsValue
import org.apache.spark.sql.sources.Filter
import org.apache.spark.SparkEnv
import akka.event.Logging


/**
 * @author yanglei
 * JsonStoreRDDPartition defines each partition as a subset of a query result:
  * the limit rows returns and the skipped rows.
 */

private[spark] class JsonStoreRDDPartition(val skip: Int, val limit: Int,
    val idx: Int, val config: CloudantConfig,
    val attrToFilters: Map[String, Array[Filter]])
    extends Partition with Serializable{
  val index = idx
}

/**
 * @author yanglei
 *  Create parallel read by partitioning getAll()
 *  Partiotions are calculated based on:
 *  1) Database size, so that a single request to db doesn't exceed 1Mb
 *  2) If (1) doesn't work, partitions will be calculated based on
 *  config parameters minInPartition, maxInPartition
 *  maxRowsInPartition: -1 means unlimited
 */
class JsonStoreRDD(@transient sc: SparkContext, config: CloudantConfig,
    url: String)(implicit requiredcolumns: Array[String] = null,
    attrToFilters: Map[String, Array[Filter]]=null)
    extends RDD[String](sc, Nil) {

  override def getPartitions: Array[Partition] = {
    var(numOfPartitions:Int, limitPerPartition:Int, totalRows:Int) =
        new JsonStoreDataAccess(config).getPartitions(url)

    implicit val system = SparkEnv.get.actorSystem
    val logger = Logging(system, getClass)
    logger.info(s"*** Using number of partitions:$numOfPartitions, " +
        s"limit per partition:$limitPerPartition for totalRows of $totalRows")

    if (numOfPartitions == 0) {
      //we could not calculate partitions based on db size,
      //calculate them based on config parameters
      numOfPartitions = {
        if (totalRows == 0 || !config.allowPartition()) 1
        else if (totalRows < config.partitions * config.minInPartition) {
          val total = totalRows / config.minInPartition
          if (total == 0) total + 1
          else total
        }
        else if (config.maxInPartition <= 0) config.partitions
        else {
          val total = totalRows / config.maxInPartition
          if (totalRows % config.maxInPartition != 0) total + 1
          else total
        }
      }
      limitPerPartition = totalRows / numOfPartitions
    }
    val remainder = totalRows - (limitPerPartition*numOfPartitions)

    (0 until numOfPartitions).map(i => {
      val skip = i * limitPerPartition
      if (i == (numOfPartitions-1)){
        limitPerPartition += remainder
      }
      new JsonStoreRDDPartition(skip, limitPerPartition, i, config,
        attrToFilters).asInstanceOf[Partition]
    }).toArray
  }

  override def compute(splitIn: Partition, context: TaskContext): 
      Iterator[String] = {
    val myPartition = splitIn.asInstanceOf[JsonStoreRDDPartition]
    new JsonStoreDataAccess(myPartition.config).getIterator(myPartition.skip,
        myPartition.limit, url)
  }
}
