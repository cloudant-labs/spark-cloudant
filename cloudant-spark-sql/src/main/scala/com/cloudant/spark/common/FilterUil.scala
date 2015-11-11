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

import org.apache.spark.sql.sources._
import play.api.libs.json.JsValue
import play.api.libs.json.Json
import play.api.libs.json.JsSuccess
import play.api.libs.json.JsError
import scala.collection.immutable.StringOps
import play.api.libs.json.JsNumber
import play.api.libs.json.JsBoolean
import play.api.libs.json.JsString
import org.apache.spark.SparkEnv
import akka.event.Logging

/**
 * Only handles the following filter condition
 * 1. EqualTo,GreaterThan,LessThan,GreaterThanOrEqual,LessThanOrEqual,In
 * 2. recursive AND of (filters in 1 and AND). Issue: Spark 1.3.0 does not return AND filter instead returned 2 filters
 * @author yanglei
 */
class FilterInterpreter( origFilters: Array[Filter]){

	implicit val system = SparkEnv.get.actorSystem
			private val logger = Logging(system, getClass)

			lazy val firstField = {
					if (origFilters.length>0) getFilterAttribute(origFilters(0))
					else null
	}

	private lazy val filtersByAttr = { 
			origFilters
			.filter(f => getFilterAttribute(f)!=null)
			.map(f => (getFilterAttribute(f), f))
			.groupBy(attrFilter => attrFilter._1)
			.mapValues(a => a.map(p => p._2))
	}

	private def getFilterAttribute(f: Filter): String = {
			val result = f match {
			case EqualTo(attr, v) => attr
			case GreaterThan(attr, v) => attr
			case LessThan(attr, v) => attr
			case GreaterThanOrEqual(attr, v) => attr
			case LessThanOrEqual(attr, v) => attr
			case In(attr, v) => attr
			case IsNotNull(attr) =>attr
			case IsNull(attr) => attr
			case _  => null
			}
			result
	}

	def containsFiltersFor(key: String): Boolean = {
			filtersByAttr.contains(key)
	}

	private lazy val analyzedFilters ={
			filtersByAttr.map(m => m._1 -> analyze(m._2))
	}

	private def analyze(filters: Array[Filter]):(Any, Boolean, Any,Boolean, Array[Filter]) =  {

			var min:Any = null
					var minInclusive: Boolean = false
					var max: Any = null
					var maxInclusive:Boolean = false
					var others: Array[Filter] = Array[Filter]()

					def evaluate(filter: Filter) {
		filter match{
		case GreaterThanOrEqual(attr, v) => {min = v; minInclusive=true}
		case LessThanOrEqual(attr, v) => {max = v;maxInclusive=true}
		case EqualTo(attr,v) => {min = v;  max = v}
		case GreaterThan(attr, v) => {min = v}
		case LessThan(attr, v) =>  {max = v}
		case _ => {others  = others :+  filter}
		}
	}

	filters.map(f=>evaluate(f))

	logger.info(s"Calculated range info: min=$min, minInclusive=$minInclusive,max=$max,maxInclusive=$maxInclusive, others=$others")
	(min, minInclusive,max,maxInclusive, others)
	}

	def getInfo(field: String):(Any, Boolean, Any,Boolean) ={
			if (field==null) ( null, false, null,false)
			else {
				val data  =analyzedFilters.getOrElse(field, ( null, false, null,false, null))
						(data._1,data._2,data._3, data._4)
			}
	}

	def getFiltersForPostProcess(pushdownField: String) ={
			filtersByAttr.map(f=>{
				if (f._1.equals(pushdownField)) f._1-> analyzedFilters.get(pushdownField).get._5
				else f._1 -> f._2
			})
	}
}

/**
 * @author yanglei
 */
class FilterUtil(filters: Map[String, Array[Filter]]){

	implicit val system = SparkEnv.get.actorSystem
			private val logger = Logging(system, getClass)

			def  apply (implicit r: JsValue =null): Boolean = {
					if (r == null) return true
							val satisfied = filters.forall({
							case (attr, filters) =>{
								val field = JsonUtil.getField(r, attr).getOrElse(null)
										if (field == null)
										{
											logger.info(s"field $attr not exisit:$r")
											false
										}else
										{
											if (field.isInstanceOf[JsNumber]) {
												if (field.as[JsNumber].value.getClass.equals(Double)) satisfiesAll(field.as[JsNumber].value.doubleValue(), filters)
												else if (field.as[JsNumber].value.getClass.equals(scala.Float)) satisfiesAll(field.as[JsNumber].value.floatValue(), filters)
												else if (field.as[JsNumber].value.getClass.equals(Long)) satisfiesAll(field.as[JsNumber].value.longValue(), filters)
												else if (field.as[JsNumber].value.getClass.equals(Int)) satisfiesAll(field.as[JsNumber].value.intValue(), filters)
												else if (field.as[JsNumber].value.getClass.equals(Short)) satisfiesAll(field.as[JsNumber].value.shortValue(), filters)
												else if (field.as[JsNumber].value.getClass.equals(Byte)) satisfiesAll(field.as[JsNumber].value.byteValue(), filters)
												else true
											}
											else if (field.isInstanceOf[JsBoolean])  satisfiesAll(field.as[JsBoolean].value, filters)
											else if (field.isInstanceOf[JsString]) satisfiesAll(field.as[JsString].value, filters)
											else true
										}
							}
							})
							satisfied
	}

	private def satisfiesAll(value: Double, filters: Array[Filter]): Boolean = {
			val satisfied = filters.forall({
			case EqualTo(attr, v) => value.equals(v)
			case GreaterThan(attr, v) => value > v.asInstanceOf[Double]
			case LessThan(attr, v) =>  value < v.asInstanceOf[Double]
			case GreaterThanOrEqual(attr, v) => value >= v.asInstanceOf[Double]
			case LessThanOrEqual(attr, v) => value <= v.asInstanceOf[Double]
			case In(attr, vs) => vs.exists(v => value.equals(asInstanceOf[Double]))
			case IsNotNull(attr) => value !=null
			case IsNull(attr) => value == null
			case _ => true
			})
					satisfied
	}

	private def satisfiesAll(value: Float, filters: Array[Filter]): Boolean = {
			val satisfied = filters.forall({
			case EqualTo(attr, v) => value.equals(v)
			case GreaterThan(attr, v) => value > v.asInstanceOf[Float]
			case LessThan(attr, v) =>  value < v.asInstanceOf[Float]
			case GreaterThanOrEqual(attr, v) => value >= v.asInstanceOf[Float]
			case LessThanOrEqual(attr, v) => value <= v.asInstanceOf[Float]
			case In(attr, vs) => vs.exists(v => value.equals(asInstanceOf[Float]))
			case IsNotNull(attr) => value !=null
			case IsNull(attr) => value == null
			case _ => true
			})
					satisfied
	}

	private def satisfiesAllNumber(value: Long, filters: Array[Filter]): Boolean = {
			val satisfied = filters.forall({
			case EqualTo(attr, v) => value.equals(v)
			case GreaterThan(attr, v) => value > v.asInstanceOf[Long]
			case LessThan(attr, v) =>  value < v.asInstanceOf[Long]
			case GreaterThanOrEqual(attr, v) => value >= v.asInstanceOf[Long]
			case LessThanOrEqual(attr, v) => value <= v.asInstanceOf[Long]
			case In(attr, vs) => vs.exists(v => value.equals(asInstanceOf[Long]))
			case IsNotNull(attr) => value !=null
			case IsNull(attr) => value == null
			case _ => true
			})
					satisfied
	}

	private def satisfiesAllNumber(value: Integer, filters: Array[Filter]): Boolean = {
			val satisfied = filters.forall({
			case EqualTo(attr, v) => value.equals(v)
			case GreaterThan(attr, v) => value > v.asInstanceOf[Integer]
			case LessThan(attr, v) =>  value < v.asInstanceOf[Integer]
			case GreaterThanOrEqual(attr, v) => value >= v.asInstanceOf[Integer]
			case LessThanOrEqual(attr, v) => value <= v.asInstanceOf[Integer]
			case In(attr, vs) => vs.exists(v => value.equals(asInstanceOf[Integer]))
			case IsNotNull(attr) => value !=null
			case IsNull(attr) => value == null
			case _ => true
			})
					satisfied
	}

	private def satisfiesAllNumber(value: Short, filters: Array[Filter]): Boolean = {
			val satisfied = filters.forall({
			case EqualTo(attr, v) => value.equals(v)
			case GreaterThan(attr, v) => value > v.asInstanceOf[Short]
			case LessThan(attr, v) =>  value < v.asInstanceOf[Short]
			case GreaterThanOrEqual(attr, v) => value >= v.asInstanceOf[Short]
			case LessThanOrEqual(attr, v) => value <= v.asInstanceOf[Short]
			case In(attr, vs) => vs.exists(v => value.equals(asInstanceOf[Short]))
			case IsNotNull(attr) => value !=null
			case IsNull(attr) => value == null
			case _ => true
			})
					satisfied
	}

	private def satisfiesAllNumber(value: Byte, filters: Array[Filter]): Boolean = {
			val satisfied = filters.forall({
			case EqualTo(attr, v) => value.equals(v)
			case GreaterThan(attr, v) => value > v.asInstanceOf[Byte]
			case LessThan(attr, v) =>  value < v.asInstanceOf[Byte]
			case GreaterThanOrEqual(attr, v) => value >= v.asInstanceOf[Byte]
			case LessThanOrEqual(attr, v) => value <= v.asInstanceOf[Byte]
			case In(attr, vs) => vs.exists(v => value.equals(asInstanceOf[Byte]))
			case IsNotNull(attr) => value !=null
			case IsNull(attr) => value == null
			case _ => true
			})
					satisfied
	}



	private def satisfiesAll(value: String, filters: Array[Filter]): Boolean = {
			val satisfied = filters.forall({
			case EqualTo(attr, v:String) => value.equals(v.asInstanceOf[String])
			case GreaterThan(attr, v:String) => new StringOps(value) > v.asInstanceOf[String]
			case LessThan(attr, v:String) => new StringOps(value) < v.asInstanceOf[String]
			case GreaterThanOrEqual(attr, v: String) => new StringOps(value) >= v.asInstanceOf[String]
			case LessThanOrEqual(attr, v:String) => new StringOps(value) <= v.asInstanceOf[String]
					//case In(attr, vs) => vs.exists(v => value.equals(v.asInstanceOf[String]))
			case IsNotNull(attr) => value!=null
			case IsNull(attr) => value == null
			case _ => true
			})
					satisfied
	}

	private def satisfiesAll(value: Boolean, filters: Array[Filter]): Boolean = {
			val satisfied = filters.forall({
			case EqualTo(attr, v) => value.equals(v.asInstanceOf[Boolean])
			case In(attr, vs) => vs.exists(v => value.equals(asInstanceOf[Boolean]))
			case IsNotNull(attr) => value!=null
			case IsNull(attr) => value == null
			case _ => false
			})
					satisfied
	}

}

