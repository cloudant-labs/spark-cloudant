package com.cloudant.spark

import play.api.libs.json.{JsUndefined, JsValue}
import scala.util.control.Breaks._

object JsonUtil{
  def getField(row: JsValue, field: String) : Option[JsValue] = {
    var path = field.split('.')
    var currentValue = row
    var finalValue: Option[JsValue] = None
    breakable {
      for (i <- path.indices){
        val f: Option[JsValue] = (currentValue \ path(i)).toOption
        f match {
          case Some(f2) =>  currentValue = f2
          case None => break
        }
        if (i == path.length -1) //The leaf node
          finalValue = Some(currentValue)
      }
    }
    finalValue
  }
}
