package com.socrata.datacoordinator.service

import com.rojoma.json.ast.{JNull, JValue, JObject}

import com.socrata.datacoordinator.truth.{JsonDataWritingContext, DataReadingContext}

class Exporter(val dataContext: DataReadingContext with JsonDataWritingContext) {
  def export(id: String, columns: Option[Set[String]])(f: Iterator[JObject] => Unit): Boolean = {
    val res = for {
      ctxOpt <- dataContext.datasetReader.openDataset(id, latest = true)
      ctx <- ctxOpt
    } yield {
      import ctx._
      val jsonSchema = schema.filter { case (cid, ci) =>
        columns match {
          case Some(set) => set(ci.logicalName)
          case None => true
        }
      }.mapValuesStrict(dataContext.jsonRepForColumn)
      withRows { it =>
        val objectified = it.map { row =>
          val res = new scala.collection.mutable.HashMap[String, JValue]
          row.foreach { case (cid, value) =>
            if(jsonSchema.contains(cid)) {
              val rep = jsonSchema(cid)
              val v = rep.toJValue(value)
              if(JNull != v) res(rep.name) = v
            }
          }
          JObject(res)
        }
        f(objectified)
      }
    }

    res.isDefined
  }
}
