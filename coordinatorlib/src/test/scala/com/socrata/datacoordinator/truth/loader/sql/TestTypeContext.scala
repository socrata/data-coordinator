package com.socrata.datacoordinator
package truth.loader
package sql

import scala.collection.JavaConverters._

import com.socrata.datacoordinator.truth.{RowUserIdMap, TypeContext}
import com.socrata.datacoordinator.id.RowId
import com.socrata.soql.environment.TypeName

object TestTypeContext extends TypeContext[TestColumnType, TestColumnValue] {
  def makeValueFromSystemId(id: RowId) = LongValue(id.underlying)
  def makeSystemIdFromValue(id: TestColumnValue) = {
    require(id.isInstanceOf[LongValue], "Not an id")
    new RowId(id.asInstanceOf[LongValue].value)
  }

  def isNull(v: TestColumnValue) = v == NullValue
  def nullValue = NullValue

  val types = Map(
    TypeName("long") -> LongColumn,
    TypeName("string") -> StringColumn
  )

  def typeFromNameOpt(name: TypeName) = types.get(name)
  def nameFromType(typ: TestColumnType) = typ match {
    case LongColumn => TypeName("long")
    case StringColumn => TypeName("string")
  }

  def makeIdMap[V](ignored: TestColumnType) = {
    new RowUserIdMap[TestColumnValue, V] {
      val m = new java.util.HashMap[TestColumnValue, V]
      def put(x: TestColumnValue, v: V) { m.put(x, v) }
      def apply(x: TestColumnValue) = { val r = m.get(x); if(r == null) throw new NoSuchElementException; r }
      def contains(x: TestColumnValue) = m.containsKey(x)

      def get(x: TestColumnValue) = Option(m.get(x))

      def clear() { m.clear() }

      def isEmpty = m.isEmpty

      def size = m.size

      def foreach(f: (TestColumnValue, V) => Unit) {
        val it = m.entrySet.iterator
        while(it.hasNext) {
          val ent = it.next()
          f(ent.getKey, ent.getValue)
        }
      }

      def valuesIterator = m.values().iterator.asScala
    }
  }
}
