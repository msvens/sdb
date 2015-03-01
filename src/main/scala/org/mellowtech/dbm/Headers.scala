package org.mellowtech.dbm

/**
 * @author msvens
 *
 */
import DbType._
import TableType._
import SearchType._
import org.json4s._
import org.json4s.native.Serialization
import org.json4s.ext._
import org.json4s.native.Serialization.{ read, write }

case class TableHeader(name: String,
                       keyType: DbType = STRING, primColumn: Option[ColumnName] = None, cacheSize: Int = -1, maxKeySize: Option[Int] = None, maxRowSize: Option[Int] = None, highId: Long = 1,
                       cached: Boolean = false, sorted: Boolean = true, logging: Boolean = false, tableType: TableType = ROW, indexed: Boolean = false, genKey: Boolean = false, memMap: Boolean = false, cacheUnit: Byte = 0)

case class ColumnHeader(
  name: ColumnName, table: String, keyType: DbType = STRING, valueType: DbType = STRING, sorted: Boolean = true,
  maxValueSize: Option[Int] = Some(256), maxKeySize: Option[Int] = Some(64), index: Integer = -1, cacheSize: Integer = -1, search: SearchType = NONE, nullable: Boolean = true)

class DbTypeSerializer extends CustomSerializer[DbType](format => (
  { case JObject(JField("$dbtype", JString(s)) :: Nil) => DbType.withName(s) },
  { case t: DbType => JObject(JField("$dbtype", JString(t.toString)) :: Nil) }))

class TableTypeSerializer extends CustomSerializer[TableType](format => (
  { case JObject(JField("$tabletype", JString(v)) :: Nil) => TableType.withName(v) },
  { case tt: TableType => JObject(JField("$tabletype", JString(tt.toString)) :: Nil) }))

object TableHeader {

  implicit val formats = Serialization.formats(NoTypeHints) + new EnumNameSerializer(DbType) + new EnumNameSerializer(TableType)

  def fromJson(json: String): TableHeader = read[TableHeader](json)
  def asJson(th: TableHeader): String = write(th)

}

object ColumnHeader {

  implicit val formats = Serialization.formats(NoTypeHints) + new EnumNameSerializer(DbType)

  def fromJson(json: String): ColumnHeader = read[ColumnHeader](json)
  def asJson(th: ColumnHeader): String = write(th)
  
  def in(th: TableHeader, name: String, valueType: DbType = STRING): ColumnHeader = {
    apply(name,th.name, th.keyType, valueType, sorted = th.sorted, maxKeySize = th.maxKeySize)
  }

}