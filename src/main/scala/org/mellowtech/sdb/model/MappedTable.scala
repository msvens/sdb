package org.mellowtech.sdb.model

import org.mellowtech.sdb.SRow
import scala.reflect.ClassTag
import org.mellowtech.sdb.{Db,KeyRow,OptKeyRow, ColumnName}

trait RowMapper[A, B] {
  
  def to(b: B): OptKeyRow[A] = {
    val l = list(b)
    val optKey: Option[A] = l._1
    val cols = l._2
    val row = cols.foldLeft(SRow()){(r,kv) => kv._2 match {
      case None => r //column empty
      case Some(v) => r + (kv._1,v)
      case _ => r + (kv._1,kv._2)
      }
    }
    (optKey,row)
  }
  
  def from(a: KeyRow[A]): B
  def list(b: B): (Option[A],List[(ColumnName,Any)])
  //def toKey(b: B): Option[A]
}

abstract class MappedTable[A: Ordering, R](val name: String)(implicit val db: Db, ct: ClassTag[A]) {
  import org.mellowtech.sdb.DbType._
  import org.mellowtech.sdb._

  val ktype = dbType[A]
  var thead = TableHeader(name, ktype)
  var searchable = false

  def primaryColumn[B: ClassTag](name: String, options: COption[B]*) = {
    column(name, (COption.Primary +: options): _*)
  }
  
  def O = new AnyRef with COptions

  def column[B: ClassTag](name: String, options: COption[B]*): ColumnHeader = {
    val vtype = dbType[B]
    val oset = options.toSet
    val h = oset.foldLeft(ColumnHeader(name, this.name, ktype, vtype))((a, b) => {
      b match {
        case COption.Sorted       => a.copy(sorted = true)
        case COption.Primary      => { thead = thead.copy(primColumn = Some(name)); a }
        case COption.Length(x, y) => a.copy(maxValueSize = Some(x))
        case COption.FieldSearch  => { searchable = true; ; a.copy(search = SearchType.FIELD) }
        case COption.TextSearch   => { searchable = true; a.copy(search = SearchType.TEXT) }
        case COption.Nullable     => a.copy(nullable = true)
        case COption.NotNull      => a.copy(nullable = false)
        case _                    => a
      }
    })
    
    if (oset(COption.Primary) && oset(COption.Nullable)) thead = thead.copy(genKey = true)
    if(searchable && !thead.indexed) thead = thead.copy(indexed = true)
    
    h
  }

  def headers: Seq[ColumnHeader]

  def * : RowMapper[A, R]

  lazy val table: STable[A] = db.get[A](name) match {
      case Some(t) => t
      case None =>
        db.+=[A](thead)
        val t = db[A](name)
        for (c <- headers) t.addColumn(c)
        t
  }

  def get(id: A): Option[R] = table row id match {
    case None    => None
    case Some(r) => Some(* from (id,r))
  }

  def find[B](cn: ColumnName, v: B): Iterator[R] = for (r <- table.find(cn, v)) yield (* from r)

  def +(r: R): Unit = {
    val row = * to r
    //println(row)
    table + row
  }
  
  def -(k: A): Unit = table - k
  
  

}