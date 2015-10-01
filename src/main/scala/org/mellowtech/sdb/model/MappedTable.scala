package org.mellowtech.sdb.model

import org.mellowtech.sdb.SRow
import scala.reflect.ClassTag
import org.mellowtech.sdb.{Db,KeyRow,OptKeyRow, ColumnName}
import org.mellowtech.sdb.TableType._

abstract class COption[+A]
abstract class TOption[+A]

object COption {
  case object Optional extends COption[Nothing]
  case object Forced extends COption[Nothing]
  case object AutoInc extends COption[Nothing]
  case object Sorted extends COption[Nothing]
  case object Primary extends COption[Nothing]
  case object FieldSearch extends COption[Nothing]
  case object TextSearch extends COption[Nothing]
  case class Length(length: Int, varying: Boolean = true) extends COption[Nothing]
}

trait COptions {
  def Optional = COption.Optional
  def Sorted = COption.Sorted
  def Primary = COption.Primary
  def NotNull = COption.Forced
  def Length = COption.Length
  def FieldSearch = COption.FieldSearch
  def TextSearch = COption.TextSearch
}

object TOption {
  case object AutoInc extends TOption[Nothing]
  case object Sorted extends TOption[Nothing]
  case object Hashed extends TOption[Nothing]
  case class Type(t: TableType) extends TOption[Nothing]
  case object Logged extends TOption[Nothing]
  case object Indexed extends TOption[Nothing]
}

trait TOptions {
  def AutoInc = TOption.AutoInc
  def Sorted = TOption.Sorted
  def Hashed = TOption.Hashed
  def Type = TOption.Type
  def Logged = TOption.Logged
  def Indexed = TOption.Indexed
}

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
  //var thead = TableHeader(name, ktype)
  var searchable = false

  def primaryColumn[B: ClassTag](name: String, options: COption[B]*) = {
    column(name, (COption.Primary +: options): _*)
  }
  
  def O = new AnyRef with COptions
  
  def T = new AnyRef with TOptions
  
  def tableProperties: List[TOption[A]] = List(T.Sorted, T.Type(TableType.ROW))
  
  var thead = tableProperties.foldLeft(TableHeader(name,ktype))((th, p) => p match {
    case TOption.Sorted => th.copy(sorted = true)
    case TOption.Hashed => th.copy(sorted = false)
    case TOption.AutoInc => th.copy(genKey = true)
    case TOption.Logged => th.copy(logging = true)
    case TOption.Type(t) => th.copy(tableType = t)
    case TOption.Indexed => th.copy(indexed = true)
    case _ => th
  })
  
  private var _headers: List[ColumnHeader] = Nil

  def column[B: ClassTag](name: String, options: COption[B]*): Unit = {
    val vtype = dbType[B]
    val oset = options.toSet
    val h = oset.foldLeft(ColumnHeader(name, this.name, ktype, vtype))((a, b) => {
      b match {
        case COption.Sorted       => a.copy(sorted = true)
        case COption.Primary      => { thead = thead.copy(primColumn = Some(name)); a }
        case COption.Length(x, y) => a.copy(maxValueSize = Some(x))
        case COption.FieldSearch  => { searchable = true; ; a.copy(search = SearchType.FIELD) }
        case COption.TextSearch   => { searchable = true; a.copy(search = SearchType.TEXT) }
        case COption.Optional     => a.copy(nullable = true)
        case COption.Forced      => a.copy(nullable = false)
        case _                    => a
      }
    })
    
    if (oset(COption.Primary) && oset(COption.Optional))
      throw new Error("primary column cannot be optional")
    
    if(searchable && !thead.indexed) thead = thead.copy(indexed = true)
    _headers = h :: _headers
  }

  final def headers = _headers

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