package org.mellowtech.dbm.model

/**
 * @author msvens
 */

abstract class COption[+A]

object COption {
  case object Nullable extends COption[Nothing]
  case object NotNull extends COption[Nothing]
  case object AutoInc extends COption[Nothing]
  case object Sorted extends COption[Nothing]
  case object Primary extends COption[Nothing]
  case object FieldSearch extends COption[Nothing]
  case object TextSearch extends COption[Nothing]
  case class Length(length: Int, varying: Boolean = true) extends COption[Nothing]
}

trait COptions {
  def Nullable = COption.Nullable
  def AutoInc = COption.AutoInc
  def Sorted = COption.Sorted
  def Primary = COption.Primary
  def NotNull = COption.NotNull
  def Length = COption.Length
  def FieldSearch = COption.FieldSearch
  def TextSearch = COption.TextSearch
}

object simple {
  import org.mellowtech.dbm._
  import org.mellowtech.dbm.DbType._
  import scala.reflect.ClassTag
  
  def mapify[T: Mappable](t: T) = implicitly[Mappable[T]].toMap(t)
  def materialize[T: Mappable](map: Map[String, Any]) = implicitly[Mappable[T]].fromMap(map)
  
  implicit def toHeaders(s: Seq[SimpleColumn[_]]): Seq[ColumnHeader] = {
    val c = for {
      i <- s
    } yield(i.h)
    c
  }
  
  implicit def cTt(t: (ColumnHeader,Any)):(String,Any) = t._1.nullable match {
    case true => (t._1.name, Some(t._2))
    case false => (t._1.name, t._2)
  }
  
  case class SimpleColumn[A](h: ColumnHeader)
  
  abstract class SimpleTable[A : ClassTag,R: Mappable](val name: String)(implicit val db: Db) {
    
    import Mappable._
    import scala.util
    
    val ktype = dbType[A]
    var thead = TableHeader(name, ktype)
    var searchable = false
    
    def O = new AnyRef with COptions
    
    def column[B : ClassTag](name: String, options: COption[B]*): SimpleColumn[B] = {
      val vtype = dbType[B]
      val h = options.foldLeft(ColumnHeader(name, this.name, vtype, ktype))((a,b) => {
        b match {
          case COption.Sorted => a.copy(sorted = true)
          case COption.Primary => {thead = thead.copy(primColumn = Some(name)); a}
          case COption.Length(x,y) => a.copy(maxValueSize = Some(x))
          case COption.FieldSearch => {searchable = true; ;a.copy(search = SearchType.FIELD)}
          case COption.TextSearch => {searchable = true; a.copy(search = SearchType.TEXT)}
          case COption.Nullable => a.copy(nullable = true)
          case COption.NotNull => a.copy(nullable = false)
          case _ => a
        }
      })
      SimpleColumn(h)
    }
    
    def * : Seq[SimpleColumn[_]]
    
    def headers: Seq[ColumnHeader] = *
    
    def idc: SimpleColumn[A]
    
    lazy val table: Table[A] = {
      val t = db.get[A](name) match {
        case Some(t) => t
        case None => {
          db.+=[A](thead)
          val t = db[A](name)
          for(c <- headers) t.addCol(c)
          t
        }
      }
      if(searchable) Table.searchable(t) else t
    }
    
    private def conv(r: Row[A]): R = {
      val p: (String,Any) = cTt((idc.h, r.key))
      val t = addOption(r.iterator.toMap) + p
      materialize[R](t.withDefaultValue(None))
    }
    
    def get(id: A): Option[R] = table rowIf id match {
      case None => None
      case Some(r) => {
        val p: (String,Any) = cTt((idc.h, r.key))
        val t = addOption(r.iterator.toMap) + p
        Some(materialize[R](t.withDefaultValue(None))) 
      } 
    }
    
    def find[B](cn: ColumnName, v: B): Iterator[R] = for(r <- table.find(cn, v)) yield(conv(r))
    
    def ins(r: R)(implicit ord: Ordering[A]): Unit = {
      val m = noOption(mapify(r))
      println(m)
      val k: A = noOp(m(idc.h.name)).asInstanceOf[A]
      val row = Row(k, m - idc.h.name)
      table.+=(row)
    }
    
    private def noOp[A](a: Any): Any = a match {
      case Some(b) => b
      case None => null
      case _ => a
    }
    
    
    
    private def noOption(m: Map[String,Any]): Map[String,Any] = m.foldLeft(Map[String,Any]())((b,kv) => kv._2 match {
      case Some(vv) => m + ((kv._1, vv))
      case None => m - kv._1
      case _ => m + kv
    })
    
    private def addOption(m: Map[String,Any]): Map[String,Any] = m.foldLeft(Map[String,Any]())((b,kv) => {
       if(table.columnHeader(kv._1).get.nullable)
         b + ((kv._1,Some(kv._2)))
       else
         b + kv
      })
    


    
  }
  
}