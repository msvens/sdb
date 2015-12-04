package org.mellowtech.sdb.model

/**
 * @author msvens
 */

object simple {
  import org.mellowtech.sdb._
  import org.mellowtech.sdb.DbType._
  import scala.reflect.ClassTag
  
  /*def mapify[T: Mappable](t: T) = implicitly[Mappable[T]].toMap(t)
  def materialize[T: Mappable](map: Map[String, Any]) = implicitly[Mappable[T]].fromMap(map)
  
  def anyToOption[A](a: Any): Option[A] = a match {
      case Some(b) => Some(b.asInstanceOf[A])
      case None => None
      case _ => Some(a.asInstanceOf[A])
    }
  
  def optionToNullable(a: Any): Any = a match {
    case Some(b) => b
    case None => null
    case _ => a
  }
  
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
  
  case class SimpleColumn[A](h: ColumnHeader)*/


  
  
  
//  abstract class SimpleTable[A: Ordering,R: Mappable](val name: String)(implicit val db: Db, ct: ClassTag[A]) {
//    
//    //import Mappable._
//    //import scala.util
//    
//    val ktype = dbType[A]
//    var thead = TableHeader(name, ktype)
//    var searchable = false
//    
//    def O = new AnyRef with COptions
//
//    def primaryColumn[B: ClassTag](name: String, options: COption[B]*) = {
//      column(name, (COption.Primary +: options):_*)
//    }
//    
//    def column[B : ClassTag](name: String, options: COption[B]*): SimpleColumn[B] = {
//      val vtype = dbType[B]
//      val oset = options.toSet
//      val h = oset.foldLeft(ColumnHeader(name, this.name, ktype, vtype))((a,b) => {
//        b match {
//          case COption.Sorted => a.copy(sorted = true)
//          case COption.Primary => {thead = thead.copy(primColumn = Some(name)); a}
//          case COption.Length(x,y) => a.copy(maxValueSize = Some(x))
//          case COption.FieldSearch => {searchable = true; ;a.copy(search = SearchType.FIELD)}
//          case COption.TextSearch => {searchable = true; a.copy(search = SearchType.TEXT)}
//          case COption.Optional => a.copy(nullable = true)
//          case COption.Forced => a.copy(nullable = false)
//          case _ => a
//        }
//      })
//      if(oset(COption.Primary) && oset(COption.Optional)){
//        thead = thead.copy(genKey = true)
//      }
//      SimpleColumn(h)
//    }
//    
//    
//    def * : Seq[SimpleColumn[_]]
//    
//    def headers: Seq[ColumnHeader] = *
//    
//    def idc: SimpleColumn[A]
//    
//    lazy val table: STable[A] = {
//      val t = db.get[A](name) match {
//        case Some(t) => t
//        case None => {
//          db.+=[A](thead)
//          val t = db[A](name)
//          for(c <- headers) t.addColumn(c)
//          t
//        }
//      }
//      if(searchable) STable.searchable(t) else t
//    }
//    
//    private def conv(r: KeyRow[A]): R = {
//      val p: (String,Any) = cTt((idc.h, r._1))
//      val t = addOption(r._2.iterator.toMap) + p
//      materialize[R](t.withDefaultValue(None))
//    }
//    
//    def get(id: A): Option[R] = table row id match {
//      case None => None
//      case Some(r) => {
//        val p: (String,Any) = cTt((idc.h, id))
//        val t = addOption(r.iterator.toMap) + p
//        Some(materialize[R](t.withDefaultValue(None))) 
//      } 
//    }
//    
//    def find[B](cn: ColumnName, v: B): Iterator[R] = for(r <- table.find(cn, v)) yield(conv(r))
//    
//    def ins(r: R): Unit = {
//      val m = noOption(mapify(r))
//      //println(m)
//      //val k: A = noOp(m(idc.h.name)).asInstanceOf[A]
//      val k: Option[A] = noOp(m(idc.h.name))
//      val row = SRow(m - idc.h.name)
//      //println(row)
//      table + ((k,row))
//    }
//    
//    private def noOp[A](a: Any): Option[A] = a match {
//      case Some(b) => Some(b.asInstanceOf[A])
//      case None => None
//      case _ => Some(a.asInstanceOf[A])
//    }
//    
//    
//    
//    private def noOption(m: Map[String,Any]): Map[String,Any] = m.foldLeft(Map[String,Any]())((b,kv) => kv._2 match {
//      case Some(vv) => m + ((kv._1, vv))
//      case None => m - kv._1
//      case _ => m + kv
//    })
//    
//    private def addOption(m: Map[String,Any]): Map[String,Any] = m.foldLeft(Map[String,Any]())((b,kv) => {
//       if(table.columnHeader(kv._1).get.nullable)
//         b + ((kv._1,Some(kv._2)))
//       else
//         b + kv
//      })
//    
//  }
  
}