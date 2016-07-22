package org.mellowtech.sdb

import java.util.Date

import org.mellowtech.core.bytestorable._
import org.mellowtech.core.collections.DiscMap

import scala.collection.{Map,SortedMap}
import scala.collection.immutable.TreeMap
import scala.util.{Success, Try}


/**
 * Created by msvens on 20/09/15.
 */
trait SColumn[A,B] {

  def size: Long
  def get(key: A): Option[B]
  def +(kv: (A,B)): SColumn[A,B]
  def -(key: A): SColumn[A,B]
  def header: ColumnHeader
  def iterator: Iterator[(A,B)]

  lazy val name: String = header.name
  def apply(key: A) = get(key).getOrElse(throw new NoSuchElementException)
  def map: Map[A,B] = iterator.toMap

  override def toString: String = {
    val sb = new StringBuilder
    sb ++= "col: "+name+"\n"
    for(cv <- iterator) {sb ++= cv._1+":\t"+cv._2+"\n"}
    sb.toString()
  }

  override def equals(other: Any): Boolean = other match  {
    case that: SColumn[A,B] => {
      def m(kv: (A,B)): Boolean = {
        that.get(kv._1) match {
          case None => false
          case Some(v) => kv._2 == v
        }
      }
      that.size == this.size && that.name == this.name && iterator.forall(m)
    }
    case _ => false
  }

  override def hashCode = name.hashCode

}

object SColumn {

  import scala.reflect._

  def calcSize(header: ColumnHeader): ColumnHeader = {
    def size(b: BStorable[_, _], ms: Option[Int]): Int = b.isFixed match {
      case true => b.byteSize
      case false => ms match {
        case Some(s) => s
        case None => Int.MaxValue
      }
    }
    val kSize = size(bctype(header.keyType).newInstance(), header.maxKeySize)
    val vSize = size(bctype(header.valueType).newInstance(), header.maxValueSize)
    header.copy(maxKeySize = Some(kSize), maxValueSize = Some(vSize))
  }

  def apply[A, B](header: ColumnHeader, path: String)(implicit ord: Ordering[A]): SColumn[A, B] = {
    import DbType._
    val m: DiscColumn[_,_] = new DiscColumn(header, path)
    m.asInstanceOf[SColumn[A, B]]
  }

  /*def apply[A, B <: BComparable[A,B],C, D <: BComparable[C,D]](header: ColumnHeader, path: String)(implicit ord: Ordering[A]): Column[A,C] = {
    new DiscColumn(header, path, bctype[A,B](header.keyType), bctype[C,D](header.valueType))
  }*/

  def apply[A, B](map: Map[A, B], header: ColumnHeader)(implicit ord: Ordering[A]): SColumn[A, B] = map match {
    case m: SortedMap[A, B] => new MapColumn(map, header)
    case _ => new MapColumn(map, header)
  }

  def apply(): SColumn[String, String] = {
    import scala.util.Random
    val tname = "table" + Random.alphanumeric.take(10).mkString("")
    val ch = ColumnHeader(tname, "notable", keyType = DbType.STRING, valueType = DbType.STRING)
    apply(ch)(Ordering[String])
  }

  def apply[A](t: STable[A])(implicit ord: Ordering[A]): SColumn[A, String] = {
    import scala.util.Random
    val cname = "col" + Random.alphanumeric.take(10).mkString("")
    val ch = ColumnHeader(cname, "notable", keyType = t.header.keyType, valueType = DbType.STRING, sorted = t.header.sorted)
    apply(ch)
  }

  def apply[A, B](header: ColumnHeader)(implicit ord: Ordering[A]): SColumn[A, B] = header.sorted match {
    case true => new MapColumn(TreeMap.empty, header)
    case false => new MapColumn(Map.empty, header)
  }
}