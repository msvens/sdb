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
    /*val m = (header.keyType, header.valueType) match {
      case (STRING, STRING) => new DiscColumn[String, CBString, String, CBString](header, path, classOf[CBString], classOf[CBString])
      case (STRING, CHAR) => new DiscColumn[String, CBString, Character, CBChar](header, path, classOf[CBString], classOf[CBChar])
      case (STRING, BYTE) => new DiscColumn[String, CBString, java.lang.Byte, CBByte](header, path, classOf[CBString], classOf[CBByte])
      case (STRING, BOOLEAN) => new DiscColumn[String, CBString, java.lang.Boolean, CBBoolean](header, path, classOf[CBString], classOf[CBBoolean])
      case (STRING, SHORT) => new DiscColumn[String, CBString, java.lang.Short, CBShort](header, path, classOf[CBString], classOf[CBShort])
      case (STRING, INT) => new DiscColumn[String, CBString, Integer, CBInt](header, path, classOf[CBString], classOf[CBInt])
      case (STRING, LONG) => new DiscColumn[String, CBString, java.lang.Long, CBLong](header, path, classOf[CBString], classOf[CBLong])
      case (STRING, FLOAT) => new DiscColumn[String,CBString, java.lang.Float, CBFloat](header, path, classOf[CBString], classOf[CBFloat])
      case (STRING, DOUBLE) => new DiscColumn[String,CBString, java.lang.Double, CBDouble](header, path, classOf[CBString], classOf[CBDouble])
      case (STRING, DATE) => new DiscColumn[String,CBString, Date, CBDate](header, path, classOf[CBString], classOf[CBDate])
      case (STRING, BYTES) => new DiscColumn[String, CBString, Array[Byte], CBByteArray](header, path, classOf[CBString], classOf[CBByteArray])
      case (INT, STRING) => new DiscColumn[Integer, CBInt, String, CBString](header, path, classOf[CBInt], classOf[CBString])
      case (INT, CHAR) => new DiscColumn[Integer, CBInt, Character, CBChar](header, path, classOf[CBInt], classOf[CBChar])
      case (INT, BYTE) => new DiscColumn[Integer, CBInt, java.lang.Byte, CBByte](header, path, classOf[CBInt], classOf[CBByte])
      case (INT, BOOLEAN) => new DiscColumn[Integer, CBInt, java.lang.Boolean, CBBoolean](header, path, classOf[CBInt], classOf[CBBoolean])
      case (INT, SHORT) => new DiscColumn[Integer, CBInt, java.lang.Short, CBShort](header, path, classOf[CBInt], classOf[CBShort])
      case (INT, INT) => new DiscColumn[Integer, CBInt, Integer, CBInt](header, path, classOf[CBInt], classOf[CBInt])
      case (INT, LONG) => new DiscColumn[Integer, CBInt, java.lang.Long, CBLong](header, path, classOf[CBInt], classOf[CBLong])
      case (INT, FLOAT) => new DiscColumn[Integer,CBInt, java.lang.Float, CBFloat](header, path, classOf[CBInt], classOf[CBFloat])
      case (INT, DOUBLE) => new DiscColumn[Integer,CBInt, java.lang.Double, CBDouble](header, path, classOf[CBInt], classOf[CBDouble])
      case (INT, DATE) => new DiscColumn[Integer,CBInt, Date, CBDate](header, path, classOf[CBInt], classOf[CBDate])
      case (INT, BYTES) => new DiscColumn[Integer, CBInt, Array[Byte], CBByteArray](header, path, classOf[CBInt], classOf[CBByteArray])
      case (LONG, STRING) => new DiscColumn[java.lang.Long, CBLong, String, CBString](header, path, classOf[CBLong], classOf[CBString])
      case (LONG, CHAR) => new DiscColumn[java.lang.Long, CBLong, Character, CBChar](header, path, classOf[CBLong], classOf[CBChar])
      case (LONG, BYTE) => new DiscColumn[java.lang.Long, CBLong, java.lang.Byte, CBByte](header, path, classOf[CBLong], classOf[CBByte])
      case (LONG, BOOLEAN) => new DiscColumn[java.lang.Long, CBLong, java.lang.Boolean, CBBoolean](header, path, classOf[CBLong], classOf[CBBoolean])
      case (LONG, SHORT) => new DiscColumn[java.lang.Long, CBLong, java.lang.Short, CBShort](header, path, classOf[CBLong], classOf[CBShort])
      case (LONG, INT) => new DiscColumn[java.lang.Long, CBLong, Integer, CBInt](header, path, classOf[CBLong], classOf[CBInt])
      case (LONG, LONG) => new DiscColumn[java.lang.Long, CBLong, java.lang.Long, CBLong](header, path, classOf[CBLong], classOf[CBLong])
      case (LONG, FLOAT) => new DiscColumn[java.lang.Long,CBLong, java.lang.Float, CBFloat](header, path, classOf[CBLong], classOf[CBFloat])
      case (LONG, DOUBLE) => new DiscColumn[java.lang.Long,CBLong, java.lang.Double, CBDouble](header, path, classOf[CBLong], classOf[CBDouble])
      case (LONG, DATE) => new DiscColumn[java.lang.Long,CBLong, Date, CBDate](header, path, classOf[CBLong], classOf[CBDate])
      case (LONG, BYTES) => new DiscColumn[java.lang.Long, CBLong, Array[Byte], CBByteArray](header, path, classOf[CBLong], classOf[CBByteArray])

      case _ => throw new Error("unknown column type " + header.keyType + " " + header.valueType)
    }*/
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