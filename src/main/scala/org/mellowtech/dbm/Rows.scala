/**
 *
 */
package org.mellowtech.dbm

import scala.collection.JavaConversions._
import java.util.{Map => JMap}
import scala.reflect.ClassTag
import scala.collection.immutable.TreeMap

trait Row[K] {
  def key: K
  def size: Int
  def get[V](column: String): Option[V]
  def isEmpty: Boolean
  def update[V](c: ColumnName, value: V): Row[K]
  def +[V](cv: (ColumnName,V)): Row[K] = update(cv._1, cv._2)
  def delete(c: ColumnName): Row[K]
  def - (c: ColumnName): Row[K] = delete(c)
  def hasColumn(c: ColumnName): Boolean
  def toList: List[(String,Any)] = iterator.toList
  def iterator: Iterator[(String,Any)]
  def apply[V](c: ColumnName): V = get[V](c).get
  
  override def toString: String = {
    val sb = new StringBuilder
    sb ++= "row: "+key+"\n"
    for(cv <- iterator) {sb ++= cv._1+":\t"+cv._2+"\n"}
    sb.toString
  }
  
  override def equals(other: Any): Boolean = other match  {
      case that: Row[K] => {
        def m(kv: (String,Any)): Boolean = {
          that.get[Any](kv._1) match {
            case None => false
            case Some(v) => kv._2 == v
          }
        }
        that.size == this.size && that.key == this.key && iterator.forall(m)
      }
      case _ => false
    }
  
  override def hashCode = key.hashCode
}

object Row {
  import java.io.ByteArrayOutputStream
  import java.nio.ByteBuffer
  import com.mellowtech.core.bytestorable.{CBInt, PrimitiveObject, PrimitiveIndexedObject, CBString}
  
  def apply[K](key: K)(implicit ord: Ordering[K]) = new SparseRow(key, TreeMap[String,Any]())
  
  def apply[K](key: K, th: TableHeader)(implicit ord: Ordering[K]): Row[K] = th.sorted match {
      case true => new SparseRow(key, TreeMap[String,Any]())
      case false => new SparseRow(key, Map[String,Any]())
    }
  
  def apply[K](key: K, map: JMap[String,Any])(implicit ord: Ordering[K]): Row[K] = new SparseRow(key,map.toMap)
  
  def apply[K](key: K, m: Map[String,Any])(implicit ord: Ordering[K]): Row[K] = new SparseRow(key,m)
  
  def toBytes[K](row: Row[K]): Array[Byte] = {
     val bos = new ByteArrayOutputStream
     (new CBInt(row.size)).toBytes(bos)
     row.iterator foreach { case (c,v) => {(new CBString(c)).toBytes(bos);(new PrimitiveObject(v)).toBytes(bos)}}
     bos.toByteArray
  }
  
  def toBytes[K](row: Row[K], t: IndexTable[K]): Array[Byte] = {
    val bos = new ByteArrayOutputStream
    (new CBInt(row.size)).toBytes(bos)
    for (i <- row.toList){
      new PrimitiveIndexedObject(i._2, t.columnIndex(i._1)).toBytes(bos)
    }
    bos.toByteArray
  }
  
  def apply[K](key: K, th: TableHeader, b: Array[Byte], t: IndexTable[K])(implicit ord: Ordering[K]): Row[K] = {
    val bb = ByteBuffer.wrap(b)
    val size = (new CBInt).fromBytes(bb).get
    val pio = new PrimitiveIndexedObject
    
    null
  }
  
  def apply[K](key: K, th: TableHeader, b: Array[Byte])(implicit ord: Ordering[K]): Row[K] = {
    val bb = ByteBuffer.wrap(b)
    val size = (new CBInt).fromBytes(bb).get
    val po = new PrimitiveObject[Any]
    val cv = for {
      i <- 0 until size
    } yield ((new CBString()).fromBytes(bb).get,po.fromBytes(bb).get)
    val m = th.sorted match {
      case true => TreeMap[String,Any]() ++ cv
      case false => Map[String,Any]() ++ cv
    }
    apply(key,m)
  }
  
}

/**
 * @author msvens
 */
class SparseRow[K : Ordering](val key: K, val m: Map[String,Any]) extends Row[K]{

  def get[V](column: String): Option[V] = m.get(column) match {
    case Some(e) => e match {
      case v:V => Some(v)
      case _ => throw new ClassCastException
    }
    case None => None
  }
  def size = m.size
  def isEmpty: Boolean = m.isEmpty
  def update[V](c: ColumnName, value: V): Row[K] = Row(key, m.updated(c, value))
  def delete(c: ColumnName): Row[K] = Row(key, m - c)
  def hasColumn(c: ColumnName): Boolean = m  contains c
  override def toList: List[(String,Any)] = m.toList
  def iterator: Iterator[(String, Any)] = m.toIterator
   
}

class SparseIndexedRow[K : Ordering](val key: K, val m: Map[Int,Any], val t: IndexTable[K], val to: Int, val from: Int) extends Row[K] {
  
  def get[V](column: String): Option[V] = {
    val idx = t.columnIndex(column)
    m.get(idx) match {
      case Some(e) => e match {
        case v:V => Some(v)
        case _ => throw new ClassCastException
      }
      case None => None
    }
  }
  def size = m.size
  def isEmpty: Boolean = m.isEmpty
  def update[V](c: ColumnName, value: V): Row[K] = {
     val idx = t.columnIndex(c)
     new SparseIndexedRow(key, m.updated(idx, value), t, to, from)
     
  }
  def delete(c: ColumnName): Row[K] = {
    val idx = t.columnIndex(c)
    new SparseIndexedRow(key, m - idx, t, to, from)
  }
  
  def hasColumn(c: ColumnName): Boolean = m contains t.columnIndex(c)
  
  def iterator: Iterator[(String, Any)] = for {
    (idx,value) <- m.toIterator  
  } yield (t.columnName(idx), value)
 
  //override def toList: List[(String, Any)] = null
}
