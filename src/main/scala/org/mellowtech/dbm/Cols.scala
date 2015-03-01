package org.mellowtech.dbm
import scala.collection.SortedMap

import scala.collection.immutable.TreeMap
import com.mellowtech.core.collections.mappings.{BCMapping,BSMapping}
import com.mellowtech.core.bytestorable._
import com.mellowtech.core.collections._
import com.mellowtech.core.collections.tree._
import com.mellowtech.core.collections.KeyValue
import scala.util.{Try,Success,Failure}

trait Column[K,V] {
  
  def get(key: K): Option[V]
  def header: ColumnHeader
  val name = header.name
  def size: Long
  def update(key: K, value: V): Column[K,V]
  def delete(key: K): Column[K,V]
  def + (kv: (K,V)): Column[K,V] = update(kv._1, kv._2)
  def - (k: K): Column[K,V] = delete(k)
  def toStream: Stream[(K,V)]
  def toIterator: Iterator[(K,V)]
  def asMap: Map[K,V] = toStream.toMap
  def apply(key: K): V = get(key).get
  def flush: Try[Unit] = Success()
  def close: Try[Unit] = Success()
  
  //override some default functions
  override def toString: String = {
    val sb = new StringBuilder
    sb ++= "col: "+name+"\n"
    for(cv <- toIterator) {sb ++= cv._1+":\t"+cv._2+"\n"}
    sb.toString
  }
  
  override def equals(other: Any): Boolean = other match  {
      case that: Column[K,V] => {
        def m(kv: (K,V)): Boolean = {
          that.get(kv._1) match {
            case None => false
            case Some(v) => kv._2 == v
          }
        }
        that.size == this.size && that.name == this.name && toIterator.forall(m)
      }
      case _ => false
    }
  
  override def hashCode = name.hashCode
}


trait MapColumn[K,V] extends Column[K,V] {
  implicit val ord: Ordering[K]
  def map: Map[K,V]
  def size = map.size
  def get(key: K): Option[V] = map.get(key)
  def toStream: Stream[(K,V)] = map.toStream
  def toIterator: Iterator[(K,V)] = map.toIterator
  def update(key: K, value: V): Column[K,V] = Column(map + ((key,value)), header)
  def delete(key: K) = Column(map - key, header)
  override def asMap: Map[K,V] = map
  override def toString = map mkString " "
  override def apply(key: K) = map(key)
}

class SparseColumn[K,V](val map: Map[K,V], val header: ColumnHeader)(implicit val ord: Ordering[K]) extends MapColumn[K,V] {

}

class DiscColumn[K,V](val header: ColumnHeader, path: String, kmap: BCMapping[K], vmap: BSMapping[V]) extends Column[K,V] {
  
  import scala.collection.JavaConverters._
  
  val valueBlockSize = 2048*2;
  val keyBlockSize = 1048*1
  
  lazy val isBlob: Boolean = Column.calcSize(header).maxValueSize.get > valueBlockSize / 10
  
  private def init: DiscMap[K,V] = header.sorted match {
      case true => new DiscBasedMap(kmap, vmap, path, valueBlockSize, keyBlockSize, isBlob, false)
      case false => new DiscBasedHashMap(kmap, vmap, path)
  }
  
  val dbmap = init
  
  def size = dbmap.size
  
  
  def update(key: K, value: V): Column[K,V] = {
    dbmap.put(key,value)
    this
  }
  
  def delete(key: K): Column[K,V] = {
    dbmap.remove(key)
    this
  }
  
  def toIterator: Iterator[(K,V)] = for{
      e <- dbmap.iterator().asScala
    } yield((e.getKey,e.getValue))

  def toStream: Stream[(K,V)] = toIterator.toStream
  
  def get(key: K): Option[V] = Option(dbmap.get(key))
    
    
  /*override def apply(key: K) = get(key) match {
      case Some(x) => x
      case None => throw new NoSuchElementException
    }*/
    
  override def flush: Try[Unit] = Try(dbmap.save)
  override def close: Try[Unit] = Try(dbmap.close)
  
}


object Column {
  import scala.reflect._
  
  def calcSize(header: ColumnHeader): ColumnHeader = {
   def size[T](b: ByteStorable[T], ms: Option[Int]):Int = b.isFixed() match {
     case true => b.byteSize
     case false => ms match {
       case Some(s) => s
       case None => Int.MaxValue
     }
   }
   val kSize = size(bcmap(header.keyType).getTemplate, header.maxKeySize)
   val vSize = size(bcmap(header.valueType).getTemplate, header.maxValueSize)
   header.copy(maxKeySize = Some(kSize), maxValueSize = Some(vSize))
  }
  
  def apply[K,V](header: ColumnHeader, path: String)(implicit ord: Ordering[K]): Column[K,V] = {
    new DiscColumn(header, path, bcmap[K](header.keyType), bcmap[V](header.valueType))
  }
  def apply[K,V](map: Map[K,V], header: ColumnHeader)(implicit ord: Ordering[K]): Column[K,V] = map match {
    case m: SortedMap[K,V] => new SparseColumn(map, header)
    case _ => new SparseColumn(map, header)
  }
  
  def apply(): Column[String,String] = {
    import scala.util.Random
    val tname = "table"+Random.alphanumeric.take(10).mkString("")
    val ch = ColumnHeader(tname, "notable", keyType = DbType.STRING, valueType = DbType.STRING)
    apply(ch)(Ordering[String])
  }
  
  def apply[K](t: Table[K])(implicit ord: Ordering[K]): Column[K,String] = {
    import scala.util.Random
    val cname = "col"+Random.alphanumeric.take(10).mkString("")
    val ch = ColumnHeader(cname, "notable", keyType = t.header.keyType, valueType = DbType.STRING, sorted = t.header.sorted)
    apply(ch)
  }
  
  def apply[K,V](header: ColumnHeader)(implicit ord: Ordering[K]): Column[K,V] = header.sorted match {
    case true => new SparseColumn(TreeMap.empty, header)
    case false => new SparseColumn(Map.empty, header)
  }
}