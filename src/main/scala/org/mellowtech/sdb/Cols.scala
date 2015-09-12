package org.mellowtech.sdb
import scala.collection.SortedMap

import scala.collection.immutable.TreeMap
import org.mellowtech.core.bytestorable._
import org.mellowtech.core.collections._
import org.mellowtech.core.collections.tree._
import org.mellowtech.core.collections.KeyValue
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

class DiscColumn[A, B <: BComparable[A,B], C, D <: BStorable[C,D]](val header: ColumnHeader, 
    path: String, ktype: Class[B], vtype: Class[D]) extends Column[A,C] {
  
  import scala.collection.JavaConverters._
  
  
  
  val valueBlockSize = 2048*2;
  val keyBlockSize = 1048*1
  
  lazy val isBlob: Boolean = Column.calcSize(header).maxValueSize.get > valueBlockSize / 10
  
  private def init: DiscMap[A,C] = header.sorted match {
      case true => new DiscBasedMap(ktype, vtype, path, valueBlockSize, keyBlockSize, isBlob, false)
      case false => new DiscBasedHashMap(ktype, vtype, path, isBlob, false)
  }
  
  val dbmap = init
  
  def size = dbmap.size
  
  
  def update(key: A, value: C): Column[A,C] = {
    dbmap.put(key,value)
    this
  }
  
  def delete(key: A): Column[A,C] = {
    dbmap.remove(key)
    this
  }
  
  def toIterator: Iterator[(A,C)] = for{
      e <- dbmap.iterator().asScala
    } yield((e.getKey,e.getValue))

  def toStream: Stream[(A,C)] = toIterator.toStream
  
  def get(key: A): Option[C] = Option(dbmap.get(key))
    
    
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
   def size(b: BStorable[_,_], ms: Option[Int]):Int = b.isFixed() match {
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
  
  def apply[A,C](header: ColumnHeader, path: String)(implicit ord: Ordering[A]): Column[A,C] = {
    import DbType._
    val m = (header.keyType,header.valueType) match {
    case (STRING,STRING) => new DiscColumn[String,CBString,String,CBString](header,path,classOf[CBString], classOf[CBString])
    case (STRING,INT) => new DiscColumn[String,CBString,Integer,CBInt](header,path,classOf[CBString], classOf[CBInt])
    case (STRING, BYTES) => new DiscColumn[String, CBString, Array[Byte], CBByteArray](header, path, classOf[CBString], classOf[CBByteArray])
    case (INT,INT) => new DiscColumn[Integer,CBInt,Integer,CBInt](header,path,classOf[CBInt], classOf[CBInt])
    case (INT,STRING) => new DiscColumn[Integer,CBInt,String,CBString](header,path,classOf[CBInt], classOf[CBString])
    case (INT, BYTES) => new DiscColumn[Integer, CBInt, Array[Byte], CBByteArray](header, path, classOf[CBInt], classOf[CBByteArray])
    case _ => throw new Error("unknown column type "+header.keyType+" "+header.valueType)
    }
    m.asInstanceOf[Column[A,C]]
  }
  
  /*def apply[A, B <: BComparable[A,B],C, D <: BComparable[C,D]](header: ColumnHeader, path: String)(implicit ord: Ordering[A]): Column[A,C] = {
    new DiscColumn(header, path, bctype[A,B](header.keyType), bctype[C,D](header.valueType))
  }*/
  
  def apply[A,B](map: Map[A,B], header: ColumnHeader)(implicit ord: Ordering[A]): Column[A,B] = map match {
    case m: SortedMap[A,B] => new SparseColumn(map, header)
    case _ => new SparseColumn(map, header)
  }
  
  def apply(): Column[String,String] = {
    import scala.util.Random
    val tname = "table"+Random.alphanumeric.take(10).mkString("")
    val ch = ColumnHeader(tname, "notable", keyType = DbType.STRING, valueType = DbType.STRING)
    apply(ch)(Ordering[String])
  }
  
  def apply[A](t: Table[A])(implicit ord: Ordering[A]): Column[A,String] = {
    import scala.util.Random
    val cname = "col"+Random.alphanumeric.take(10).mkString("")
    val ch = ColumnHeader(cname, "notable", keyType = t.header.keyType, valueType = DbType.STRING, sorted = t.header.sorted)
    apply(ch)
  }
  
  def apply[A,B](header: ColumnHeader)(implicit ord: Ordering[A]): Column[A,B] = header.sorted match {
    case true => new SparseColumn(TreeMap.empty, header)
    case false => new SparseColumn(Map.empty, header)
  }
  
  
}