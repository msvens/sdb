package org.mellowtech.sdb
import org.mellowtech.core.bytestorable._
import org.mellowtech.core.collections._
import org.mellowtech.core.collections.impl.{DiscBasedHashMap, DiscBasedMap}

import scala.collection.Map
import scala.util.Try


class MapColumn[A,B](var m: Map[A,B], val header: ColumnHeader)(implicit val ord: Ordering[A]) extends SColumn[A,B] {

  override def map = m
  def size = m.size
  def get(key: A): Option[B] = m.get(key)
  def iterator: Iterator[(A,B)] = m.toIterator
  def +(kv: (A,B)) = {
    m += kv; 
    this
    }
  def -(key: A) = {m -= key; this}
  override def toString = map mkString " "
}

class DiscColumn[A,B](val header: ColumnHeader, path: String) extends SColumn[A,B] with Closable{
  import scala.collection.JavaConverters._
  val valueBlockSize = DiscMapBuilder.DEFAULT_VALUE_BLOCK_SIZE
  //val keyBlockSize = Dis.DEFAULT_KEY_BLOCK
  lazy val isBlob: Boolean = SColumn.calcSize(header).maxValueSize.get > valueBlockSize / 10

  val builder = SDiscMapBuilder()
  builder.blobValues(isBlob)
  val dbmap = builder.build[A,B](header.keyType, header.valueType, path, header.sorted)

  //val dbmap = new DiscMapBuilder().blobValues(isBlob).memMappedKeyBlocks(true).build(kType, vType,path, header.sorted)

  def size = dbmap.size

  override def map = dbmap.asScala

  def + (kv: (A,B)): SColumn[A,B] = {
    dbmap.put(kv._1, kv._2)
    this
  }

  def -(key: A): SColumn[A,B] = {
    dbmap.remove(key)
    this
  }

  def iterator: Iterator[(A,B)] = for{
    e <- dbmap.iterator().asScala
  } yield (e.getKey, e.getValue)


  def get(key: A): Option[B] = Option(dbmap.get(key))

  override def flush: Try[Unit] = Try(dbmap.save())
  override def close: Try[Unit] = Try(dbmap.close())
}

/*
class DiscColumn[A, B <: BComparable[A,B], C, D <: BStorable[C,D]](val header: ColumnHeader,
    path: String, ktype: Class[B], vtype: Class[D]) extends SColumn[A,C] with Closable {
  
  import scala.collection.JavaConverters._
  
  
  
  val valueBlockSize = 2048*2
  val keyBlockSize = 1048*1
  
  lazy val isBlob: Boolean = SColumn.calcSize(header).maxValueSize.get > valueBlockSize / 10
  
  private def init: DiscMap[A,C] = header.sorted match {
      case true => new DiscBasedMap(ktype, vtype, path, valueBlockSize, keyBlockSize, isBlob, false)
      case false => new DiscBasedHashMap(ktype, vtype, path, isBlob, false)
  }
  
  val dbmap = init
  
  def size = dbmap.size

  override def map = dbmap.asScala
  
  def + (kv: (A,C)): SColumn[A,C] = {
    dbmap.put(kv._1, kv._2)
    this
  }

  def -(key: A): SColumn[A,C] = {
    dbmap.remove(key)
    this
  }
  
  def iterator: Iterator[(A,C)] = for{
      e <- dbmap.iterator().asScala
    } yield (e.getKey, e.getValue)

  
  def get(key: A): Option[C] = Option(dbmap.get(key))
    
  override def flush: Try[Unit] = Try(dbmap.save())
  override def close: Try[Unit] = Try(dbmap.close())
  
}
*/
