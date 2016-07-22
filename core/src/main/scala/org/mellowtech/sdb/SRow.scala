package org.mellowtech.sdb

import scala.collection.Map

import scala.collection.immutable.TreeMap


/**
 * Created by msvens on 17/09/15.
 */
/**
 * An SRow is a represtantion of a row in a STable. It contains an identifying key and a number
 * of (column name, value) pairs.
 */
trait SRow{

  /**
   * get the key for this row
   * @return key
   */
  //def key: A

  /**
   * Check if this row dont contain any values
   * @return true if empty
   */
  def isEmpty = list == Nil

  /**
   * Number of valus in this columns in this row
   * @return
   */
  def size = list.size

  /**
   * Get the column,value pairs
   * @return list
   */
  def list: List[(ColumnName,Any)] = iterator.toList

  def iterator: Iterator[(ColumnName,Any)]

  def +[B](cv: (ColumnName,B)): SRow
  def ++(vals: Map[ColumnName, Any]): SRow
  def -(c: ColumnName): SRow

  def get[B](column: ColumnName): Option[B]

  def contains(column: ColumnName) = get(column).isDefined

  def apply[B](c: ColumnName): B = get[B](c).get

  override def toString: String = iterator.foldLeft("")((s,cv) => s + cv._1 + "::" +cv._2+" ")

  override def equals(other: Any): Boolean = other match  {
    case that: SRow =>
      def m(kv: (String,Any)): Boolean = {
        that.get[Any](kv._1) match {
          case None => false
          case Some(v) => kv._2 == v
        }
      }
      that.size == this.size && iterator.forall(m)
    case _ => false
  }

  //override def hashCode = key.hashCode

}

object SRow {
  import java.io.ByteArrayOutputStream
  import java.nio.ByteBuffer
  import org.mellowtech.core.bytestorable.{CBInt, PrimitiveObject, PrimitiveIndexedObject, CBString}
  import java.util.{Map => JMap}
  import scala.collection.JavaConverters._

  def apply(): SRow = new SparseRow(TreeMap[ColumnName,Any]())

  def apply(th: TableHeader): SRow = th.sorted match {
    case true => new SparseRow(TreeMap[ColumnName,Any]())
    case false => new SparseRow(Map[ColumnName,Any]())
  }

  def apply(map: JMap[ColumnName,Any]): SRow = new SparseRow(map.asScala)


  def apply(map: Map[ColumnName,Any]): SRow = new SparseRow(map)

  def toBytes[A](row: SRow): Array[Byte] = {
    val bos = new ByteArrayOutputStream
    new CBInt(row.size).to(bos)
    row.iterator foreach { case (c,v) => new CBString(c).to(bos); new PrimitiveObject(v).to(bos) }
    bos.toByteArray
  }

  def toBytes[A](row: SRow, t: IndexTable[A]): Array[Byte] = {
    val bos = new ByteArrayOutputStream
    new CBInt(row.size).to(bos)
    for (i <- row.list){
      new PrimitiveIndexedObject(i._2, t.columnIndex(i._1)).to(bos)
    }
    bos.toByteArray
  }

  def apply[A](th: TableHeader, b: Array[Byte], t: IndexTable[A])(implicit ord: Ordering[A]): SRow = ???/*{
    val bb = ByteBuffer.wrap(b)
    val size = (new CBInt).from(bb).value
    val pio = new PrimitiveIndexedObject

    null
  }*/

  def apply(th: TableHeader, b: Array[Byte]): SRow = {
    val bb = ByteBuffer.wrap(b)
    val size = (new CBInt).from(bb).value
    val po = new PrimitiveObject[Any]
    val cv = for {
      i <- 0 until size
    } yield (new CBString().from(bb).get,po.from(bb).get)
    val m = th.sorted match {
      case true => TreeMap[ColumnName,Any]() ++ cv
      case false => Map[ColumnName,Any]() ++ cv
    }
    apply(m)
  }

}