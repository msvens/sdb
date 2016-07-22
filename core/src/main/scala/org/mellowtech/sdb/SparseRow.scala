/**
 *
 */
package org.mellowtech.sdb

import scala.collection.Map



/**
 * @author msvens
 */
class SparseRow(val m: Map[ColumnName,Any]) extends SRow{

  override def iterator: Iterator[(String, Any)] = m.iterator

  override def get[V](column: ColumnName): Option[V] = m.get(column) match {
    case Some(e) => e match {
      case v: V => Some(v)
      case _ => throw new ClassCastException
    }
    case _ => None
  }

  override def +[B](cv: (ColumnName, B)): SRow = new SparseRow(m + (cv))

  override def ++(vals: Map[ColumnName, Any]): SRow = new SparseRow(m ++ vals)

  override def -(c: ColumnName): SRow = new SparseRow(m - c)

  override def size = m.size

  override def isEmpty = m.isEmpty

}

class SparseIndexedRow(val m: Map[Int,Any], val t: IndexTable[_], val to: Int, val from: Int) extends SRow{

  override def iterator: Iterator[(ColumnName, Any)] = for {
    (idx, value) <- m.iterator
  } yield(t.columnName(idx), value)

  override def get[B](column: ColumnName): Option[B] = {
    val idx = t.columnIndex(column)
    m.get(idx) match {
      case Some(e) => e match {
        case v:B => Some(v)
        case _ => throw new ClassCastException
      }
      case _ => None
    }
  }

  override def ++(vals: Map[ColumnName, Any]): SRow = {
    val newMap = vals.foldLeft(m)((a,b) => {
      val idx = t.columnIndex(b._1)
      a.updated(idx, b._2)
    })
    new SparseIndexedRow(newMap, t, to, from)
  }

  override def +[B](cv: (ColumnName, B)): SRow = {
    val idx = t.columnIndex(cv._1)
    new SparseIndexedRow(m.updated(idx, cv._2), t, to, from)
  }

  override def -(c: ColumnName): SRow = {
    val idx = t.columnIndex(c)
    new SparseIndexedRow(m - idx, t, to, from)
  }

  override def size = m.size

  override def isEmpty = m.isEmpty
}
