package org.mellowtech.sdb

import scala.util.{Success, Failure, Try}

/**
 * Created by msvens on 21/09/15.
 */
class RowTable[A : Ordering](val header: TableHeader, override val path: Option[String], colHeaders: Map[ColumnName,ColumnHeader]) extends STable[A] with FileTable[A]{
  var cHeaders = openColHeaders.toMap
  colHeaders.values foreach (addColumn(_))


  val dataColHeader = ColumnHeader(name = "datacol", maxValueSize = Some(4096), keyType = header.keyType,
    valueType = DbType.BYTES, table = header.name, sorted = true)

  val dataCol: SColumn[A, Array[Byte]] = SColumn(dataColHeader, path.get+"/datacol")

  override def columnHeaders = cHeaders.valuesIterator

  override def flush: Try[Unit] = dataCol.asInstanceOf[Closable].flush

  override def close: Try[Unit] = dataCol.asInstanceOf[Closable].close

  override def size: Long = dataCol.size

  override def column[B](name: ColumnName): SColumn[A, B] = cHeaders get name match {
    case None => throw new NoSuchElementException("no such column "+name)
    case Some(_) =>
      val c: Iterator[(A,B)] = for {
        kv <- dataCol.iterator
        r = SRow(header, kv._2)
        v = r.get(name) if v.isDefined
      } yield(kv._1, v.get)
      SColumn(c.toMap, cHeaders(name))
  }

  override def internalPut(key: A, row: SRow): Try[Unit] = Try(dataCol + (key, SRow.toBytes(row)))

  override def -(key: A) = {
    dataCol - key
    this
  }

  override def columnIterator: Iterator[(String, SColumn[A, Any])] = for {
    cn <- cHeaders.keysIterator
    c = column[Any](cn)
  } yield(cn,c)

  override def addColumn(ch: ColumnHeader): RowTable.this.type = cHeaders get ch.name match {
    case Some(h) => this
    case None =>
      cHeaders += Tuple2(ch.name, ch)
      saveColHeader(ch)
      this
  }

  override def row(key: A): Option[SRow] = dataCol get key match {
    case None => None
    case Some(b) => Some(SRow(header, b))
  }

  override def rowIterator: Iterator[KeyRow[A]] = for(kv <- dataCol.iterator) yield (kv._1, SRow(header, kv._2))

}

class ColTable[A: Ordering](val header: TableHeader, override val path: Option[String], cHeaders: Map[ColumnName, ColumnHeader]) extends STable[A] with FileTable[A] {

  var colHeaders: Map[ColumnName,ColumnHeader] = openColHeaders.toMap
  var cols: Map[ColumnName, SColumn[A,Any]] = openCols(colHeaders.values).toMap

  cHeaders.valuesIterator.foreach(addColumn(_))

  override def size: Long = header.primColumn match {
    case None => throw new NoSuchElementException("no primary column defined")
    case Some(cn) => cols(cn).size
  }

  override def column[B](name: ColumnName): SColumn[A, B] = cols(name).asInstanceOf[SColumn[A,B]]

  override def internalPut(key: A, row: SRow): Try[Unit] = Try(for(r <- row.list) cols(r._1) + (key, r._2))
  

  override def columnIterator: Iterator[(String, SColumn[A, Any])] = cols.toIterator

  override def -(key: A): ColTable.this.type = {
    cols.values.foreach(_ - key)
    this
  }

  override def addColumn(ch: ColumnHeader): ColTable.this.type = colHeaders contains ch.name match{
    case true => this
    case false =>
      colHeaders = colHeaders + ((ch.name, ch))
      saveColHeader(ch)
      val column: SColumn[A,Any] = SColumn(ch, colPath(ch))
      cols = cols + ((ch.name, column))
      this
  }

  override def row(key: A): Option[SRow] = {
    val r = cols.values.foldLeft(SRow(header))((r,c) => c get key match {
      case Some(v) => r + ((c.name,v))
      case None => r
    })
    if(r.size == 0) None else Some(r)
  }

  override def rowIterator: Iterator[KeyRow[A]] =  header.primColumn match {
    case Some(c) => for{
      pcol <- cols(c).iterator
    } yield (pcol._1, row(pcol._1).get)
    case None => throw new NoSuchElementException("No primary column defined")
  }

  override def flush: Try[Unit] = {
    val flushes = cols.values.map(_.asInstanceOf[Closable].flush)
    flushes.find(_.isFailure) match {
      case Some(f) => f
      case None => Success()
    }
  }

  override def close: Try[Unit] = {
    val closes = cols.values.map(_.asInstanceOf[Closable].close)
    closes.find(_.isFailure) match {
      case Some(f) => f
      case None => Success()
    }
  }

  override def columnHeaders = colHeaders.valuesIterator

}
