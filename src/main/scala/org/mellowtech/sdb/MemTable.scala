package org.mellowtech.sdb
import scala.util.{Success, Try}


class MemRowTable[A: Ordering](val header: TableHeader, var table: Map[A, SRow], var colHeaders: Map[ColumnName, ColumnHeader]) extends STable[A] {

  import scala.collection.immutable.SortedMap

  override def size: Long = table.size

  override def column[B](name: ColumnName): SColumn[A, B] = {
    val iter: Iterator[(A,B)] = for {
      r <- table.iterator
      v = r._2.get(name)
      if v.isDefined
    } yield (r._1,v.get)

    header.sorted match {
      case false => SColumn(iter.toMap,colHeaders(name))
      case true => SColumn(SortedMap(iter.to:_*),colHeaders(name))
    }
  }

  override def internalPut(k: A, row: SRow): Try[Unit] = Success(table = table + ((k,row)))
  

  override def columnIterator: Iterator[(String, SColumn[A, Any])] = for {
    i <- colHeaders.valuesIterator
  } yield(i.name, column[Any](i.name))

  override def -(key: A): MemRowTable.this.type = {table -= key; this}

  override def addColumn(ch: ColumnHeader): MemRowTable.this.type = colHeaders get ch.name match {
    case Some(_) => this
    case None => {
      colHeaders += ((ch.name,ch))
      this
    }
  }

  override def row(key: A): Option[SRow] = table.get(key)

  override def rowIterator: Iterator[KeyRow[A]] = table.iterator

  override def columnHeaders = colHeaders.valuesIterator
}

class MemColTable[A: Ordering](val header: TableHeader, var table: Map[ColumnName, SColumn[A,Any]]) extends STable[A]{
  
  override def size: Long = header.primColumn match {
    case None => throw new NoSuchElementException("no primary column set...cannot determine table size")
    case Some(cn) => table(cn).size
  }

  override def column[B](name: ColumnName): SColumn[A, B] = table get name match {
    case None => throw new NoSuchElementException("undefined column")
    case Some(c) => c match {
      case cc: SColumn[A,B] => cc
      case _ => throw new Error("value type dont match")
    } 
  }
  

  override def internalPut(key: A, row: SRow): Try[Unit] = Try(for(v <- row.iterator) {
      val c = table(v._1)
      c + (key,v._2)
      table += ((v._1, c))
  })
    
  override def columnIterator: Iterator[(String, SColumn[A, Any])] = table.iterator

  override def -(key: A): MemColTable.this.type = table.values.foldLeft[this.type](this)((t,c) => {c - key; t})

  override def addColumn(ch: ColumnHeader): MemColTable.this.type = table get ch.name match {
    case Some(_) => this
    case None =>
      table += ((ch.name,SColumn[A,Any](ch)))
      this
  }

  override def row(key: A): Option[SRow] = {
    val f: Iterable[(ColumnName,Any)] = for {
      i <- table.values
      v = i.get(key)
      if v.isDefined
    } yield(i.name, v.get)
    f.isEmpty match {
      case true => None
      case _ => Some(SRow(f.toMap))
    }
  }

  override def rowIterator: Iterator[KeyRow[A]] = header.primColumn match {
    case None => throw new NoSuchElementException("no primary column defined")
    case Some(c) => for(cv <- table(c).iterator) yield (cv._1, row(cv._1).get)
  }
  
  override def columnHeader(cn: ColumnName) = table get cn match {
    case Some(col) => Some(col.header)
    case None => None
  }
  
  override def columnHeaders = for(columns <- table.values.iterator) yield columns.header
}
/*
class MemColTable[K: Ordering](val header: TableHeader, var table: Map[ColumnName, Column[K,Any]]) extends Table[K]{
  import scala.collection.immutable.SortedMap

  def path = None

  def columnHeader(cn: ColumnName) = table get cn match {
    case Some(col) => Some(col.header)
    case None => None
  }

  def headers = for {
    v <- table.valuesIterator
  } yield(v.header)

  def row(key: K): Row[K] = {
    val f: Iterator[(ColumnName,Any)] = for {
      i <- table.valuesIterator
      if(i.get(key) != None)
    } yield(i.name, i(key))
    Row(key, f.toMap)
  }


  def rows: Iterator[Row[K]] = header.primColumn match {
    case None => throw new Exception("no primary column defined")
    case Some(cn) => for{
      i <- table(cn).toIterator
    } yield(row(i._1))
  }

  def addCol[V](c: ColumnHeader) = table get c.name match {
    case Some(_) => this
    case None => {
      table += ((c.name, Column(c)))
      this
    }
  }

  def column[V](cn: ColumnName): Column[K,V] = table get cn match{
    case None => throw new Error("no such column")
    case Some(c) => c match {
      case c1:Column[K,V] => c1
      case _ => throw new Error("wrong column type")
    }
  }

  def columns: Iterator[(String,Column[K,Any])] = for {
    i <- table.valuesIterator
  } yield(i.name, i)

  def +=(row: Row[K]) = row.iterator.foldLeft[this.type](this)((b,i) => b.+=(row.key,i._1,i._2))



  def +=[V](key: K, col: ColumnName, value: V) = table get col match {
    case None => throw new Error("no such column")
    case Some(c) => {
      val c1 = c + (key,value)
      table += ((col,c1))
      this
    }
  }

  def -=(n: ColumnName, key: K) = table get n match {
    case None => this
    case Some(c) => c get key match {
      case None => this
      case Some(_) => {
        table += ((n, c - key))
        this
      }
    }
  }

  def -=(key: K) = table.keysIterator.foldLeft[this.type](this)((b,i) => b.-=(i, key))

  def flush: Try[Unit] = Success()
  def close: Try[Unit] = Success()

  override def size: Long = header.primColumn match {
    case None => -1
    case Some(cn) => table(cn).size
  }
}
*/