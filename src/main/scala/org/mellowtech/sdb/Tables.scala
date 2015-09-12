package org.mellowtech.sdb
import scala.util.{Try,Success,Failure}
import org.mellowtech.core.bytestorable.BComparable
import org.mellowtech.core.bytestorable.BStorable

trait Table[K] {
  
  
  lazy val name: String = header.name
  def row(key: K): Row[K]
  def rowIf(key: K): Option[Row[K]] = {
    val r = row(key)
    if(r.size < 1) None else Some(r)
  }
  
  def header: TableHeader
  def column[V](n: ColumnName): Column[K,V]
  def rows: Iterator[Row[K]]
  def columns: Iterator[(String,Column[K,Any])]
  def columnHeader(cn: ColumnName): Option[ColumnHeader]
  def headers: Iterator[ColumnHeader]
  def path: Option[String]
  def addCol[V](c: ColumnHeader): Table.this.type
  def +=[V](row: K, col: ColumnName, v: V): Table.this.type
  def +=(row: Row[K]): Table.this.type
  def -=(n: ColumnName, key: K): Table.this.type
  def -=(key: K): Table.this.type
  def find[A](cn: ColumnName, v: A): Iterator[Row[K]] = columnHeader(cn) match {
    case None => throw new Error("no such column")
    case Some(ch) => for{
      r <- rows
        if(r.get(cn) != None && r(cn) == v)
      } yield(r)
  }
  def flush: Try[Unit]
  def close: Try[Unit]
  def size: Long = -1
  
  def verify(r: Row[K]): Try[Unit] = {
    
    val noNull = headers.find(h => if(!h.nullable && r.get(h.name) == None) true else false)
    val noCol = r.iterator.find(kv => columnHeader(kv._1) match {
      case None => true
      case Some(cn) => {
        dbType(kv._2) != cn.valueType
      }
    });
    
    noNull match {
      case Some(c) => Failure(new Error("non nullable column was not set: "+noNull.get.name))
      case _ => noCol match {
        case Some(cc) => {
          if(columnHeader(cc.toString) != None)
            Failure(new Error("column does not exist: "+cc))
          else
            Failure(new Error("value type does not match for column: "+cc))
        }
        case None => Success()
      }
    }
  }
  
}

trait IndexTable[K] extends Table[K] {
  def columnIndex(c: ColumnName): Int
  def columnName(idx: Int): String
}

trait FileTable[K] {this: Table[K] =>
  import java.io._
  import scala.io._
  
  val ColHead = "col.head"
  
  
  def colPath(ch: ColumnHeader): String = {
    val p = new File(path.get+"/"+ch.name+"/data").getAbsolutePath
    p
  }
  
  def openColHeader(d: File): ColumnHeader = {
      val json = Source.fromFile(new File(d,ColHead)).mkString
      ColumnHeader.fromJson(json)
    }
  
  def openColHeaders: Seq[(ColumnName, ColumnHeader)] = for {
      f <- new File(path.get).listFiles
      if f.isDirectory && !f.getName.equals("tblidx")
      c = openColHeader(f)
    } yield((c.name, c))
  
  def saveColHeader(ch: ColumnHeader): String = {
    val d = new File(path.get+"/"+ch.name)
    if(!d.exists) d.mkdir
    val w = new FileWriter(new File(d, ColHead))
    w.write(ColumnHeader asJson ch)
    w.close
    d.getAbsolutePath
  }
    
  def openCols(headers: Iterable[ColumnHeader])(implicit ord: Ordering[K]): Iterable[(ColumnName,Column[K,Any])] = for {
      ch <- headers
      d = new File(path.get + "/" + ch.name + "/data")
    } yield((ch.name,Column[K,Any](ch, d.getAbsolutePath)))
}

trait Queryable {
  
}

object Table {
  import scala.collection.immutable.TreeMap
  import org.mellowtech.sdb.search.IndexingTable
  
  private def fill[K](items: Seq[(K,ColumnName,Any)])(t: Table[K]): Table[K] = items.foldLeft(t)((b,i) => b.+=(i._1, i._2, i._3))
  
  /**
 * @param th
 * @param ord
 * @return
 */
  
  def apply(): Table[String] = {
    import scala.util.Random
    val tname = "table"+Random.alphanumeric.take(10).mkString("")
    val th = TableHeader(tname, DbType.STRING, tableType = TableType.MEMORY_COLUMN)
    apply(th)(Ordering[String])
  }
  
  def apply[K](th: TableHeader)(implicit ord: Ordering[K]): Table[K] = new MemColTable(th, Map.empty)
  
  def apply[K](th: TableHeader, columns: Seq[ColumnHeader])(implicit ord: Ordering[K]): Table[K] = apply(th, columns, Seq.empty:_*)
  
  def apply[K](th: TableHeader, columns: Seq[ColumnHeader], items: (K,ColumnName,Any)*)(implicit ord: Ordering[K]): Table[K] = apply(th, columns, null, items:_*)
  
  def apply[K](th: TableHeader, path: String)(implicit ord: Ordering[K]): Table[K] = apply(th, Seq.empty, path, Seq.empty:_*)
  
  def apply[K](th: TableHeader, columns: Seq[ColumnHeader], path: String)(implicit ord: Ordering[K]): Table[K] = apply(th,columns, path, Seq.empty:_*)
  
  def apply[K](th: TableHeader, columns: Seq[ColumnHeader], path: String, items: (K,ColumnName,Any)*)(implicit ord: Ordering[K]): Table[K] = 
    fill(items) {
      val m = {for(i <- columns; ii = i.copy(sorted = th.sorted, keyType = th.keyType)) yield(ii.name, ii)}.toMap
      val op = Option(path)
      th.tableType match {
        case TableType.MEMORY_COLUMN => new MemColTable(th,m.mapValues(Column[K,Any](_)))
        case TableType.MEMORY_ROW => {
          val t:Map[K,Row[K]] = if(th.sorted) TreeMap.empty else Map.empty
          new MemRowTable(th, t, m)
        }
        case TableType.COLUMN => new ColTable(th, op, m)
        case TableType.ROW => new RowTable(th, op, m)
      }
  }
  
  def searchable[K](t: Table[K]): IndexingTable[K] = new IndexingTable(t)

}

class RowTable[K : Ordering](val header: TableHeader, val path: Option[String], colHeaders: Map[String,ColumnHeader]) extends Table[K] with FileTable[K]{
  var columnHeaders = openColHeaders.toMap
  colHeaders.values foreach (addCol(_))
  
  
  val dataColHeader = ColumnHeader(name = "datacol", maxValueSize = Some(4096), keyType = header.keyType, 
      valueType = DbType.BYTES, table = header.name, sorted = true)
      
  val dataCol: Column[K, Array[Byte]] = Column(dataColHeader, path.get+"/datacol")
  
  def columnHeader(cn: ColumnName): Option[ColumnHeader] = columnHeaders get cn
  
  def headers = colHeaders.valuesIterator
  
  def addCol[V](ch: ColumnHeader) = columnHeaders get ch.name match {
    case Some(h) => this
    case None => {
      columnHeaders += Tuple2(ch.name, ch)
      saveColHeader(ch)
      this
    }
  }
  
  def +=[V](row: K, col: ColumnName, v: V) = columnHeaders get col match {
    case Some(ch) => {
      dataCol get row match {
        case Some(b) => {
          val r = Row(row, header, b) + (col,v)
          dataCol + (row, Row.toBytes(r))
        }
        case None => {
          val r = Row(row) + (col,v)
          dataCol + (row,Row.toBytes(r))
        }
      }
      this
    }
    case None => throw new Error("No Such Column")
  }
  
  def +=(row: Row[K]) = {
    val f = for {
      kv <- row.iterator
      if(columnHeaders contains kv._1)
    } yield (kv)
    val r = Row(row.key,f.toMap)
    dataCol + (row.key, Row.toBytes(r))
    this
  }
  
  def -=(n: ColumnName, key: K) = dataCol get key match{
    case None => this
    case Some(b) => {
      val r = Row(key, header, b) - n
      dataCol + (key, Row.toBytes(r))
      this
    }
  }
  
  def -=(key: K) = {
    dataCol - key
    this
  }
  def column[V](n: ColumnName): Column[K,V] = columnHeaders get n match{
    case None => throw new Error("no such column")
    case Some(_) => {
      val c: Iterator[(K,V)] = for{
        kv <- dataCol.toIterator
        r = Row.apply(kv._1, header, kv._2)
        if(r.get(n) != None)
      } yield(r.key, r(n))
      Column(c.toMap, columnHeaders(n))
    }
  }
  
  def row(key: K): Row[K] = dataCol get key match{
    case None => Row(key)
    case Some(b) => Row(key, header, b)
  }
  
  def rows: Iterator[Row[K]] = for {
    kv <- dataCol.toIterator
  } yield(Row(kv._1, header, kv._2))
  
  def columns: Iterator[(String,Column[K,Any])] = for {
    cn <- columnHeaders.keysIterator
    c = column[Any](cn)
  } yield(cn,c)
  
  def flush = dataCol.flush
  def close = dataCol.close
  
  override def size: Long = dataCol.size
  
}


//maybe change this to immutable
class ColTable[K : Ordering](val header: TableHeader, val path: Option[String], colHeaders: Map[ColumnName, ColumnHeader]) extends Table[K] with FileTable[K]{
  
  import java.io._
  import scala.io._
  
  var columnHeaders: Map[ColumnName,ColumnHeader] = openColHeaders.toMap
  var cols: Map[ColumnName, Column[K,Any]] = openCols(columnHeaders.values).toMap
   
  colHeaders.valuesIterator.foreach(addCol(_))
  
  def columnHeader(cn: ColumnName) = colHeaders get cn
  def headers = columnHeaders.valuesIterator
  
  def addCol[V](ch: ColumnHeader) = columnHeaders contains ch.name match{
    case true => this
    case false => {
      columnHeaders = columnHeaders + ((ch.name, ch))
      saveColHeader(ch)
      val column: Column[K,Any] = Column(ch, colPath(ch))
      cols = cols + ((ch.name, column))
      this
    }
  }
  
  def +=[V](row: K, col: ColumnName, v: V) = cols get col match{
    case Some(c) => {
      val c1 = c + ((row,v))
      cols = cols.updated(col, c1)
      this
    }
    case None => throw new Error("no such column")
  }
  
  def +=(row: Row[K]) = {
    for(r <- row.toList){
      cols get r._1 match {
        case Some(c) => {
          val c1 = c + ((row.key,r._2))
          cols = cols.updated(r._1, c1)
        }
        case None => ()
      }
    }
    this
  }
  
  def -=(n: ColumnName, key: K) =  cols get n match{
    case Some(c) => {
      val c1 = c - key
      cols = cols.updated(n, c1)
      this
    }
    case None => throw new Error("no such column")
  }
  
  def -=(key: K) = {
    columnHeaders.keySet.foreach(n => this.-=(n,key))
    this
  }
  
  def column[V](n: ColumnName): Column[K,V] = {
    cols(n).asInstanceOf[Column[K,V]]
  }
  
  def rows: Iterator[Row[K]] = header.primColumn match {
    case Some(c) => for{
        pcol <- cols(c).toIterator
      } yield(row(pcol._1))
    case None => throw new Error("no primary column defined")
   }
  
  def columns: Iterator[(String,Column[K,Any])] = cols.toIterator
  
  def row(key: K): Row[K] = cols.values.foldLeft(Row[K](key,header))((r,c) => c get key match {
      case Some(v) => r + ((c.name,v))
      case None => r
    })
    
  def flush = {
    val v = cols.values.map(_.flush)
    Try(v.map(_.get))
  }
  
  def close = {
    val v = cols.values.map(_.close)
    Try(v.map(_.get))
  }
  override def size = header.primColumn match {
    case None => throw new Error("no primary column defined")
    case Some(cn) => cols(cn).size
  }
  
}

class MemRowTable[K: Ordering](val header: TableHeader,var table: Map[K, Row[K]],var colHeaders: Map[ColumnName,ColumnHeader]) extends Table[K] {
  import scala.collection.immutable.SortedMap
  
  def row(key: K): Row[K] = table(key)
  
  def rows: Iterator[Row[K]] = table.values.iterator
  
  val path = None
  
  def columnHeader(cn: ColumnName) = colHeaders get cn
  
  def headers = colHeaders.valuesIterator
  
  def addCol[V](c: ColumnHeader) = colHeaders get c.name match {
    case Some(_) => this
    case None => {
      colHeaders += ((c.name,c))
      this
    }
  }
  
  def column[V](cn: ColumnName): Column[K,V] = {
    val iter: Iterable[(K,V)] = for {
      r <- table.values
      if(r.get(cn) != None)
    } yield((r.key,r(cn)))
    header.sorted match {
      case false => Column(iter.toMap,colHeaders(cn))
      case true => Column(SortedMap(iter.to:_*),colHeaders(cn))
    }
  }
  
  def columns: Iterator[(String,Column[K,Any])] = for {
      i <- colHeaders.valuesIterator
    } yield(i.name, column(i.name))
  
  def +=(row: Row[K]) = {
      table += ((row.key,row))
      this
    }
  
  def +=[V](key: K, col: ColumnName, value: V) = colHeaders get col match {
      case None => throw new Error("no such column")
      case Some(_) => {
        val r = table get key match {
          case None => Row(key, Map(col -> value))
          case Some(i) => i + ((col,value))
        }
        this.+=(r)
      }
  }
  
  def -=(n: ColumnName, key: K) = table get key match {
    case None => this
    case Some(r) => {
      table.updated(key, r - n)
      this
    }
  }
  def -=(key: K) = {
    table -= key
    this
  }
  
  def flush: Try[Unit] = Success()
  def close: Try[Unit] = Success()
  override def size: Long = table.size
}

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
