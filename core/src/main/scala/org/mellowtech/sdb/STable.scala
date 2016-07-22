package org.mellowtech.sdb




import scala.collection.immutable.TreeMap
import scala.util.{Success, Failure, Try}

/**
 * Created by msvens on 17/09/15.
 */
abstract class STable[A : Ordering] {

  def size: Long
  def column[B](name: ColumnName): SColumn[A,B]
  def contains(key: A) = row(key) != None
  def row(key: A): Option[SRow]
  def header: TableHeader
  def columnIterator: Iterator[(String,SColumn[A,Any])]
  def rowIterator: Iterator[KeyRow[A]]
  def path: Option[String] = None

  def addColumn(ch: ColumnHeader): STable.this.type

  def internalPut(key: A, row: SRow): Try[Unit]

  def put(rk: OptKeyRow[A]): A = STable.verify(rk._2, this) match {
    case Failure(e) => throw e
    case Success(_) => {
      val r = STable.autoIncKey(rk._1, this)
      internalPut(r, rk._2) match {
        case Success(_) => r
        case Failure(e) => throw e
      }
    }
  }

  def put[B](key: Option[A], cn: ColumnName, v: B): STable.this.type = this + (key, SRow(Map(cn -> v)))

  def set[B](key: A, cn: ColumnName, v: B): STable.this.type = set(key, Map(cn -> v))

  def set(key: A, vals: Map[ColumnName, Any]): STable.this.type = {
    for {
      old <- row(key) match {
        case None => Failure(new NoSuchElementException("could not find key: " + key))
        case Some(r) => Success(r)
      }
      n = old ++ vals
      ver <- STable.verify(n, this)
    } yield internalPut(key, n)
  } match {
    case Success(_) => this
    case Failure(e) => throw e
  }

  def set(key: A, r: SRow): STable.this.type = set((key,r))

  def set(rk: KeyRow[A]): STable.this.type = {
    for {
      vt <- STable.verify(rk._2, this)
      ct <- if (contains(rk._1)) Success() else Failure(new NoSuchElementException)
    } yield internalPut(rk._1, rk._2)
  } match {
      case Success(_) => this
      case Failure(e) => throw e
  }

  def +=(rk: KeyRow[A]): STable.this.type = set(rk)

  def +(rk: OptKeyRow[A]): STable.this.type = {
    put(rk)
    this
  }

  def -(key: A): STable.this.type

  def columnHeader(ch: ColumnName): Option[ColumnHeader] = column[Any](ch).header match {
    case null => None
    case ch: ColumnHeader => Some(ch)
  }
  def columnHeaders: Iterator[ColumnHeader] = for(a <- columnIterator) yield a._2.header


  def find[B](cn: ColumnName, v: B): Iterator[KeyRow[A]] = columnHeader(cn) match {
    case None => throw new NoSuchElementException("undefined column")
    case Some(ch) => for{
      r <- rowIterator
      rv = r._2.get(cn)
      if rv.isDefined && rv.get.equals(v)
    } yield r
  }
}

trait IndexTable[A] {this: STable[A] =>
  def columnIndex(c: ColumnName): Int
  def columnName(idx: Int): String
}

trait FileTable[A] extends Closable {this: STable[A] =>
  import java.io._
  import scala.io._

  val ColHead = "col.head"

  def colPath(ch: ColumnHeader): String = new File(path.get+"/"+ch.name+"/data").getAbsolutePath


  def openColHeader(d: File): ColumnHeader = {
    val json = Source.fromFile(new File(d,ColHead)).mkString
    ColumnHeader.fromJson(json)
  }

  def openColHeaders: Seq[(ColumnName, ColumnHeader)] = for {
    f <- new File(path.get).listFiles()
    if f.isDirectory && !f.getName.equals("tblidx")
    c = openColHeader(f)
  } yield(c.name, c)

  def saveColHeader(ch: ColumnHeader): String = {
    val d = new File(path.get+"/"+ch.name)
    if(!d.exists) d.mkdir
    val w = new FileWriter(new File(d, ColHead))
    w.write(ColumnHeader asJson ch)
    w.close()
    d.getAbsolutePath
  }

  def openCols(headers: Iterable[ColumnHeader])(implicit ord: Ordering[A]): Iterable[(ColumnName,SColumn[A,Any])] = for {
    ch <- headers
    d = new File(path.get + "/" + ch.name + "/data")
  } yield (ch.name, SColumn[A,Any](ch, d.getAbsolutePath))
}

import org.mellowtech.sdb.search.TableSearcher

class IndexingTable[A : Ordering](var t: STable[A]) extends STable[A] with TableSearcher[A] with Closable {
  import scala.util.Try

  override def path = t.path
  
  override def size: Long = t.size

  override def column[B](name: ColumnName): SColumn[A, B] = t.column(name)

  override def internalPut(k: A, r: SRow): Try[Unit] = t.internalPut(k,r) match {
    case Failure(e) => Failure(e)
    case Success(_) =>
      val kr = (k, row(k).get)
      addIdx(kr)
      Success()
  }

  override def columnIterator: Iterator[(String, SColumn[A, Any])] = t.columnIterator

  override def header: TableHeader = t.header

  override def -(key: A): IndexingTable.this.type = delIdx{t - key; key}


  override def addColumn(ch: ColumnHeader): IndexingTable.this.type = {t.addColumn(ch); this}

  override def row(key: A): Option[SRow] = t.row(key)

  override def rowIterator: Iterator[KeyRow[A]] = t.rowIterator

  override def flush: Try[Unit] = {
    def ft = t match {
      case c: Closable => c.flush
      case _ => Success()
    }
    for(s <- ft) yield dbi.flush
  }

  override def close: Try[Unit] = {
    def ct = t match {
      case c: Closable => c.close
      case _ => Success()
    }
    for(s <- ct) yield dbi.close
  }

  override def columnHeaders = t.columnHeaders

  override def columnHeader(cn: ColumnName) = t.columnHeader(cn)

  override def find[B](cn: ColumnName, v: B): Iterator[KeyRow[A]] = this.columnHeader(cn) match {
    case None => throw new NoSuchElementException("undefined column: "+cn)
    case Some(ch) => ch.search match {
      case SearchType.NONE => t.find(cn,v)
      case _ => toRows(this.queryKeys(cn,v, Seq.empty))
    }
  }
}

object STable {

  /**
   * Verify that a row conforms to the column definitions of the table
 *
   * @param r row to check
   * @param t table to check against
   * @tparam A key type
   * @return Success or an error descring the failure
   */
  def verify[A](r: SRow, t: STable[A]): Try[Unit] = {
    def noNull = {
      t.columnHeaders.find { ch => if (!ch.nullable && !r.contains(ch.name)) true else false }
    }
    def noCol = r.iterator.find(kv => t.columnHeader(kv._1) match {
      case None => true
      case Some(ch) => dbType(kv._2) != ch.valueType
    })
    noNull match {
      case Some(c) => Failure(new Error(s"non nullable column was not set: ${noNull.get.name}"))
      case _ => noCol match {
        case Some(cc) =>
          if(t.columnHeader(cc._1).isEmpty)
            Failure(new Error("column does not exist: "+cc._1))
          else
            Failure(new Error("value type does not match for column: "+dbType(cc._2)))
        case None => Success()
      }
    }
  }

  def autoIncKey[A](key: Option[A], t: STable[A]): A = t.header.genKey match {
    case true => t.header.incrementAndGet //dont test if key.isEmpty
    case false => key.get
  }


  //FIXA: NOT CORRECT!!!!!
  private def fill[A](items: Seq[(A,ColumnName,Any)])(t: STable[A]): STable[A] = items.foldLeft(t){(b, i) =>
    b
  }


  def apply(): STable[String] = {
    import scala.util.Random
    val tname = "table"+Random.alphanumeric.take(10).mkString("")
    val th = TableHeader(tname, DbType.STRING, tableType = TableType.MEMORY_COLUMN)
    apply(th)(Ordering[String])
  }

  def apply[A](th: TableHeader)(implicit ord: Ordering[A]): STable[A] = apply(th, Nil, null, Seq.empty:_*)//new MemColTable(th, Map.empty)

  def apply[A](th: TableHeader, columns: Seq[ColumnHeader])(implicit ord: Ordering[A]): STable[A] = apply(th, columns, Seq.empty:_*)

  def apply[A](th: TableHeader, columns: Seq[ColumnHeader], items: (A,ColumnName,Any)*)(implicit ord: Ordering[A]): STable[A] = apply(th, columns, null, items:_*)

  def apply[A](th: TableHeader, path: String)(implicit ord: Ordering[A]): STable[A] = apply(th, Seq.empty, path, Seq.empty:_*)

  def apply[A](th: TableHeader, columns: Seq[ColumnHeader], path: String)(implicit ord: Ordering[A]): STable[A] = apply(th,columns, path, Seq.empty:_*)

  def apply[A](th: TableHeader, columns: Seq[ColumnHeader], path: String, items: (A,ColumnName,Any)*)(implicit ord: Ordering[A]): STable[A] =
    fill(items) {
      val m = {for(i <- columns; ii = i.copy(sorted = th.sorted, keyType = th.keyType)) yield(ii.name, ii)}.toMap
      val op = Option(path)
      val t: STable[A] = th.tableType match {
        case TableType.MEMORY_COLUMN => new MemColTable(th,m.mapValues(SColumn[A,Any](_)))
        case TableType.MEMORY_ROW => {
          val t:Map[A,SRow] = if(th.sorted) TreeMap.empty else Map.empty
          new MemRowTable(th, t, m)
        }
        case TableType.COLUMN => new ColTable(th, op, m)
        case TableType.ROW => new RowTable(th, op, m)
      }
      th.indexed match {
        case false => t
        case true => new IndexingTable(t)
      }
    }

  def searchable[A](t: STable[A])(implicit ord: Ordering[A]): IndexingTable[A] = new IndexingTable(t)
}
