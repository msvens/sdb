/**
 *
 */
package org.mellowtech.dbm

import org.apache.lucene.search.SortField
import org.apache.lucene.document._
import org.apache.lucene.index.IndexableField
import org.apache.lucene.index.Term
import org.apache.lucene.search.NumericRangeQuery
import org.apache.lucene.search.Query
import org.apache.lucene.search.TermQuery
import org.apache.lucene.search.BooleanClause
  
import SearchType._
import DbType._

class IndexingTable[K](var t: Table[K]) extends Table[K] with TableSearcher[K] {
  import scala.util.Try
  
  def path = t.path
  
  def row(key: K): Row[K] = t.row(key)
  def header: TableHeader = t.header
  def column[V](n: ColumnName): Column[K,V] = t.column(n)
  def rows: Iterator[Row[K]] = t.rows
  def columns: Iterator[(String,Column[K,Any])] = t.columns
  override def columnHeader(cn: ColumnName): Option[ColumnHeader] = t.columnHeader(cn)
  def headers = t.headers
  def addCol[V](c: ColumnHeader) = {
    t.addCol(c)
    this
  }
  def +=[V](key: K, col: ColumnName, v: V) = addIdx{
    t += (key, col, v)
    row(key)
  }
  def +=(r: Row[K]) = addIdx{
    t += r
    row(r.key)
  }
  
  def -=(n: ColumnName, key: K) = addIdx{
    t -= (n,key)
    row(key)
  }
  def -=(key: K) = delIdx{
    t -= key
    key
  }
  def flush: Try[Unit] = for (tt <- t.flush) yield dbi.flush
  def close: Try[Unit] = for{
    tt <- dbi.close
  } yield t.close
  
  override def size: Long = t.size
}

trait TableSearcher[K] { that: Table[K] =>
  
  println(header+" "+path)
  val dbi: DbIndexer[K] = DbIndexer(header,path)(that)
  
  def addIdx(f: => Row[K]): that.type = {
    dbi.update(f)
    this
  }
  
  def delIdx(f: => K): that.type = {
    dbi.delete(f)
    this
  }
  
  def queryKeys[A](cn: String, q: String, sortBy: Seq[ColumnName]): Seq[K] = this.columnHeader(cn) match {
    case Some(ch) => dbi.rows(RowQuery[A](ch, q, dbi.parser), toHeader(sortBy))
    case None => throw new Error("no such column")
  }
  
  def queryKeys(q: Query, sortBy: Seq[ColumnName]): Seq[K] = dbi.rows(q, toHeader(sortBy))
  def queryKeys(q: String, sortBy: Seq[ColumnName]): Seq[K] = dbi.rows(q, toHeader(sortBy))
  def queryKeys[A](q: RowQuery[A], sortBy: Seq[ColumnName]): Seq[K] = dbi.rows(q, toHeader(sortBy))
  def queryKeys(q: Seq[RowQuery[_]], sortBy: Seq[ColumnName]): Seq[K] = dbi.rows(q, toHeader(sortBy))
  
  def toRows(keys: Seq[K]): Seq[Row[K]] = for(k <- keys) yield row(k)
  
  def refresh: Unit = dbi.refresh
  
  private def toHeader(sortBy: Seq[ColumnName]): Seq[ColumnHeader] = for {
    cn <- sortBy
    ch = columnHeader(cn)
    if (ch != None)
  } yield(ch.get) 
}

case class DbDocument[K](row: Row[K], doc: Document = new Document)(implicit t: Table[K]) {
  doc.add(Converter.rowKey(DbDocument.RowKey,row.key))
  row.iterator foreach (kv => {
      t.columnHeader(kv._1) match {
        case Some(c) => c.search match {
          case SearchType.FIELD => doc add Converter.toField(kv._1, kv._2, false, false)
          case SearchType.TEXT => doc add Converter.toField(kv._1, kv._2, false, true)
          case SearchType.NONE =>
        }
        case None =>
      }
    }
  )
  println(doc)
}

object DbDocument {
  val RowKey = "rowKey"
  def toQuery[A](k: A): Query = Converter.toQuery(RowKey, k)
  def toQuery[A](row: Row[A]): Query = toQuery(row.key)
}

class RowQuery[V](val query: Query, val occur: BooleanClause.Occur) {
  
  //def query: Query = Converter.toQuery(column, value)
  
}

object RowQuery {
  import org.apache.lucene.queryparser.classic.QueryParser;
  def apply[V](col: String, q: String, occur: BooleanClause.Occur = BooleanClause.Occur.MUST) = 
    new RowQuery[V](Converter.toQuery(col, q), occur)
  def apply[V](ch: ColumnHeader, q: String, p: QueryParser) = ch.search match {
    case SearchType.TEXT => p.parse(ch.name+":"+q)
    case _ => Converter.toQuery(ch.name, q)
  }
}


class DbIndexer[K](th: TableHeader, path: Option[String])(implicit t: Table[K]) {
  import scala.util.Try
  import java.io.File
  import org.apache.lucene.store.{Directory,FSDirectory,RAMDirectory}
  import org.apache.lucene.analysis.Analyzer
  import org.apache.lucene.analysis.standard.StandardAnalyzer
  import org.apache.lucene.util.Version
  import org.apache.lucene.index.{IndexWriter,IndexWriterConfig,Term}
  import org.apache.lucene.search._
  import org.apache.lucene.queryparser.classic.QueryParser;
  import org.mellowtech.dbm.actor._
  
  val d = path match {
    case Some(p) => {
       val idxPath = p + "/tblidx"
       val f = new File(idxPath)
       if(!f.exists()) f.mkdirs()
       FSDirectory.open(f)
    }
    case None => new RAMDirectory
  }
  
  //val analyzer = new StandardAnalyzer(Version.LUCENE_4_10_3);
  
  val iwf = new IndexWriterConfig(Version.LUCENE_4_10_3, new StandardAnalyzer());
    iwf.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);

  val indexWriter = new IndexWriter(d, iwf)
  indexWriter.commit()
  //val indexWriter = new NRTManager.TrackingIndexWriter(new IndexWriter(d, iwf));
  val searchManager = new SearcherManager(d, new SearcherFactory());
  val parser = new QueryParser(Version.LUCENE_4_10_3, "one", new StandardAnalyzer());
  
  val keyClass = th.keyType match {
    case STRING | CHAR | DATE => DbIndexer.TEXT_KEY
    case BYTES => DbIndexer.BINARY_KEY
    case _ => DbIndexer.NUMERIC_KEY
  }
  
  private val refreshActor = DbIndexer.system.actorOf(SearchRefresh.props(500,500,this))
  
  def refresh: Try[Unit] = Try(searchManager.maybeRefresh)
  
  def delete(key: K): Try[Unit] = for {
    d <- Try(indexWriter.deleteDocuments(DbDocument.toQuery(key)))
    r <- Try(indexWriter.commit)
  } yield r
  
  def update(row: Row[K]): Try[Unit] = for {
      x <- Try(indexWriter.deleteDocuments(DbDocument.toQuery(row)))
      d = DbDocument(row)
      y <- Try(indexWriter.addDocument(d.doc))
      r <- Try(indexWriter.commit)
    } yield r
    
  private def key(sd: ScoreDoc, is: IndexSearcher):K = {
      val d = th.keyType match {
        case STRING => is.doc(sd.doc).getField(DbDocument.RowKey).stringValue
        case BYTES => is.doc(sd.doc).getField(DbDocument.RowKey).binaryValue.bytes
        case INT => is.doc(sd.doc).getField(DbDocument.RowKey).numericValue.intValue
        case LONG => is.doc(sd.doc).getField(DbDocument.RowKey).numericValue.longValue
        case DOUBLE => is.doc(sd.doc).getField(DbDocument.RowKey).numericValue.doubleValue
        case FLOAT => is.doc(sd.doc).getField(DbDocument.RowKey).numericValue.floatValue
        case SHORT => is.doc(sd.doc).getField(DbDocument.RowKey).numericValue.shortValue
        case CHAR => is.doc(sd.doc).getField(DbDocument.RowKey).stringValue.charAt(0)
      }
      d.asInstanceOf[K]
    }
  
  def rows(q: Query): Seq[K] = {
    //Yes
    searchManager.maybeRefresh
    val is = searchManager.acquire
    val td = is.search(q, Int.MaxValue)
    println("td: "+td.scoreDocs.length+" query: "+q)
    try{
      if(td.scoreDocs == null) List.empty else for {
          sd <- td.scoreDocs
        } yield key(sd, is)
        
    } finally searchManager.release(is)
  }
  
  def rows(column: String, value: String): Seq[K] = rows(new TermQuery( new Term(column,value)))
  
  def flush: Try[Unit] = Try(indexWriter.commit)
  
  def close: Try[Unit] = {
    DbIndexer.system.stop(refreshActor)
    for {
      iw <- Try(indexWriter.close)
    } yield searchManager.close
  }
  def rows(q: Query, sortBy: Seq[ColumnHeader]): Seq[K] = {
    if(sortBy.isEmpty) 
      rows(q) 
    else {
      val s = new Sort
      val fields = {for {
        ch <- sortBy
        sf = Converter.toSortField(ch)
        if(sf != None)
      } yield sf}.flatten
      s.setSort(fields:_*)
      val is = searchManager.acquire
      val td = is.search(q, Int.MaxValue, s)
      try{
        if(td.scoreDocs == null) List.empty else for {
            sd <- td.scoreDocs
          } yield key(sd, is)   
      } finally searchManager.release(is)
    }
  }
  
  def rows(q: String, sortBy: Seq[ColumnHeader]): Seq[K] = rows(parser.parse(q), sortBy)
  def rows[A](q: RowQuery[A], sortBy: Seq[ColumnHeader]): Seq[K] = rows(q.query, sortBy)
  def rows(q: Seq[RowQuery[_]], sortBy: Seq[ColumnHeader]): Seq[K] = {
     val qq = q.foldLeft(new BooleanQuery)((a,b)=>{a.add(b.query,b.occur);a})
     rows(qq, sortBy)
  }
  
  
}

object DbIndexer {
  import akka.actor.ActorSystem
  
  val BINARY_KEY = 1
  val NUMERIC_KEY = 2
  val TEXT_KEY = 3
  
  val system = ActorSystem("dbmsystem")
  
  def shutdown = system.shutdown
  
  def apply[K](th: TableHeader, path: Option[String])(implicit t: Table[K]) = {
    new DbIndexer(th,path)
  }
  
}

/**
 * @author msvens
 *
 */
object Converter {
  
  def toSortField(ch: ColumnHeader): Option[SortField] = ch.search match {
    case FIELD => {
      val f = ch.valueType match{
        case STRING => SortField.Type.STRING
        case INT => SortField.Type.INT
        case LONG => SortField.Type.LONG
        case DOUBLE => SortField.Type.DOUBLE
        case FLOAT => SortField.Type.FLOAT
        case BYTES => SortField.Type.BYTES
        case _ => SortField.Type.STRING
        
      }
      Some(new SortField(ch.name, f))
    }
    case _ => None
  }
  
  def toField[V](ch: ColumnHeader, value: V): Option[IndexableField] = ch.search match{
    case NONE => None
    case _ => Some(toField(ch.name, value, false, false))
  }
  
  
  def toField[V](name: String, value: V, rowKey: Boolean, text: Boolean): IndexableField = {
    val s = if(rowKey) Field.Store.YES else Field.Store.NO
    value match {
      case v: String => if(text) new TextField(name, v, s) else new StringField(name, v, s)
      case v: Int => new IntField(name, v, s)
      case v: Short => new IntField(name, v, s)
      case v: Long => new LongField(name, v, s)
      case v: Double => new DoubleField(name, v, s)
      case v: Float => new FloatField(name, v, s)
      case x => new StringField(name, x.toString, s)
    }
  }
  
  def toQuery[V](name: String, value: V): Query = value match{
    case v: String => new TermQuery(new Term(name, v))
    case v: Int => NumericRangeQuery.newIntRange(name, v, v, true, true)
    case v: Short => NumericRangeQuery.newIntRange(name, v, v, true, true)
    case v: Long => NumericRangeQuery.newLongRange(name, v, v, true, true)
    case v: Double => NumericRangeQuery.newDoubleRange(name, v, v, true, true)
    case v: Float => NumericRangeQuery.newFloatRange(name, v, v, true, true)
    case x => new TermQuery(new Term(name, x.toString))
  }
  
  def rowKey[A](name: String, value: A): IndexableField = {
    val s = Field.Store.YES
    value match {
      case v: String => new StringField(name, v, s)
      case v: Int => new IntField(name, v, s)
      case v: Short => new IntField(name, v, s)
      case v: Long => new LongField(name, v, s)
      case v: Double => new DoubleField(name, v, s)
      case v: Float => new FloatField(name, v, s)
      case x => new StringField(name, x.toString, s)
    }
  }
}