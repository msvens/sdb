/**
 *
 */
package org.mellowtech.sdb.search


//import akka.actor.ActorSystem
//import java.io.File
import org.apache.lucene.document._
import org.apache.lucene.index._

import scala.util.{Success, Failure}

//import org.apache.lucene.analysis.standard.StandardAnalyzer
//import org.apache.lucene.queryparser.classic.QueryParser
import org.apache.lucene.search._
import org.apache.lucene.store.{RAMDirectory, FSDirectory}
//import org.apache.lucene.util.Version
import org.mellowtech.sdb._
import org.mellowtech.sdb.DbType._
import org.mellowtech.sdb.SearchType._

//import scala.util.Try



trait TableSearcher[A] { that: STable[A] =>

  //println(header+" "+path)
  val dbi: DbIndexer[A] = DbIndexer(header,path)(that)
  
  def addIdx(f: => KeyRow[A]): that.type = {
    dbi.update(f)
    this
  }
  
  def delIdx(f: => A): that.type = {
    dbi.delete(f)
    this
  }
  
  def queryKeys[B](cn: ColumnName, q: B, sortBy: Seq[ColumnName] = Seq.empty): Iterator[A] = this.columnHeader(cn) match {
    case Some(ch) => dbi.rows(RowQuery(ch, q, dbi.parser), toHeader(sortBy))
    case None => throw new Error("no such column")
  }
  
  def queryKeys(q: Query, sortBy: Seq[ColumnName]): Iterator[A] = dbi.rows(q, toHeader(sortBy))
  def queryKeys(q: String, sortBy: Seq[ColumnName]): Iterator[A] = dbi.rows(q, toHeader(sortBy))
  def queryKeys(q: RowQuery, sortBy: Seq[ColumnName]): Iterator[A] = dbi.rows(q, toHeader(sortBy))
  def queryKeys(q: Seq[RowQuery], sortBy: Seq[ColumnName]): Iterator[A] = dbi.rows(q, toHeader(sortBy))
  
  def toRows(keys: Iterator[A]): Iterator[KeyRow[A]] = for(k <- keys.toIterator) yield (k,row(k).get)
  
  def refresh(): Unit = dbi.refresh
  
  private def toHeader(sortBy: Seq[ColumnName]): Seq[ColumnHeader] = for {
    cn <- sortBy
    ch = columnHeader(cn)
    if ch.isDefined
  } yield ch.get
}

case class DbDocument[A](keyRow: KeyRow[A], doc: Document = new Document)(implicit t: STable[A]) {
  doc.add(Converter.rowKey(DbDocument.RowKey,keyRow._1))
  keyRow._2.iterator foreach (kv => {
      t.columnHeader(kv._1) match {
        case Some(c) => c.search match {
          case SearchType.FIELD => doc add Converter.toField(kv._1, kv._2, false, true)
          case SearchType.TEXT => doc add Converter.toField(kv._1, kv._2, false, false)
          case SearchType.NONE =>
        }
        case None =>
      }
    }
  )
  //println(doc)
}

object DbDocument {
  val RowKey = "rowKey"
  def toQuery[A](k: A): Query = Converter.toQuery(RowKey, k)
  //def toQuery[A](row: SRow): Query = toQuery(row.key)
}

class RowQuery(val query: Query, val occur: BooleanClause.Occur) {
  
  //def query: Query = Converter.toQuery(column, value)
  
}

object RowQuery {
  import org.apache.lucene.queryparser.classic.QueryParser
  def apply[V](col: String, q: V, occur: BooleanClause.Occur = BooleanClause.Occur.MUST) = 
    new RowQuery(Converter.toQuery(col, q), occur)
  def apply[V](ch: ColumnHeader, q: V, p: QueryParser) = ch.search match {
    case SearchType.TEXT => p.parse(ch.name+":"+q)
    case _ => Converter.toQuery(ch.name, q)
  }
}


class DbIndexer[A](th: TableHeader, path: Option[String])(implicit t: STable[A]) {
  import scala.util.Try
  import java.io.File
  import org.apache.lucene.store.{Directory,FSDirectory,RAMDirectory}
  //import org.apache.lucene.analysis.Analyzer
  import org.apache.lucene.analysis.standard.StandardAnalyzer
  import org.apache.lucene.util.Version
  import org.apache.lucene.index.{IndexWriter,IndexWriterConfig,Term}
  import org.apache.lucene.search._
  import org.apache.lucene.queryparser.classic.QueryParser;
  import org.mellowtech.sdb.actor._
  import scala.collection.Iterator
  
  //println("creating DBIndexer")
  val d = path match {
    case Some(p) =>
      val idxPath = p + "/tblidx"
      //println("opening index: "+idxPath)
      val f = new File(idxPath)
      if(!f.exists()) f.mkdirs()
      FSDirectory.open(f.toPath())
    case None => 
      //println("opening ram directory")
      new RAMDirectory
  }
  
  //val analyzer = new StandardAnalyzer(Version.LUCENE_4_10_3);
  
  val iwf = new IndexWriterConfig(new StandardAnalyzer())
    iwf.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND)

  val indexWriter = new IndexWriter(d, iwf)
  indexWriter.commit()
  //val indexWriter = new NRTManager.TrackingIndexWriter(new IndexWriter(d, iwf));
  //val searchManager = new SearcherManager(indexWriter, true, new SearcherFactory())
  //searchManager.maybeRefresh()
  val searchManager = new SearcherManager(d, new SearcherFactory())
  val parser = new QueryParser("one", new StandardAnalyzer())
  
  val keyClass = th.keyType match {
    case STRING | CHAR | DATE => DbIndexer.TEXT_KEY
    case BYTES => DbIndexer.BINARY_KEY
    case _ => DbIndexer.NUMERIC_KEY
  }
  
  private val refreshActor = Db.system.actorOf(SearchRefresh.props(500,500,this))
  
  def refresh: Try[Unit] = Try(searchManager.maybeRefresh)
  
  def delete(key: A): Try[Unit] = for {
    d <- Try(indexWriter.deleteDocuments(DbDocument.toQuery(key)))
    r <- Try(indexWriter.commit())
  } yield r
  
  def update(keyRow: KeyRow[A]): Try[Unit] = for {
      x <- Try(indexWriter.deleteDocuments(DbDocument.toQuery(keyRow._1)))
      d = DbDocument(keyRow)
      y <- Try(indexWriter.addDocument(d.doc))
      r <- Try(indexWriter.commit())
    } yield r
    
  private def key(sd: ScoreDoc, is: IndexSearcher):A = {
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
      d.asInstanceOf[A]
    }
  
  def rows(q: Query): Iterator[A] = {
    //Yes
    searchManager.maybeRefresh
    val is = searchManager.acquire
    val td = is.search(q, Int.MaxValue)
    //println("td: "+td.scoreDocs.length+" query: "+q)
    try{
      if(td.scoreDocs == null) Iterator.empty else for {
          sd <- td.scoreDocs.toIterator
        } yield key(sd, is)
        
    } finally searchManager.release(is)
  }
  
  def rows(column: String, value: String): Iterator[A] = rows(new TermQuery( new Term(column,value)))
  
  def flush: Try[Unit] = Try(indexWriter.commit())
  
  def close: Try[Unit] = {
    Db.system.stop(refreshActor)
    for {
      iw <- Try(indexWriter.close())
    } yield searchManager.close()
  }
  def rows(q: Query, sortBy: Seq[ColumnHeader]): Iterator[A] = {
    if(sortBy.isEmpty) 
      rows(q) 
    else {
      val s = new Sort
      val fields = {for {
        ch <- sortBy
        sf = Converter.toSortField(ch)
        if sf.isDefined
      } yield sf}.flatten
      s.setSort(fields:_*)
      val is = searchManager.acquire
      val td = is.search(q, Int.MaxValue, s)
      try{
        if(td.scoreDocs == null) Iterator.empty else for {
            sd <- td.scoreDocs.toIterator
          } yield key(sd, is)   
      } finally searchManager.release(is)
    }
  }
  
  def rows(q: String, sortBy: Seq[ColumnHeader]): Iterator[A] = rows(parser.parse(q), sortBy)
  def rows(q: RowQuery, sortBy: Seq[ColumnHeader]): Iterator[A] = rows(q.query, sortBy)
  def rows(q: Seq[RowQuery], sortBy: Seq[ColumnHeader]): Iterator[A] = {
     val qq = q.foldLeft(new BooleanQuery)((a,b)=>{a.add(b.query,b.occur);a})
     rows(qq, sortBy)
  }
  
  
}

object DbIndexer {
  
  val BINARY_KEY = 1
  val NUMERIC_KEY = 2
  val TEXT_KEY = 3
  
  def apply[A](th: TableHeader, path: Option[String])(implicit t: STable[A]) = {
    new DbIndexer(th,path)
  }
  
}

/**
 * @author msvens
 *
 */
object Converter {
  
  def toSortField(ch: ColumnHeader): Option[SortField] = ch.search match {
    case FIELD =>
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
    case v: Short => NumericRangeQuery.newIntRange(name, v.asInstanceOf[Int], v.asInstanceOf[Int], true, true)
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