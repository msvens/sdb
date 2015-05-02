package org.mellowtech.dbm


import org.mellowtech.core.util.DelDir
import scala.util.Try
import scala.util.Success
import scala.util.Failure

trait Db {
  def name: String
  def get[K](table: String): Option[Table[K]]
  def apply[K](table: String): Table[K] = get[K](table)  match{
    case Some(t) => t
    case None => throw new NoSuchElementException
  }
  def +=[K](th: TableHeader): Db
  def -=(table: String): Db
  def flush: Try[Unit]
  def close: Try[Unit]
  
}   

class FileDb(val name: String, val path: String) extends Db {
  import java.io.File
  import scala.io.Source
  import TableType._
  
  var tables: Map[String,Table[_]] = Map()
  
  createFiles
  
  def createFiles() = {
    val f = new File(path)
    f.mkdirs
    println(path)
  }
  
  private def deleteFiles() = DelDir.d(path)
  
  private def openDb():Try[Unit] = {
    
    def open(f: File): Table[_] = {
        val s = Source.fromFile(f.getAbsolutePath+"/"+Db.TableHeaderFile)
        val th = TableHeader fromJson s.mkString
        Db.openTable(th,f.getAbsolutePath)
      }
    

    val tlist = for {
      f <- new File(path).listFiles; if f.isDirectory()
      t = Try(open(f))
      if t.isSuccess
    } yield(t.get.name, t.get)
    
    tables = tlist.toMap
    
    Success()
    
  }
  
  def flush:Try[Unit] = Try(
      tables.values.foreach(x => {x.flush; saveHeader(x.header)})
  )
  
  def close: Try[Unit] = Try(
    tables.values.foreach(x => {x.close; saveHeader(x.header)})    
  )
  
  def get[K](table: String): Option[Table[K]] = tables get table match{
    case None => None
    case Some(t) => t match {
      case tt: Table[K] => Some(tt)
      case _ => throw new ClassCastException
    }
  }
  
  def +=[K](th: TableHeader): Db = tables get th.name match{
    case Some(t) => this
    case None => {
      val f = new File(path+"/"+th.name)
      f.mkdir
      saveHeader(th)
      tables = tables + Tuple2(th.name,Db.openTable(th, f.getAbsolutePath))
      this
    }
  }
  
  def -=(table: String): Db = tables get table match{
    case None => this
    case Some(t) => {
      tables = tables - table
      t.close
      DelDir.d(path + "/" + table)
      this
    }
  }
  
  private def saveHeader(th: TableHeader): Try[Unit] = Try{
    import java.io.FileWriter
    val w = new FileWriter(new File(path+"/"+th.name, Db.TableHeaderFile))
    w.write(TableHeader asJson th)
    w.close
  }
  
}

object Db{
  import com.github.nscala_time.time.Imports._
  import akka.actor.ActorSystem
  import java.util.Date
  
  val TableHeaderFile = "header.mth"
  
  val system = ActorSystem("dbmsystem")
  def shutdown = system.shutdown
  
  def openTable(th: TableHeader, p: String): Table[_] = th.keyType match{  
    case DbType.INT => Table[Int](th, p)
    case DbType.STRING => Table[String](th, p)
    case DbType.BYTE => Table[Byte](th, p)
    case DbType.CHAR => Table[Char](th, p)
    case DbType.SHORT => Table[Short](th, p)
    case DbType.LONG => Table[Long](th, p)
    case DbType.FLOAT => Table[Float](th, p)
    case DbType.DOUBLE => Table[Double](th, p)
    case DbType.DATE => Table[Date](th, p)
    case DbType.BYTES => Table[Iterable[Byte]](th,p)
  }
  def apply(name: String, path: String): Db = new FileDb(name, path)  
}


