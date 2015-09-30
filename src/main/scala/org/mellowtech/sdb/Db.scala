package org.mellowtech.sdb


import org.mellowtech.core.util.DelDir
import scala.util.Try
import scala.util.Success
import scala.util.Failure

trait Db extends Closable{
  def name: String
  def get[A](table: String): Option[STable[A]]
  def apply[A](table: String): STable[A] = get[A](table)  match{
    case Some(t) => t
    case None => throw new NoSuchElementException
  }
  def +=[A](th: TableHeader): Db
  def -=(table: String): Db
  def flush: Try[Unit]
  def close: Try[Unit]
}   

class FileDb(val name: String, val path: String) extends Db {
  import java.io.File
  import scala.io.Source
  import TableType._
  
  var tables: Map[String,STable[_]] = Map()
  
  createFiles()
  
  def createFiles() = {
    val f = new File(path)
    f.mkdirs
    //println(path)
  }
  
  private def deleteFiles() = DelDir.d(path)
  
  private def openDb():Try[Unit] = {
    
    def open(f: File): STable[_] = {
        val s = Source.fromFile(f.getAbsolutePath+"/"+Db.TableHeaderFile)
        val th = TableHeader fromJson s.mkString
        Db.openTable(th,f.getAbsolutePath)
      }
    

    val tlist = for {
      f <- new File(path).listFiles; if f.isDirectory()
      t = Try(open(f))
      if t.isSuccess
    } yield(t.get.header.name, t.get)
    
    tables = tlist.toMap
    
    Success()
    
  }

  def flush: Try[Unit] = {
    val sc = for{
      t <- tables.values
      if t.isInstanceOf[Closable]
    } yield t.asInstanceOf[Closable].flush
    sc.++(for {
      t <- tables.values
    } yield saveHeader(t.header))
    sc.find(_.isFailure) match {
      case Some(f) => f
      case None => Success()
    }
  }

  def close:Try[Unit] = Try{
    val sc = for{
      t <- tables.values
      if t.isInstanceOf[Closable]
    } yield t.asInstanceOf[Closable].close
    sc.++(for {
      t <- tables.values
    } yield saveHeader(t.header))
    sc.find(_.isFailure) match {
      case None => Success()
      case f => f.get
    }
  }
  
  def get[A](table: String): Option[STable[A]] = tables get table match{
    case None => None
    case Some(t) => t match {
      case tt: STable[A] => Some(tt)
      case _ => throw new ClassCastException
    }
  }
  
  def +=[A](th: TableHeader): Db = tables get th.name match{
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
    case Some(t) =>
      tables = tables - table
      t match {
        case tt: Closable => tt.close
      }
      DelDir.d(path + "/" + table)
      this
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
  
  def openTable(th: TableHeader, p: String): STable[_] = th.keyType match{
    case DbType.INT => STable[Int](th, p)
    case DbType.STRING => STable[String](th, p)
    case DbType.BYTE => STable[Byte](th, p)
    case DbType.CHAR => STable[Char](th, p)
    case DbType.SHORT => STable[Short](th, p)
    case DbType.LONG => STable[Long](th, p)
    case DbType.FLOAT => STable[Float](th, p)
    case DbType.DOUBLE => STable[Double](th, p)
    case DbType.DATE => STable[Date](th, p)
    case DbType.BYTES => STable[Iterable[Byte]](th,p)
  }
  def apply(name: String, path: String): Db = new FileDb(name, path)  
}


