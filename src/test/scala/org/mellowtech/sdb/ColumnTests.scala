package org.mellowtech.sdb


import org.scalatest._
import java.io.File

object Files {
  import org.mellowtech.core.util.Platform
  import org.mellowtech.core.util.DelDir
  import java.net.URL
  
  def temp(dir: String): String = new File(Platform.getTempDir+"/"+dir).getAbsolutePath
  
  def fileTemp(dir: Option[String], f: String) = {
    val tdir = dir match {
      case Some(d) => temp(d)
      case None => temp("")
    }
    file(Some(tdir), f)
  }
  
  def file(dir: Option[String], f: String): File = dir match{
    case Some(d) => new File(d + "/" + f)
    case None => new File(f)
  }
  
  def file(f: String): File = file(null, f)
  
  def del(dir: String):Boolean = DelDir.d(dir)
  def delTemp(dir: String):Boolean=DelDir.d(temp(dir))
  
  
  def create(dir: String): Boolean = {
    new File(dir).mkdir()
  } 
  def createTemp(dir: String): Boolean = create(temp(dir))

  def resource(r: String): File = {
    val url = Files.getClass.getResource("/"+r)
    new File(url.getFile)
  }
  
  def resource(pkg: String, r: String): File = {
    if(pkg == null){
      resource(r)
    }
    else{
      pkg.replace(".", "/") match {
        case x if x.startsWith("/") => resource(x+"/"+r)
        case x => resource("/"+x+"/"+r)
      }
    }
  }
}

trait Columns extends SuiteMixin { this: Suite =>
  import scala.util.Random
  import scala.collection.mutable.ArrayBuffer
  import scala.collection.mutable.HashMap
  
  val ch = ColumnHeader("col1", "table1")
  val dir = "dbmtest"
  val f = "discBasedMap";
  val cols: ArrayBuffer[Column[String,String]] = ArrayBuffer()
  
  abstract override def withFixture(test: NoArgTest) = {
    cols += Column[String,String](ch)
    val tdir = Files.temp(dir+""+Random.alphanumeric.take(10).mkString(""))
    Files.create(tdir)
    cols += Column[String,String](ch,tdir+"/"+f)
    try super.withFixture(test) // To be stackable, must call super.withFixture
    finally {
      cols.clear
      Files.del(tdir)
    }
  }
  
}

class ColumnSpec extends FlatSpec with Columns{
  
  behavior of "an empty column"
  
  it should "have size zero" in {
    cols.foreach(f => assert(f.size === 0))
  }
  
  it should "return none when getting a value" in {
    cols.foreach(f => assert(f.get("someKey") === None))
  }
  
  it should "throw an exception when calling apply" in {
    cols.foreach(f => intercept[NoSuchElementException] (f("someKey")))
  }
  
  behavior of "adding to column"
  
  it should "contain one more than the previous column" in {
    cols.foreach{f =>
      val psize = f.size
      val f1 = f + ("key1","val1")
      assert(psize === (f1.size - 1))
    }
  }
  
  it should "contain an added value" in {
    cols.foreach{c =>
      val c1 = c + ("key2", "val2")
      assert(c1.get("key2") === Some("val2"))
      assert(c1("key2") === "val2")
    }
  }
  
  it should "return an iterator with the added value" in {
    cols.foreach{c =>
      val c1 = c + ("key3", "val3")
      val value = for {
        (k,v) <- c1.toIterator
        if(v.equals("val3"))
      } yield v
      assert(value.toList.head === "val3")
    }
  }
  
  behavior of "deleting from a column"
  
  it should "contain one less item than the previous column" in {
    cols.foreach{c =>
      val c1 = c + ("key1", "val1")
      val s1 = c1.size
      val c2 = c1 - "key1"
      assert(s1 === c2.size + 1)
    }
  }
  
  it should "not contain a previously removed key" in {
    cols.foreach{c =>
      val c1 = c + ("key1", "val1")
      val c2 = c1 - "key1"
      assert(c2.get("key1") === None)
    }
  }

}