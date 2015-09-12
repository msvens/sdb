package org.mellowtech.sdb


import org.scalatest._
import java.io.File

trait TableBuffer extends SuiteMixin { this: Suite =>
  import scala.util.Random
  import scala.collection.mutable.ArrayBuffer

  val dbuffer = new ArrayBuffer[Table[Int]]
  val mbuffer = new ArrayBuffer[Table[Int]]
  val all = new ArrayBuffer[Table[Int]]
  
  val rhead = TableHeader("rowtable", DbType.INT, tableType=TableType.ROW, primColumn=Some("col1"))
  val chead = TableHeader("coltable", DbType.INT, tableType=TableType.COLUMN, primColumn=Some("col1"))
  val mrhead = TableHeader("memrowtable", DbType.INT, tableType=TableType.MEMORY_COLUMN, primColumn=Some("col1"))
  val mchead = TableHeader("memcoltable", DbType.INT, tableType=TableType.MEMORY_ROW, primColumn=Some("col1"))
  val rc = List(ColumnHeader("col1", "rowtable"),ColumnHeader("col2", "rowtable", valueType = DbType.INT))
  val cc = List(ColumnHeader("col1", "coltable"),ColumnHeader("col2", "coltable", valueType = DbType.INT))
  val mrc = List(ColumnHeader("col1", "memrowtable"),ColumnHeader("col2", "memrowtable", valueType = DbType.INT))
  val mcc = List(ColumnHeader("col1", "memcoltable"),ColumnHeader("col2", "memcoltable", valueType = DbType.INT))
  
  val dir = "dbmtest"
 
  val cpath = "coltable"
  val rpath = "rowtable"
  
  val cdir = Files.temp(dir+""+Random.alphanumeric.take(10).mkString(""))
  val rdir = Files.temp(dir+""+Random.alphanumeric.take(10).mkString(""))
  
  abstract override def withFixture(test: NoArgTest) = {
    Files.create(cdir)
    Files.create(rdir)
    dbuffer += Table(rhead,rc,rdir) += Table(chead,cc,cdir)
    mbuffer += Table(mrhead,mrc) += Table(mchead,mcc)
    all ++= dbuffer ++= mbuffer
    try super.withFixture(test) // To be stackable, must call super.withFixture
    finally {
      dbuffer.foreach(_.flush)
      Files.del(cdir)
      Files.del(rdir)
      dbuffer.clear
    }
  }
}

class TableSpec extends FlatSpec with TableBuffer {
  val row1 = Row(1, Map("col1" -> "val1", "col2" -> 1000))
  val row2 = Row(2, Map("col1" -> "val2", "col2" -> 2000))
  
  def addValues = for{
    t <- all
    tt = t += row1 += row2
  }yield(tt)
  
  behavior of "a non empty table"
  
  it should "have retrievable rows" in {
    addValues.foreach{t => {
      assert(t.row(1) === row1)
      assert(t.row(2) === row2)
    }}
  }
  
  it should "return col iterators" in {
    
  }
  
}

class DiscTableSpec extends FlatSpec with TableBuffer{
  
  val row1 = Row(1, Map("col1" -> "val1", "col2" -> 2000))
  val row2 = Row(2, Map("col1" -> "val2", "col2" -> 2000))
  
  def addValues: Unit = dbuffer.foreach(_ += row1 += row2)
  
  behavior of "an empty table"
  
  it should "have size zero" in {
    dbuffer.foreach(t => assert(t.size === 0))
  }
  
  it should "return an empty row for any key" in {
    dbuffer.foreach(t => assert(t.row(10).size === 0))
  }
  
  it should "return an empty row iterator" in {
    dbuffer.foreach(t => assert(t.rows.hasNext === false))
  }
  
  behavior of "flushing a disc based table"
  
  it should "be able to be reopened" in {
    addValues
    //dbuffer.foreach(t => assert(t.size === 2))
    dbuffer.foreach(_.close)
    val t1 = Table[Int](rhead, rdir)
    val t2 = Table[Int](chead, cdir)
    assert(t1.column("col2").size === 2)
    assert(t2.column("col2").size === 2)
  }
  
  it should "save the correct data to disc" in {
    addValues
    dbuffer.foreach(_.flush)
    val t1 = Table[Int](rhead, rdir)
    val t2 = Table[Int](chead, cdir)
    val r1 = t1.row(1)
    
    val r2 = t2.row(2)
    assert(r1[Int]("col2") === 2000)
    assert(r2[Int]("col2") === 2000)
  }
  


}