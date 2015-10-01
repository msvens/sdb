package org.mellowtech.sdb.example

import java.io.File
import org.mellowtech.sdb._
import org.mellowtech.sdb.model.{MappedTable,RowMapper}
//import org.mellowtech.sdb.model.simple._

object MappedTableExample extends App{
  
  val dir = new File("/Users/msvens/dbmtestmapped")
  
  if(!dir.exists) dir.mkdir()
  
  implicit val db = Db("sample",dir.getAbsolutePath)
  
  case class TestModel(id: Option[Int], c2: Option[String])
  
  class TestModelMapper extends RowMapper[Int, TestModel] {
    def from(a: KeyRow[Int]): TestModel = TestModel(Some(a._1),a._2.get[String]("c2"))
    def list(b: TestModel): (Option[Int],List[(ColumnName,Any)]) = (b.id, List(("c2",b.c2)))
  }
  
  class TestTable extends MappedTable[Int, TestModel]("TestTable") {
    
    override val * : RowMapper[Int, TestModel] = new TestModelMapper()

    val c2 = column[String]("c2", O.Length(128,true))
    
    
    
  }
  
  val t = new TestTable

  //println(t.headers.mkString("\n"))
  for(ch <- t.table.columnHeaders) println(ch)
  
  t + (TestModel(Some(1),Some("one")))
  t + (TestModel(Some(2), Some("one")))
  t + (TestModel(Some(3), Some("two")))
  t + (TestModel(Some(4), Some("one")))
  
  
  println("put a char")
  System.in.read()
  //val g = t.get(1)

  //println(g.toString)
  println(t.get(4))
  val s = t.find("c2", "one")
  println("any results: "+s.hasNext)
  println(s mkString "\n")
  db.close
  Db.shutdown
  
}