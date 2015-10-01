package org.mellowtech.sdb.example

import java.io.File
import org.mellowtech.sdb._
import org.mellowtech.sdb.model.{MappedTable,AutoMappedTable}
import org.mellowtech.sdb.model.simple._
//import org.mellowtech.sdb.model.simple._

object AutoMappedTableExample extends App{
  
  val dir = new File("/Users/msvens/dbmtestautomapped")
  
  if(!dir.exists) dir.mkdir()
  
  implicit val db = Db("sample",dir.getAbsolutePath)
  
  case class TestModel(id: Int, c2: Option[String])
  
  class TestTable extends AutoMappedTable[Int, TestModel]("TestTable", "id") {
    
    val c2 = column[String]("c2", O.Length(128,true), O.Optional)
    
  }
  
  val t = new TestTable

  for(ch <- t.table.columnHeaders) println(ch)
  
  t + (TestModel(1,Some("one")))
  t + (TestModel(2, Some("one")))
  t + (TestModel(3, Some("two")))
  t + (TestModel(4, Some("one")))
  
  
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