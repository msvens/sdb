/**
 *
 */
package org.mellowtech.sdb.example

import org.mellowtech.sdb.Mappable

//import java.io.File

  

/**
 * @author msvens
 *
 */
object SimpleExample extends App{
  import java.io.File
  import org.mellowtech.sdb.model.simple._
  import org.mellowtech.sdb._
  import org.mellowtech.sdb.Mappable
  import scala.reflect._
  
  val dir = new File("/Users/msvens/dbmtestsimple")
  
  if(!dir.exists) dir.mkdir()
  
  implicit val db = Db("sample",dir.getAbsolutePath)
  //implicit val ct = classTag[Int]
  
  
  case class MyRow(id: Option[Int], c2: Option[String])
  
  class TestSimple extends SimpleTable[Int,MyRow]("testTable") {
    val idc = column[Int]("id", O.Optional, O.Primary)
    val c2 = column[String]("c2", O.Length(128,true), O.FieldSearch)
    val * = List(c2)
  }
  
  val t = new TestSimple

  //println(t.headers.mkString("\n"))
  for(ch <- t.table.columnHeaders) println(ch)
  
  t.ins(MyRow(Some(1),Some("one")))
  t.ins(MyRow(Some(2), Some("one")))
  t.ins(MyRow(Some(3), Some("two")))
  
  //val g = t.get(1)

  //println(g.toString)

  val s = t.find("c2", "one")
  
  println(s mkString "\n")
  db.flush
  Db.shutdown
  

}