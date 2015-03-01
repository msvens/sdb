/**
 *
 */
package org.mellowtech.dbm.example

import java.io.File

  

/**
 * @author msvens
 *
 */
object SimpleExample extends App{
  import java.io.File
  import org.mellowtech.dbm.model.simple._
  import org.mellowtech.dbm._
  
  val dir = new File("/Users/msvens/dbmtestsimple")
  
  if(!dir.exists) dir.mkdir()
  
  implicit val db = Db("xample",dir.getAbsolutePath)
  
  
  case class MyRow(id: Option[Int], c2: Option[String])
  
  class TestSimple extends SimpleTable[Int,MyRow]("testTable") {
    val idc = column[Int]("id", O.Nullable)
    def c2 = column[String]("c2", O.Length(128,true), O.FieldSearch)
    val * = List(c2)
  }
  
  val t = new TestSimple
  println(t.headers.mkString("\n"))

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