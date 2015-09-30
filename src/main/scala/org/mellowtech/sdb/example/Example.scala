/**
 *
 */
package org.mellowtech.sdb

/**
 * @author msvens
 *
 */
object Example extends App{
  import java.io.File
  
  val dir = new File("/Users/msvens/dbmtest")
  
  if(!dir.exists) dir.mkdir()
  
  val db = Db("xample",dir.getAbsolutePath)
  
  val th = TableHeader("table1")
  val ch = ColumnHeader.in(th, "col1")
  
  val th1 = TableHeader("table2", tableType = TableType.COLUMN, primColumn = Some("col1"))
  val ch1 = ch.copy(table = "table2")
  
  
  db += th
  
  val t1: STable[String] = db("table1")
  
  t1.addColumn(ch)
  
  t1 insert ("two", ch.name, "two")
  t1 insert ("one", ch.name, "one")
  
  val c = t1.column[String]("col1")
  
  c.iterator.foreach(x => println(x._1,x._2))
  
  println(t1.size)
  
  db += th1
  val t2: STable[String] = db("table2")
  t2.addColumn(ch1)
  t2 insert ("two", ch1.name, "two")
  t2 insert ("one", ch1.name, "one")
  
  val c2 = t2.column[String]("col1")
  
  c2.iterator.foreach(x => println(x._1,x._2))
  
  println(t2.size)
  
  db.flush
  
  //System.exit(0)

}