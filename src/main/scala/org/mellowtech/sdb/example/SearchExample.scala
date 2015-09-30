/**
 *
 */
package org.mellowtech.sdb.example

import java.io.File
import org.mellowtech.sdb._
import scala.util.{Try, Failure,Success}


/**
 * @author msvens
 *
 */
object SearchExample extends App{
  import java.io.File
  import org.mellowtech.sdb.SearchType._
 
  
  
  val dir = new File("/Users/msvens/dbmtest")
  
  if(!dir.exists) dir.mkdir()
  
  val db = Db("searchxample",dir.getAbsolutePath)
  
  val th = TableHeader("table1")
  val ch = ColumnHeader.in(th, "col1").copy(search = FIELD)
  
  db += th
  
  val t: STable[String] = db("table1")
  val st = STable.searchable(t)
  
  st.addColumn(ch)
  
  st insert ("1", ch.name, "HEllo")
  st insert ("2", ch.name, "anna")
  st insert ("3", ch.name, "Hello")
  st insert ("4", ch.name, "pelle")
  
  //st.refresh
  
  //val rq = RowQuery(ch.name, "Hello")
  
  println("continue")
  scala.io.StdIn.readLine
  
  var res = st.queryKeys("col1", "Hello", Seq.empty)
  
  println("result: "+res.size+" "+res.mkString(" "))
  
  st insert ("5", ch.name, "Hello")
  scala.io.StdIn.readLine
  
  res = st.queryKeys("col1", "Hello", Seq.empty)
  
  println("result: "+res.size+" "+res.mkString(" "))
  
  st.close match {
    case Success(s) => println("flushed table successfully")
    case Failure(e) => println(e)
  }
  
  db -= t.header.name
  db.close
  Db.shutdown

}