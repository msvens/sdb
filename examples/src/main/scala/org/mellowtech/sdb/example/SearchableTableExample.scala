package org.mellowtech.sdb.example

import java.nio.file.Files

import org.mellowtech.sdb._

import scala.util.{Failure, Success}

/**
  * Created by msvens on 12/12/15.
  */
object SearchableTableExample extends App{

  val p = Files.createTempDirectory("searchtable")

  println("temp directory: "+p)

  val db = Db("searchableTable", p.toAbsolutePath.toString)


  val th = TableHeader("table1", indexed = true)
  val ch = ColumnHeader.in(th, "col1").copy(search = SearchType.FIELD)
  val ch1 = ColumnHeader.in(th, "col2").copy(search = SearchType.TEXT)

  val t: IndexingTable[String] = (db += th)("table1").asInstanceOf[IndexingTable[String]]
  println(t.getClass.getName)

  //add two columns and insert some data
  t.addColumn(ch)
  t.addColumn(ch1)
  t + (Some("1"), SRow(Map(ch.name -> "hello", ch1.name -> "Hello World")))
  t + (Some("2"), SRow(Map(ch.name -> "anna", ch1.name -> "hello all of you")))
  t + (Some("3"), SRow(Map(ch.name -> "hello", ch1.name -> "HELLO")))
  t + (Some("4"), SRow(Map(ch.name -> "pelle", ch1.name -> "do not return")))

  //now search for the key hello
  println(t.rowIterator.mkString("\n"))
  println("press any key to continue")
  scala.io.StdIn.readLine

  var res = t.queryKeys("col1", "hello", Seq.empty)
  println("result: "+res.mkString(" "))

  //now try to search in in the second column that is declared as a text field
  //in which case the column value is tokenized and search is for instance case insensitive
  res = t.queryKeys(ch1.name, "HELLO", Seq.empty);
  println("result: "+res.mkString(" "))

  //Flush table and remove it from db (which effectively deletes it)
  t.flush match {
    case Success(s) => println("flushed table successfully")
    case Failure(e) => println(e)
  }
  db -= t.header.name

  //close database
  db.close
  Db.shutdown

}
