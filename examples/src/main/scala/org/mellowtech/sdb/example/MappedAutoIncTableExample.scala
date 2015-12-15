package org.mellowtech.sdb.example

import java.nio.file.Files

import org.mellowtech.sdb._
import org.mellowtech.sdb.model.TOption.AutoInc
import org.mellowtech.sdb.model.{TOption, MappedTable, RowMapper}

/**
  * This example shows how you can use a "model" and a "mapper" to define and work with your table
  * and using auto incremented keys
  *
  * Created by msvens on 13/12/15.
  */
object MappedAutoIncTableExample extends App{


  val p = Files.createTempDirectory("mappedTableAutoInc")

  println("temp directory: "+p)

  implicit var db = Db("mappedTableAutoInc", p.toAbsolutePath.toString)


  //First create your model. The simplest way to do this is to use a case class
  //In this example we will create a model that is keyed on an Int (id) containing one string column (c1)
  //The reason for using Option is to allow for null column values and an auto incremented id
  case class TestModel(id: Option[Int], c1: Option[String])

  //Next we need to create a mapper to and from our Model and SDB's internal structure
  //This will be used in our Table projection function below
  class ModelMapper extends RowMapper[Int, TestModel] {
    def from(a: KeyRow[Int]): TestModel = TestModel(Some(a._1),a._2.get[String]("c1"))
    def list(b: TestModel): (Option[Int],List[(ColumnName,Any)]) = (b.id, List(("c1",b.c1)))
  }

  //Next we define our table
  //If you have used Slick it should look familiar.
  //You need to implement the projection function (*) and define the columns of your table (c1)
  //In order to enable autoIncremented keys we need to add that to our table properties
  class TestTable extends MappedTable[Int, TestModel]("TestTable") {
    //override val tableProperties = super.tableProperties :+ T.AutoInc
    override def tableProperties = List(T.Sorted, T.Type(TableType.ROW), T.AutoInc)
    override val * : RowMapper[Int, TestModel] = new ModelMapper()
    val c1 = column[String]("c1", O.Length(128,true))
  }

  //Now we create an instance of our Table. Observe that our implicittly defined Db will be picked
  //up by that Table as the Db to use
  val t = new TestTable

  for(ch <- t.table.columnHeaders) println(ch)

  t + TestModel(None,Some("one"))
  t + TestModel(None,Some("one"))
  t + TestModel(None,Some("two"))
  t + TestModel(None,Some("one"))

  println("get row with key 4: "+t.get(4))

  val s = t.find("c1", "one")
  println("results of finding rows with c1 value of one: "+s.hasNext)
  println(s mkString "\n")
  db.close

  //now make sure we can reopen the Db:
  db = Db("mappedTable", p.toAbsolutePath.toString)
  val tt = new TestTable
  val ss = tt.find("c1", "one")
  println("\nresults of finding rows in reponed table with c1 value of one: "+ss.hasNext)
  println(ss mkString "\n")

  db.close
  Db.shutdown







}
