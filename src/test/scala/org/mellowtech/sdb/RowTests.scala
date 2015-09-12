package org.mellowtech.sdb


import org.scalatest._
import java.io.File

trait Rows extends SuiteMixin { this: Suite =>
  import scala.util.Random
  import scala.collection.mutable.ArrayBuffer

  var sparseRow: Row[String] = null
  val th = TableHeader("table1", DbType.STRING, sorted = false)
  
  abstract override def withFixture(test: NoArgTest) = {
    sparseRow = new SparseRow("row", Map.empty)
    try super.withFixture(test) // To be stackable, must call super.withFixture
    finally {
      sparseRow = null
    }
  }
  
}

class RowSpec extends FlatSpec with Rows{
  
  behavior of "an empty row"
  
  it should "have size zero" in {
    assert(sparseRow.size === 0)
  }
  
  it should "return none when getting a column" in {
    assert(sparseRow.get("someCol") === None)

  }
  
  it should "throw an exception when calling apply" in {
    intercept[NoSuchElementException](sparseRow("someCol"))
  }
  
  behavior of "adding to row"
  
  it should "contain one more than the previous row" in {
    val psize = sparseRow.size
    val r1 = sparseRow + (("col1"), 10)
    assert(psize === r1.size - 1)
  }
  
  it should "contain an added column" in {
    val r1 = sparseRow + (("col1"), 10)
    assert(r1.get[Int]("col1") === Some(10))
    assert(r1[Int]("col1") === 10)
  }
  
  it should "return an iterator with the added column" in {
    val r1 = sparseRow + (("col1"), 10)
    val value = for {
      (c,v) <- r1.iterator
      if(v.equals(10))
    } yield v
    assert(value.toList.head === 10)
  }
  
  behavior of "deleting from a row"
  
  it should "contain one less item than the previous row" in {
    val r1 = sparseRow + ("col1", 10)
    val s1 = r1.size
     val r2 = r1 - "col1"
     assert(s1 === r2.size + 1)
    
  }
  
  it should "not contain a previously removed column" in {
    val r1 = sparseRow + ("col1", 10)
    val r2 = r1 - "col1"
    assert(r2.get("col1") === None)
  }
  
  behavior of "multiple column row"
  
  it should "return the correct class of column values" in {
    val r1 = sparseRow + ("col1", 10) + ("col2", "ten")
    val c1:Any = r1("col1")
    val c2:Any = r1("col2")
    assert(c1.isInstanceOf[Int])
    assert(c2.isInstanceOf[String])
  }
  
  behavior of "storing a row as bytes"
  
  it should "be able to recreate the row" in {
    val r1 = sparseRow + ("col1", 10) + ("col2", "ten")
    val b = Row.toBytes(r1)
    val r2 = Row(r1.key,th,b)
    assert(r1[Int]("col1") === r2[Int]("col1"))
    assert(r1[String]("col2") === r2[String]("col2"))
  }

}