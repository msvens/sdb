/**
 *
 */
package org.mellowtech.dbm

/**
 * @author msvens
 *
 */
object MappingExample extends App{
  import Mappable._
  
  case class Person(name: String, age: Int)

  implicit def mapify[T: Mappable](t: T) = implicitly[Mappable[T]].toMap(t)
  implicit def materialize[T: Mappable](map: Map[String, Any]) = implicitly[Mappable[T]].fromMap(map)

  val person = Person("john", 24)
  val pMap: Map[String,Any] = person
  assert {
    pMap == Map("name" -> "john", "age" -> 24)
  }

  val map = Map("name" -> "bob", "age" -> 22)
  val p: Person = materialize[Person](map)
  //
  assert {
    materialize[Person](map) == Person("bob", 22)
    //map == pMap
    //mapP == Person("bob", 22)
  }
  
  

}