package org.mellowtech.dbm

object dbm {
  println("Welcome to the Scala worksheet")       //> Welcome to the Scala worksheet
	
	import com.mellowtech.core.collections.mappings._
	import com.mellowtech.core.collections._
	import com.mellowtech.core.collections.tree._
	import com.mellowtech.core.bytestorable._
	import org.json4s._
	import org.json4s.native.Serialization
	import org.json4s.native.Serialization.{read, write}

	import java.util.Date
	import java.nio.ByteBuffer
	
	import DbType.DbType
	
	case class Test(one: Int, two: String)
	
	val l = (1,"hej")                         //> l  : (Int, String) = (1,hej)
	
	val ll = Test.tupled(l)                   //> ll  : org.mellowtech.dbm.dbm.Test = Test(1,hej)

                                                 
}