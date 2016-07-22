package org.mellowtech.sdb

import java.nio.ByteBuffer
import org.mellowtech.core.bytestorable._
import org.mellowtech.core.collections.{DiscMapBuilder, DiscMap}
import org.mellowtech.sdb.DbType.DbType


object SDiscMapBuilder{
  def apply(): SDiscMapBuilder = new SDiscMapBuilder
}

class SDiscMapBuilder extends DiscMapBuilder {

  /*override def build[A,B](keyClass: Class[A], valueClass: Class[B],
                          fileName: String, sorted: Boolean): DiscMap[A,B] = {
    super[A,B](keyClass, valueClass, fileName, sorted)

  }*/

  import scala.reflect._
  import java.util.Date

  private def toClass[T: ClassTag]: Class[T] = classTag[T].runtimeClass.asInstanceOf[Class[T]]

  private def toClass[T](dbType: DbType): Class[T] = DbType.asScalaClass(dbType).asInstanceOf[Class[T]]

  val BooleanClass = classOf[Boolean]
  val ByteClass = classOf[Byte]
  val ShortClass = classOf[Short]
  val IntClass = classOf[Int]
  val LongClass = classOf[Long]
  val FloatClass = classOf[Float]
  val DoubleClass = classOf[Double]
  val CharClass = classOf[Char]
  val StringClass = classOf[String]
  val ByteArrayClass = classOf[Array[Byte]]
  val DateClass = classOf[Date]

  def build[A: ClassTag, B: ClassTag](fileName: String, sorted: Boolean): DiscMap[A, B] = {
    build[A, B](toClass[A], toClass[B], fileName, sorted)
  }

  def build[A, B](keyType: DbType, valueType: DbType, fileName: String, sorted: Boolean): DiscMap[A, B] = {
    build[A, B](toClass[A](keyType), toClass[B](valueType), fileName, sorted)
  }

  override def build[A, B](keyClass: Class[A], valueClass: Class[B],
                           fileName: ColumnName, sorted: Boolean): DiscMap[A, B] = {
    //val ByteClass = class


    val m = (keyClass, valueClass) match {
      //Boolean Keys
      case (BooleanClass, BooleanClass) => create[Boolean, Boolean, SBoolean, SBoolean](classOf[SBoolean], classOf[SBoolean], fileName, sorted)
      case (BooleanClass, ByteClass) => create[Boolean, Byte, SBoolean, SByte](classOf[SBoolean], classOf[SByte], fileName, sorted)
      case (BooleanClass, ShortClass) => create[Boolean, Short, SBoolean, SShort](classOf[SBoolean], classOf[SShort], fileName, sorted)
      case (BooleanClass, IntClass) => create[Boolean, Int, SBoolean, SInt](classOf[SBoolean], classOf[SInt], fileName, sorted)
      case (BooleanClass, LongClass) => create[Boolean, Long, SBoolean, SLong](classOf[SBoolean], classOf[SLong], fileName, sorted)
      case (BooleanClass, FloatClass) => create[Boolean, Float, SBoolean, SFloat](classOf[SBoolean], classOf[SFloat], fileName, sorted)
      case (BooleanClass, DoubleClass) => create[Boolean, Double, SBoolean, SDouble](classOf[SBoolean], classOf[SDouble], fileName, sorted)
      case (BooleanClass, StringClass) => create[Boolean, String, SBoolean, CBString](classOf[SBoolean], classOf[CBString], fileName, sorted)
      case (BooleanClass, CharClass) => create[Boolean, Char, SBoolean, SChar](classOf[SBoolean], classOf[SChar], fileName, sorted)
      case (BooleanClass, ByteArrayClass) => create[Boolean, Array[Byte], SBoolean, CBByteArray](classOf[SBoolean], classOf[CBByteArray], fileName, sorted)
      case (BooleanClass, DateClass) => create[Boolean, Date, SBoolean, CBDate](classOf[SBoolean], classOf[CBDate], fileName, sorted)
      //Byte Keys
      case (ByteClass, BooleanClass) => create[Byte, Boolean, SByte, SBoolean](classOf[SByte], classOf[SBoolean], fileName, sorted)
      case (ByteClass, ByteClass) => create[Byte, Byte, SByte, SByte](classOf[SByte], classOf[SByte], fileName, sorted)
      case (ByteClass, ShortClass) => create[Byte, Short, SByte, SShort](classOf[SByte], classOf[SShort], fileName, sorted)
      case (ByteClass, IntClass) => create[Byte, Int, SByte, SInt](classOf[SByte], classOf[SInt], fileName, sorted)
      case (ByteClass, LongClass) => create[Byte, Long, SByte, SLong](classOf[SByte], classOf[SLong], fileName, sorted)
      case (ByteClass, FloatClass) => create[Byte, Float, SByte, SFloat](classOf[SByte], classOf[SFloat], fileName, sorted)
      case (ByteClass, DoubleClass) => create[Byte, Double, SByte, SDouble](classOf[SByte], classOf[SDouble], fileName, sorted)
      case (ByteClass, StringClass) => create[Byte, String, SByte, CBString](classOf[SByte], classOf[CBString], fileName, sorted)
      case (ByteClass, CharClass) => create[Byte, Char, SByte, SChar](classOf[SByte], classOf[SChar], fileName, sorted)
      case (ByteClass, ByteArrayClass) => create[Byte, Array[Byte], SByte, CBByteArray](classOf[SByte], classOf[CBByteArray], fileName, sorted)
      case (ByteClass, DateClass) => create[Byte, Date, SByte, CBDate](classOf[SByte], classOf[CBDate], fileName, sorted)
      //Short Keys
      case (ShortClass, BooleanClass) => create[Short, Boolean, SShort, SBoolean](classOf[SShort], classOf[SBoolean], fileName, sorted)
      case (ShortClass, ByteClass) => create[Short, Byte, SShort, SByte](classOf[SShort], classOf[SByte], fileName, sorted)
      case (ShortClass, ShortClass) => create[Short, Short, SShort, SShort](classOf[SShort], classOf[SShort], fileName, sorted)
      case (ShortClass, IntClass) => create[Short, Int, SShort, SInt](classOf[SShort], classOf[SInt], fileName, sorted)
      case (ShortClass, LongClass) => create[Short, Long, SShort, SLong](classOf[SShort], classOf[SLong], fileName, sorted)
      case (ShortClass, FloatClass) => create[Short, Float, SShort, SFloat](classOf[SShort], classOf[SFloat], fileName, sorted)
      case (ShortClass, DoubleClass) => create[Short, Double, SShort, SDouble](classOf[SShort], classOf[SDouble], fileName, sorted)
      case (ShortClass, StringClass) => create[Short, String, SShort, CBString](classOf[SShort], classOf[CBString], fileName, sorted)
      case (ShortClass, CharClass) => create[Short, Char, SShort, SChar](classOf[SShort], classOf[SChar], fileName, sorted)
      case (ShortClass, ByteArrayClass) => create[Short, Array[Byte], SShort, CBByteArray](classOf[SShort], classOf[CBByteArray], fileName, sorted)
      case (ShortClass, DateClass) => create[Short, Date, SShort, CBDate](classOf[SShort], classOf[CBDate], fileName, sorted)
      //Int Keys
      case (IntClass, BooleanClass) => create[Int, Boolean, SInt, SBoolean](classOf[SInt], classOf[SBoolean], fileName, sorted)
      case (IntClass, ByteClass) => create[Int, Byte, SInt, SByte](classOf[SInt], classOf[SByte], fileName, sorted)
      case (IntClass, ShortClass) => create[Int, Short, SInt, SShort](classOf[SInt], classOf[SShort], fileName, sorted)
      case (IntClass, IntClass) => create[Int, Int, SInt, SInt](classOf[SInt], classOf[SInt], fileName, sorted)
      case (IntClass, LongClass) => create[Int, Long, SInt, SLong](classOf[SInt], classOf[SLong], fileName, sorted)
      case (IntClass, FloatClass) => create[Int, Float, SInt, SFloat](classOf[SInt], classOf[SFloat], fileName, sorted)
      case (IntClass, DoubleClass) => create[Int, Double, SInt, SDouble](classOf[SInt], classOf[SDouble], fileName, sorted)
      case (IntClass, StringClass) => create[Int, String, SInt, CBString](classOf[SInt], classOf[CBString], fileName, sorted)
      case (IntClass, CharClass) => create[Int, Char, SInt, SChar](classOf[SInt], classOf[SChar], fileName, sorted)
      case (IntClass, ByteArrayClass) => create[Int, Array[Byte], SInt, CBByteArray](classOf[SInt], classOf[CBByteArray], fileName, sorted)
      case (IntClass, DateClass) => create[Int, Date, SInt, CBDate](classOf[SInt], classOf[CBDate], fileName, sorted)
      //Long Keys
      case (LongClass, BooleanClass) => create[Long, Boolean, SLong, SBoolean](classOf[SLong], classOf[SBoolean], fileName, sorted)
      case (LongClass, ByteClass) => create[Long, Byte, SLong, SByte](classOf[SLong], classOf[SByte], fileName, sorted)
      case (LongClass, ShortClass) => create[Long, Short, SLong, SShort](classOf[SLong], classOf[SShort], fileName, sorted)
      case (LongClass, IntClass) => create[Long, Int, SLong, SInt](classOf[SLong], classOf[SInt], fileName, sorted)
      case (LongClass, LongClass) => create[Long, Long, SLong, SLong](classOf[SLong], classOf[SLong], fileName, sorted)
      case (LongClass, FloatClass) => create[Long, Float, SLong, SFloat](classOf[SLong], classOf[SFloat], fileName, sorted)
      case (LongClass, DoubleClass) => create[Long, Double, SLong, SDouble](classOf[SLong], classOf[SDouble], fileName, sorted)
      case (LongClass, StringClass) => create[Long, String, SLong, CBString](classOf[SLong], classOf[CBString], fileName, sorted)
      case (LongClass, CharClass) => create[Long, Char, SLong, SChar](classOf[SLong], classOf[SChar], fileName, sorted)
      case (LongClass, ByteArrayClass) => create[Long, Array[Byte], SLong, CBByteArray](classOf[SLong], classOf[CBByteArray], fileName, sorted)
      case (LongClass, DateClass) => create[Long, Date, SLong, CBDate](classOf[SLong], classOf[CBDate], fileName, sorted)
      //Float Key
      case (FloatClass, BooleanClass) => create[Float, Boolean, SFloat, SBoolean](classOf[SFloat], classOf[SBoolean], fileName, sorted)
      case (FloatClass, ByteClass) => create[Float, Byte, SFloat, SByte](classOf[SFloat], classOf[SByte], fileName, sorted)
      case (FloatClass, ShortClass) => create[Float, Short, SFloat, SShort](classOf[SFloat], classOf[SShort], fileName, sorted)
      case (FloatClass, IntClass) => create[Float, Int, SFloat, SInt](classOf[SFloat], classOf[SInt], fileName, sorted)
      case (FloatClass, LongClass) => create[Float, Long, SFloat, SLong](classOf[SFloat], classOf[SLong], fileName, sorted)
      case (FloatClass, FloatClass) => create[Float, Float, SFloat, SFloat](classOf[SFloat], classOf[SFloat], fileName, sorted)
      case (FloatClass, DoubleClass) => create[Float, Double, SFloat, SDouble](classOf[SFloat], classOf[SDouble], fileName, sorted)
      case (FloatClass, StringClass) => create[Float, String, SFloat, CBString](classOf[SFloat], classOf[CBString], fileName, sorted)
      case (FloatClass, CharClass) => create[Float, Char, SFloat, SChar](classOf[SFloat], classOf[SChar], fileName, sorted)
      case (FloatClass, ByteArrayClass) => create[Float, Array[Byte], SFloat, CBByteArray](classOf[SFloat], classOf[CBByteArray], fileName, sorted)
      case (FloatClass, DateClass) => create[Float, Date, SFloat, CBDate](classOf[SFloat], classOf[CBDate], fileName, sorted)
      case (DoubleClass, BooleanClass) => create[Float, Boolean, SFloat, SBoolean](classOf[SFloat], classOf[SBoolean], fileName, sorted)
      //Double Key
      case (DoubleClass, ByteClass) => create[Double, Byte, SDouble, SByte](classOf[SDouble], classOf[SByte], fileName, sorted)
      case (DoubleClass, ShortClass) => create[Double, Short, SDouble, SShort](classOf[SDouble], classOf[SShort], fileName, sorted)
      case (DoubleClass, IntClass) => create[Double, Int, SDouble, SInt](classOf[SDouble], classOf[SInt], fileName, sorted)
      case (DoubleClass, LongClass) => create[Double, Long, SDouble, SLong](classOf[SDouble], classOf[SLong], fileName, sorted)
      case (DoubleClass, FloatClass) => create[Double, Float, SDouble, SFloat](classOf[SDouble], classOf[SFloat], fileName, sorted)
      case (DoubleClass, DoubleClass) => create[Double, Double, SDouble, SDouble](classOf[SDouble], classOf[SDouble], fileName, sorted)
      case (DoubleClass, StringClass) => create[Double, String, SDouble, CBString](classOf[SDouble], classOf[CBString], fileName, sorted)
      case (DoubleClass, CharClass) => create[Double, Char, SDouble, SChar](classOf[SDouble], classOf[SChar], fileName, sorted)
      case (DoubleClass, ByteArrayClass) => create[Double, Array[Byte], SDouble, CBByteArray](classOf[SDouble], classOf[CBByteArray], fileName, sorted)
      case (DoubleClass, DateClass) => create[Double, Date, SDouble, CBDate](classOf[SDouble], classOf[CBDate], fileName, sorted)
      //String Key
      case (StringClass, BooleanClass) => create[String, Boolean, CBString, SBoolean](classOf[CBString], classOf[SBoolean], fileName, sorted)
      case (StringClass, ByteClass) => create[String, Byte, CBString, SByte](classOf[CBString], classOf[SByte], fileName, sorted)
      case (StringClass, ShortClass) => create[String, Short, CBString, SShort](classOf[CBString], classOf[SShort], fileName, sorted)
      case (StringClass, IntClass) => create[String, Int, CBString, SInt](classOf[CBString], classOf[SInt], fileName, sorted)
      case (StringClass, LongClass) => create[String, Long, CBString, SLong](classOf[CBString], classOf[SLong], fileName, sorted)
      case (StringClass, FloatClass) => create[String, Float, CBString, SFloat](classOf[CBString], classOf[SFloat], fileName, sorted)
      case (StringClass, DoubleClass) => create[String, Double, CBString, SDouble](classOf[CBString], classOf[SDouble], fileName, sorted)
      case (StringClass, StringClass) => create[String, String, CBString, CBString](classOf[CBString], classOf[CBString], fileName, sorted)
      case (StringClass, CharClass) => create[String, Char, CBString, SChar](classOf[CBString], classOf[SChar], fileName, sorted)
      case (StringClass, ByteArrayClass) => create[String, Array[Byte], CBString, CBByteArray](classOf[CBString], classOf[CBByteArray], fileName, sorted)
      case (StringClass, DateClass) => create[String, Date, CBString, CBDate](classOf[CBString], classOf[CBDate], fileName, sorted)
      //Char class
      case (CharClass, BooleanClass) => create[Char, Boolean, SChar, SBoolean](classOf[SChar], classOf[SBoolean], fileName, sorted)
      case (CharClass, ByteClass) => create[Char, Byte, SChar, SByte](classOf[SChar], classOf[SByte], fileName, sorted)
      case (CharClass, ShortClass) => create[Char, Short, SChar, SShort](classOf[SChar], classOf[SShort], fileName, sorted)
      case (CharClass, IntClass) => create[Char, Int, SChar, SInt](classOf[SChar], classOf[SInt], fileName, sorted)
      case (CharClass, LongClass) => create[Char, Long, SChar, SLong](classOf[SChar], classOf[SLong], fileName, sorted)
      case (CharClass, FloatClass) => create[Char, Float, SChar, SFloat](classOf[SChar], classOf[SFloat], fileName, sorted)
      case (CharClass, DoubleClass) => create[Char, Double, SChar, SDouble](classOf[SChar], classOf[SDouble], fileName, sorted)
      case (CharClass, StringClass) => create[Char, String, SChar, CBString](classOf[SChar], classOf[CBString], fileName, sorted)
      case (CharClass, CharClass) => create[Char, Char, SChar, SChar](classOf[SChar], classOf[SChar], fileName, sorted)
      case (CharClass, ByteArrayClass) => create[Char, Array[Byte], SChar, CBByteArray](classOf[SChar], classOf[CBByteArray], fileName, sorted)
      case (CharClass, DateClass) => create[Char, Date, SChar, CBDate](classOf[SChar], classOf[CBDate], fileName, sorted)
      //Byte Array Key
      case (ByteArrayClass, BooleanClass) => create[Array[Byte], Boolean, CBByteArray, SBoolean](classOf[CBByteArray], classOf[SBoolean], fileName, sorted)
      case (ByteArrayClass, ByteClass) => create[Array[Byte], Byte, CBByteArray, SByte](classOf[CBByteArray], classOf[SByte], fileName, sorted)
      case (ByteArrayClass, ShortClass) => create[Array[Byte], Short, CBByteArray, SShort](classOf[CBByteArray], classOf[SShort], fileName, sorted)
      case (ByteArrayClass, IntClass) => create[Array[Byte], Int, CBByteArray, SInt](classOf[CBByteArray], classOf[SInt], fileName, sorted)
      case (ByteArrayClass, LongClass) => create[Array[Byte], Long, CBByteArray, SLong](classOf[CBByteArray], classOf[SLong], fileName, sorted)
      case (ByteArrayClass, FloatClass) => create[Array[Byte], Float, CBByteArray, SFloat](classOf[CBByteArray], classOf[SFloat], fileName, sorted)
      case (ByteArrayClass, DoubleClass) => create[Array[Byte], Double, CBByteArray, SDouble](classOf[CBByteArray], classOf[SDouble], fileName, sorted)
      case (ByteArrayClass, StringClass) => create[Array[Byte], String, CBByteArray, CBString](classOf[CBByteArray], classOf[CBString], fileName, sorted)
      case (ByteArrayClass, CharClass) => create[Array[Byte], Char, CBByteArray, SChar](classOf[CBByteArray], classOf[SChar], fileName, sorted)
      case (ByteArrayClass, ByteArrayClass) => create[Array[Byte], Array[Byte], CBByteArray, CBByteArray](classOf[CBByteArray], classOf[CBByteArray], fileName, sorted)
      case (ByteArrayClass, DateClass) => create[Array[Byte], Date, CBByteArray, CBDate](classOf[CBByteArray], classOf[CBDate], fileName, sorted)
      //Date Key
      case (DateClass, BooleanClass) => create[Date, Boolean, CBDate, SBoolean](classOf[CBDate], classOf[SBoolean], fileName, sorted)
      case (DateClass, ByteClass) => create[Date, Byte, CBDate, SByte](classOf[CBDate], classOf[SByte], fileName, sorted)
      case (DateClass, ShortClass) => create[Date, Short, CBDate, SShort](classOf[CBDate], classOf[SShort], fileName, sorted)
      case (DateClass, IntClass) => create[Date, Int, CBDate, SInt](classOf[CBDate], classOf[SInt], fileName, sorted)
      case (DateClass, LongClass) => create[Date, Long, CBDate, SLong](classOf[CBDate], classOf[SLong], fileName, sorted)
      case (DateClass, FloatClass) => create[Date, Float, CBDate, SFloat](classOf[CBDate], classOf[SFloat], fileName, sorted)
      case (DateClass, DoubleClass) => create[Date, Double, CBDate, SDouble](classOf[CBDate], classOf[SDouble], fileName, sorted)
      case (DateClass, StringClass) => create[Date, String, CBDate, CBString](classOf[CBDate], classOf[CBString], fileName, sorted)
      case (DateClass, CharClass) => create[Date, Char, CBDate, SChar](classOf[CBDate], classOf[SChar], fileName, sorted)
      case (DateClass, ByteArrayClass) => create[Date, Array[Byte], CBDate, CBByteArray](classOf[CBDate], classOf[CBByteArray], fileName, sorted)
      case (DateClass, DateClass) => create[Date, Date, CBDate, CBDate](classOf[CBDate], classOf[CBDate], fileName, sorted)
      //else revert to default build
      case _ => {
        println("using super to build...")
        super.build(keyClass, valueClass, fileName, sorted)
      }
    }
    m.asInstanceOf[DiscMap[A, B]]
  }
}

class SByte(val get: Byte = 0) extends BComparable[Byte, SByte] {
  def this() = this(0)

  override val isFixed = true

  override def from(bb: ByteBuffer): SByte = new SByte(bb.get)

  override def to(bb: ByteBuffer): Unit = bb.put(get)

  override def byteSize(): Int = 1

  override def byteSize(bb: ByteBuffer): Int = 1

  override def create(t: Byte) = new SByte(t)

  override def compareTo(other: SByte): Int = get.compareTo(other.get)

  override def equals(obj: scala.Any): Boolean = obj match {
    case o: SByte => get == o.get
    case _ => false
  }

  override def hashCode: Int = get

  override def toString: String = "" + get

  override def byteCompare(offset1: Int, bb1: ByteBuffer, offset2: Int, bb2: ByteBuffer): Int = bb1.get(offset1) - bb2.get(offset2)
}

class SBoolean(val get: Boolean = false) extends BComparable[Boolean, SBoolean] {
  def this() = this(false)

  def this(b: Byte) = this(b match { case 0 => false; case _ => true })

  override val isFixed = true

  override def from(bb: ByteBuffer): SBoolean = new SBoolean(bb.get)

  override def to(bb: ByteBuffer): Unit = get match {
    case true => bb.put(1.toByte)
    case false => bb.put(0.toByte)
  }

  override def byteSize(): Int = 1

  override def byteSize(bb: ByteBuffer): Int = 1

  override def create(t: Boolean) = new SBoolean(t)

  override def compareTo(other: SBoolean): Int = get.compareTo(other.get)

  override def equals(obj: scala.Any): Boolean = obj match {
    case o: SBoolean => get == o.get
    case _ => false
  }

  override def hashCode: Int = get.hashCode

  override def toString: String = "" + get

  override def byteCompare(offset1: Int, bb1: ByteBuffer, offset2: Int, bb2: ByteBuffer): Int = bb1.get(offset1) - bb2.get(offset2)
}

class SChar(val get: Char = '\u0000') extends BComparable[Char, SChar] {
  def this() = this('\u0000')

  override val isFixed = true

  override def from(bb: ByteBuffer): SChar = new SChar(bb.getChar)

  override def to(bb: ByteBuffer): Unit = bb.putChar(get)

  override def byteSize(): Int = 2

  override def byteSize(bb: ByteBuffer): Int = 2

  override def create(t: Char) = new SChar(t)

  override def compareTo(other: SChar): Int = get.compareTo(other.get)

  override def equals(obj: scala.Any): Boolean = obj match {
    case o: SShort => get == o.get
    case _ => false
  }

  override def hashCode: Int = get.hashCode

  override def toString: String = "" + get

  override def byteCompare(offset1: Int, bb1: ByteBuffer, offset2: Int, bb2: ByteBuffer): Int = bb1.getChar(offset1).compareTo(bb2.getChar(offset2))
}


class SShort(val get: Short = 0) extends BComparable[Short, SShort] {
  def this() = this(0)

  override val isFixed = true

  override def from(bb: ByteBuffer): SShort = new SShort(bb.getShort)

  override def to(bb: ByteBuffer): Unit = bb.putShort(get)

  override def byteSize(): Int = 2

  override def byteSize(bb: ByteBuffer): Int = 2

  override def create(t: Short) = new SShort(t)

  override def compareTo(other: SShort): Int = get.compareTo(other.get)

  override def equals(obj: scala.Any): Boolean = obj match {
    case o: SShort => get == o.get
    case _ => false
  }

  override def hashCode: Int = get

  override def toString: String = "" + get

  override def byteCompare(offset1: Int, bb1: ByteBuffer, offset2: Int, bb2: ByteBuffer): Int = bb1.getShort(offset1) - bb2.getShort(offset2)
}

class SInt(val get: Int = 0) extends BComparable[Int, SInt] {
  def this() = this(0)

  override val isFixed = true

  override def from(bb: ByteBuffer): SInt = new SInt(bb.getInt)

  override def to(bb: ByteBuffer): Unit = bb.putInt(get)

  override def byteSize(): Int = 4

  override def byteSize(bb: ByteBuffer): Int = 4

  override def create(t: Int) = new SInt(t)

  override def compareTo(other: SInt): Int = get.compareTo(other.get)

  override def equals(obj: scala.Any): Boolean = obj match {
    case o: SInt => get == o.get
    case _ => false
  }

  override def hashCode: Int = get

  override def toString: String = "" + get

  override def byteCompare(offset1: Int, bb1: ByteBuffer, offset2: Int, bb2: ByteBuffer): Int = bb1.getInt(offset1) - bb2.getInt(offset2)
}

class SLong(val get: Long = 0) extends BComparable[Long, SLong] {
  def this() = this(0)

  override val isFixed = true

  override def from(bb: ByteBuffer): SLong = new SLong(bb.getLong)

  override def to(bb: ByteBuffer): Unit = bb.putLong(get)

  override def byteSize(): Int = 8

  override def byteSize(bb: ByteBuffer): Int = 8

  override def create(t: Long) = new SLong(t)

  override def compareTo(other: SLong): Int = get.compareTo(other.get)

  override def equals(obj: scala.Any): Boolean = obj match {
    case o: SLong => get == o.get
    case _ => false
  }

  override def hashCode: Int = get.hashCode

  override def toString: String = "" + get

  override def byteCompare(offset1: Int, bb1: ByteBuffer, offset2: Int, bb2: ByteBuffer): Int =
    bb1.getLong(offset1).compareTo(bb2.getLong(offset2))
}

class SFloat(val get: Float = 0) extends BComparable[Float, SFloat] {
  def this() = this(0)

  override val isFixed = true

  override def from(bb: ByteBuffer): SFloat = new SFloat(bb.getFloat)

  override def to(bb: ByteBuffer): Unit = bb.putFloat(get)

  override def byteSize(): Int = 4

  override def byteSize(bb: ByteBuffer): Int = 4

  override def create(t: Float) = new SFloat(t)

  override def compareTo(other: SFloat): Int = get.compareTo(other.get)

  override def equals(obj: scala.Any): Boolean = obj match {
    case o: SFloat => get == o.get
    case _ => false
  }

  override def hashCode: Int = get.hashCode

  override def toString: String = "" + get

  override def byteCompare(offset1: Int, bb1: ByteBuffer, offset2: Int, bb2: ByteBuffer): Int =
    bb1.getFloat(offset1).compareTo(bb2.getFloat(offset2))
}

class SDouble(val get: Double = 0) extends BComparable[Double, SDouble] {
  def this() = this(0)

  override val isFixed = true

  override def from(bb: ByteBuffer): SDouble = new SDouble(bb.getDouble)

  override def to(bb: ByteBuffer): Unit = bb.putDouble(get)

  override def byteSize(): Int = 8

  override def byteSize(bb: ByteBuffer): Int = 8

  override def create(t: Double) = new SDouble(t)

  override def compareTo(other: SDouble): Int = get.compareTo(other.get)

  override def equals(obj: scala.Any): Boolean = obj match {
    case o: SDouble => get == o.get
    case _ => false
  }

  override def hashCode: Int = get.hashCode

  override def toString: String = "" + get

  override def byteCompare(offset1: Int, bb1: ByteBuffer, offset2: Int, bb2: ByteBuffer): Int =
    bb1.getDouble(offset1).compareTo(bb2.getDouble(offset2))
}