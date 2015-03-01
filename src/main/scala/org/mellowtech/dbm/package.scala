/**
 *
 */
package org.mellowtech
import com.github.nscala_time.time.Imports._
import java.util.Date
import com.mellowtech.core.collections.mappings._
/**
 * @author msvens
 *
 */
package object dbm {
  import scala.reflect._
  import com.mellowtech.core.bytestorable._

  object bcConversions {
    implicit def IntToBC(i: Int) = (new CBInt(i)).asInstanceOf[ByteComparable[Int]]
    implicit def CBIntToInt(cbi: CBInt) = cbi.get

    implicit def tToBC[T](v: T): ByteComparable[T] = {
      if(v == null) null
      else {
      val m = v match {
        case x: Int         => new CBInt(x)
        case x: String      => new CBString(x)
        case x: Byte        => new CBByte(x)
        case x: Char        => new CBChar(x)
        case x: Short       => new CBShort(x)
        case x: Long        => new CBLong(x)
        case x: Float       => new CBFloat(x)
        case x: Double      => new CBDouble(x)
        case x: DateTime    => new CBDate(x.toDate)
        case x: Date        => new CBDate(x)
        case x: Array[Byte] => new CBByteArray(x)
        case _              => throw new ClassCastException
      }
      m.asInstanceOf[ByteComparable[T]]
      }
    }
    implicit def bcToT[T](bc: ByteStorable[T]): T = {
      if(bc == null)
        null.asInstanceOf[T]
      else
        bc.get
      /*bc match {
        case x: ByteComparable[Int] => x.get
        case x: 
        case _ => throw new ClassNotFoundException*/
      //}
     
      /*val m = v match {
        case x: Int         => new CBInt(x)
        case x: String      => new CBString(x)
        case x: Byte        => new CBByte(x)
        case x: Char        => new CBChar(x)
        case x: Short       => new CBShort(x)
        case x: Long        => new CBLong(x)
        case x: Float       => new CBFloat(x)
        case x: Double      => new CBDouble(x)
        case x: DateTime    => new CBDate(x.toDate)
        case x: Date        => new CBDate(x)
        case x: Array[Byte] => new CBByteArray(x)
        case _              => throw new ClassCastException
      }
      m.asInstanceOf[ByteComparable[T]]*/
    }
    /*implicit def tToBS[T](v: T): ByteStorable[T] = {
      val m = v match {
        case x: Int         => new CBInt(x)
        case x: String      => new CBString(x)
        case x: Byte        => new CBByte(x)
        case x: Char        => new CBChar(x)
        case x: Short       => new CBShort(x)
        case x: Long        => new CBLong(x)
        case x: Float       => new CBFloat(x)
        case x: Double      => new CBDouble(x)
        case x: DateTime    => new CBDate(x.toDate)
        case x: Date        => new CBDate(x)
        case x: Array[Byte] => new CBByteArray(x)
        case _              => throw new ClassCastException
      }
      m.asInstanceOf[ByteStorable[T]]
    }*/

  }

  type ColumnName = String
  
  object SearchType extends Enumeration {
    type SearchType = Value
    val FIELD, TEXT, NONE = Value
  }

  object DbType extends Enumeration {
    type DbType = Value
    val BYTE, SHORT, CHAR, INT, LONG, FLOAT, DOUBLE, STRING, DATE, BYTES = Value
  }

  object TableType extends Enumeration {
    type TableType = Value
    val MEMORY_ROW, MEMORY_COLUMN, COLUMN, ROW = Value
  }

  val ITag = classTag[Int]
  val BTag = classTag[Byte]
  val StrTag = classTag[String]
  val CTag = classTag[Char]
  val STag = classTag[Short]
  val LTag = classTag[Long]
  val FTag = classTag[Float]
  val DTag = classTag[Double]
  val DTTag = classTag[DateTime]
  val DateTag = classTag[Date]
  val BATag = classTag[Array[Byte]]

  def bcmap[T: ClassTag]: BCMapping[T] = {
    val m = classTag[T] match {
      case ITag    => new IntegerMapping
      case BTag    => new ByteMapping
      case StrTag  => new StringMapping
      case CTag    => new CharacterMapping
      case STag    => new ShortMapping
      case LTag    => new LongMapping
      case FTag    => new FloatMapping
      case DTag    => new DoubleMapping
      case DateTag => new DateMapping
      case BATag   => new ByteArrayMapping
      case _       => throw new ClassCastException
    }
    m.asInstanceOf[BCMapping[T]]
  }

  def bcmap[T](v: T): BCMapping[T] = {
    val m = v match {
      case x: Int         => new IntegerMapping
      case x: String      => new StringMapping
      case x: Byte        => new ByteMapping
      case x: Char        => new CharacterMapping
      case x: Short       => new ShortMapping
      case x: Long        => new LongMapping
      case x: Float       => new FloatMapping
      case x: Double      => new DoubleMapping
      case x: DateTime    => new DateMapping
      case x: Date        => new DateMapping
      case x: Array[Byte] => new ByteArrayMapping
      case _              => throw new ClassCastException
    }
    m.asInstanceOf[BCMapping[T]]
  }
  
  def bcmap[T](dbType: DbType.DbType): BCMapping[T] = (dbType match{
    case DbType.INT => new IntegerMapping
    case DbType.STRING => new StringMapping
    case DbType.BYTE => new ByteMapping
    case DbType.CHAR => new CharacterMapping
    case DbType.SHORT => new ShortMapping
    case DbType.LONG => new LongMapping
    case DbType.FLOAT => new FloatMapping
    case DbType.DOUBLE => new DoubleMapping
    case DbType.DATE => new DateMapping
    case DbType.BYTES => new ByteArrayMapping
  }).asInstanceOf[BCMapping[T]]
  
  /*
  def dbOrdering(dbType: DbType.DbType) = dbType match {
    case DbType.INT => Ordering[Int]
  }*/
  
  def dbType[T:ClassTag]:DbType.DbType = classTag[T] match {
      case ITag    => DbType.INT
      case BTag    => DbType.BYTE
      case StrTag  => DbType.STRING
      case CTag    => DbType.CHAR
      case STag    => DbType.SHORT
      case LTag    => DbType.LONG
      case FTag    => DbType.FLOAT
      case DTag    => DbType.DOUBLE
      case DateTag => DbType.DATE
      case BATag   => DbType.BYTES
      case _       => throw new ClassCastException
    }

  def dbType[T](v: T): DbType.DbType = v match {
    case x: Int         => DbType.INT
    case x: String      => DbType.STRING
    case x: Byte        => DbType.BYTE
    case x: Char        => DbType.CHAR
    case x: Short       => DbType.SHORT
    case x: Long        => DbType.LONG
    case x: Float       => DbType.FLOAT
    case x: Double      => DbType.DOUBLE
    case x: DateTime    => DbType.DATE
    case x: Date        => DbType.DATE
    case x: Array[Byte] => DbType.BYTES
    case _              => throw new ClassCastException
  }

  def incr[V](v: V): V = {
    val c = v match {
      case x: Int => x + 1
      case x: String => {
        if (x.isEmpty) "" + 0.toChar else x.substring(0, x.length() - 1) + (x.last + 1).toChar
      }
      case x: Byte     => x + 1
      case x: Char     => (x + 1).toChar
      case x: Short    => x + 1
      case x: Long     => x + 1
      case x: Float    => x + 1.0
      case x: Double   => x + 1.0
      case x: DateTime => x + 1
      case x: Date     => new Date(x.getTime + 1)
      case _           => throw new ClassCastException
    }
    c.asInstanceOf[V]
  }
  def decr[V](v: V): V = {
    val c = v match {
      case x: Int => x - 1
      case x: String => {
        if (x.isEmpty) "" + 0.toChar else x.substring(0, x.length() - 1) + (x.last - 1).toChar
      }
      case x: Byte     => x - 1
      case x: Char     => (x - 1).toChar
      case x: Short    => x - 1
      case x: Long     => x - 1
      case x: Float    => x - 1.0
      case x: Double   => x - 1.0
      case x: DateTime => x - 1
      case x: Date     => new Date(x.getTime - 1)
      case _           => throw new ClassCastException
    }
    c.asInstanceOf[V]
  }
  def min[V](v: V): V = {
    val c = v match {
      case x: Int      => Int.MinValue
      case x: String   => "" + Char.MinValue
      case x: Byte     => Byte.MinValue
      case x: Char     => Char.MinValue
      case x: Short    => Short.MinValue
      case x: Long     => Long.MinValue
      case x: Float    => Float.MinValue
      case x: Double   => Double.MinValue
      case x: DateTime => new DateTime(Long.MinValue)
      case x: Date     => new Date(Long.MinValue)
      case _           => throw new ClassCastException
    }
    c.asInstanceOf[V]
  }
  def max[V](v: V): V = {
    val c = v match {
      case x: Int      => Int.MaxValue
      case x: String   => "" + Char.MaxValue
      case x: Byte     => Byte.MaxValue
      case x: Char     => Char.MaxValue
      case x: Short    => Short.MaxValue
      case x: Long     => Long.MaxValue
      case x: Float    => Float.MaxValue
      case x: Double   => Double.MaxValue
      case x: DateTime => new DateTime(Long.MaxValue)
      case x: Date     => new Date(Long.MaxValue)
      case _           => throw new ClassCastException
    }
    c.asInstanceOf[V]
  }

}