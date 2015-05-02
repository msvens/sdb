/**
 *
 */
package org.mellowtech
import com.github.nscala_time.time.Imports._
import java.util.Date
import org.mellowtech.core.bytestorable._
/**
 * @author msvens
 *
 */
package object dbm {
  import scala.reflect._
  

  object bcConversions {
   

    implicit def tToBC[T](v: T): BComparable[T,_] = {
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
      m.asInstanceOf[BComparable[T,_]]
      }
    }

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

  def bcmap[T: ClassTag]: BComparable[T,_] = {
    val m = classTag[T] match {
      case ITag    => new CBInt
      case BTag    => new CBByte
      case StrTag  => new CBString
      case CTag    => new CBChar
      case STag    => new CBShort
      case LTag    => new CBLong
      case FTag    => new CBFloat
      case DTag    => new CBDouble
      case DateTag => new CBDate
      case BATag   => new CBByteArray
      case _       => throw new ClassCastException
    }
    m.asInstanceOf[BComparable[T,_]]
  }

  def bcmap[T](v: T): BComparable[T,_] = {
    val m = v match {
      case x: Int         => new CBInt
      case x: String      => new CBString
      case x: Byte        => new CBByte
      case x: Char        => new CBChar
      case x: Short       => new CBShort
      case x: Long        => new CBLong
      case x: Float       => new CBFloat
      case x: Double      => new CBDouble
      case x: DateTime    => new CBDate
      case x: Date        => new CBDate
      case x: Array[Byte] => new CBByteArray
      case _              => throw new ClassCastException
    }
    m.asInstanceOf[BComparable[T,_]]
  }
  
  
    def bctype[A, B <: BComparable[A,B]](dbType: DbType.DbType): Class[B] = (dbType match{
      case DbType.INT => classOf[CBInt]
      case DbType.STRING => classOf[CBString]
      case DbType.BYTE => classOf[CBByte]
      case DbType.CHAR => classOf[CBChar]
      case DbType.SHORT => classOf[CBShort]
      case DbType.LONG => classOf[CBLong]
      case DbType.FLOAT => classOf[CBFloat]
      case DbType.DOUBLE => classOf[CBDouble]
      case DbType.DATE => classOf[CBDate]
      case DbType.BYTES => classOf[CBByteArray]
  }).asInstanceOf[Class[B]]
  
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