package org.mellowtech.sdb.recovery

import java.io.File
import java.nio.channels.{FileChannel, WritableByteChannel, ReadableByteChannel}
import java.nio.file.StandardOpenOption
import java.util.concurrent.atomic.AtomicInteger

import org.mellowtech.core.bytestorable.CBByteArray
import org.mellowtech.sdb.{SRow,STable,FileTable}

/**
 * @author msvens
 */
object Log {
  
  import org.mellowtech.core.bytestorable.{BStorable,CBUtil}
  import java.nio.ByteBuffer
  import scala.util.{Try,Success,Failure}
  
  object Action extends Enumeration {
    type Action = Value
    val PUT,DELETE = Value
  }
  
  object Status extends Enumeration {
    type Status = Value
    val START,ABORT,COMMIT,FAIL = Value
  }
  
  import Action._
  import Status._
  
  case class Record(id: Long = 0, action: Action = PUT, status: Status = START, content: Array[Byte] = Array[Byte](0)) extends BStorable[Record,Record]{
    
    override def get = this
    
    override def from(bb: ByteBuffer): Record = {
      CBUtil.getSize(bb, true)
      val id = bb.getLong
      val a = bb.get
      val s = bb.get
      val c = new CBByteArray().from(bb).get()
      Record(id, Action(a), Status(s), c)
    }

    override def to(bb: ByteBuffer): Unit = {
      CBUtil.putSize(internalSize,bb,true)
      bb.putLong(id)
      bb.put(action.id.asInstanceOf[Byte])
      bb.put(status.id.asInstanceOf[Byte])
      new CBByteArray(content).to(bb)
    }

    override def byteSize(): Int = CBUtil.byteSize(internalSize, true)

    override def byteSize(bb: ByteBuffer): Int = CBUtil.peekSize(bb, true)

    private def internalSize = 8+1+1+ new CBByteArray(content).byteSize()
  }

  class WAL[A : Ordering](c: WritableByteChannel, idOffset: Int){
    private val atomic = new AtomicInteger(idOffset)

    def log(action: Action, content: Array[Byte])(f: => Try[Unit]): Unit = {
      val id = atomic.getAndIncrement
      val sr = Record(id,action, START, content)
      sr.to(c)
      f match {
        case Success(_) => Record(id, action, COMMIT).to(c)
        case _ => Record(id, action, FAIL).to(c)
      }
    }

    def logPut(r: SRow)(f: => Try[Unit]): Unit = ???
    //log(PUT,SRow.toBytes(r))(f)
    def logDelete(key: A)(f: => Try[Unit]): Unit = ???
    ///log(DELETE, SRow.toBytes(SRow(key)))(f)
    def close = c.close()
  }
  
  class Recovery[A : Ordering](c: ReadableByteChannel, startOffset: Int){
    
    private val temp = Record()
    private def readNext: Try[Record] = try {
        val rr = temp.from(c)
        Success(rr)
      } catch {
        case e: Exception => Failure(e)
   }
   
   def playback: Unit = readNext match {
        case Success(rec) => {println("success"); playback}
        case Failure(e) => println("end")
    }
   
   def recover: Try[Unit] = readNext match {
     case Success(rec) => {
       rec.status match {
         case START => Success(Unit)
         case ABORT => Success(Unit)
         case COMMIT => Success(Unit)
       }
     }
     case Failure(e) => {
       Success(Unit)
     }
     
   }
    
  }

  def apply[A](t: STable[A])(implicit ord: Ordering[A]): WAL[A] = {
    val path = t.path.get + "/wal.log"
    val f = new File(path)
    val c = FileChannel.open(f.toPath, StandardOpenOption.WRITE, StandardOpenOption.CREATE)
    new WAL(c, 0)
  }
  
}

