package org.mellowtech.sdb

import java.util.Arrays
import java.util.concurrent.atomic.AtomicInteger

import scala.reflect._
import scala.util.control.Breaks._

/**
  * Created by msvens on 16/12/15.
  */
object Generators {


  private val BCTag = classTag[Array[Char]]

  final private class StringIter(from: Char = 'A', to: Char = 'Z', length: Int = 8) extends Iterator[String] {

    private val cIter = new CharIter(from, to, length, true)

    override def hasNext: Boolean = cIter.hasNext

    override def next(): String = {
      new String(cIter.next())
    }
  }

  final private class IntIter(var from: Int = 0, to: Int = Int.MaxValue) extends Iterator[Int] {
    override def hasNext: Boolean = true
    override def next(): Int = {
      from += 1
      from
    }
  }

  final private class LongIter(var from: Long = 0, to: Long = Long.MaxValue) extends Iterator[Long] {
    override def hasNext: Boolean = true
    override def next(): Long = {
      from += 1
      from
    }
  }

  final private class CharIter(from: Char = 'A', to: Char = 'Z', length: Int = 8, mutable: Boolean = false) extends Iterator[Array[Char]] {

    private val arr: Array[Char] = Array.fill(length)(from)

    override def hasNext: Boolean = true

    override def next(): Array[Char] = {
      nextRec(arr, arr.length - 1, from, to)
      if (mutable) {
        arr
      } else {
        Arrays.copyOf(arr, arr.length)
      }
    }
  }

  final private class ByteIter(from: Byte = 'A'.toByte, to: Byte = 'Z'.toByte, length: Int = 8, mutable: Boolean = false) extends Iterator[Array[Byte]] {

    private val arr: Array[Byte] = Array.fill(length)(from)

    override def hasNext: Boolean = true

    override def next(): Array[Byte] = {
      nextRec(arr, arr.length-1, from, to)
      if(mutable)
        arr
      else
        Arrays.copyOf(arr, arr.length)
    }
  }

  def nextRec(str: Array[Char], idx: Int, from: Char, to: Char): Unit = idx match {
    case -1 =>
    case _ => {
      val c = str(idx)
      if(c < to){
        str.update(idx, (c + 1).toChar)
      } else {
        str.update(idx, from)
        nextRec(str, idx - 1, from, to)
      }
    }
  }

  def nextRec(arr: Array[Byte], idx: Int, from: Byte, to: Byte): Unit = idx match {
    case -1 =>
    case _ => {
      val c = arr(idx)
      if(c < to){
        arr.update(idx, (c + 1).toByte)
      } else {
        arr.update(idx, from)
        nextRec(arr, idx - 1, from, to)
      }
    }
  }


  def incrementB(arr: Array[Byte], from: Byte = 'A'.toByte, to: Byte = 'Z'.toByte): Unit = {
    nextRec(arr, arr.length-1, from, to)
  }

  def incrementC(arr: Array[Char], from: Char = 'A', to: Char = 'Z'): Unit = {
    nextRec(arr, arr.length - 1, from, to)
  }

  def apply[A: ClassTag](mut: Boolean): Iterator[A] = {
    val iter = classTag[A] match {
      case StrTag => new StringIter()
      case BCTag => new  CharIter(mutable = mut)
      case BATag => new ByteIter(mutable = mut)
      case ITag => new IntIter
      case LTag => new LongIter
      case _ => throw new Error("no generator for type")
    }
    iter.asInstanceOf[Iterator[A]]
  }

  def apply[A: ClassTag](): Iterator[A] = apply(false)



}
