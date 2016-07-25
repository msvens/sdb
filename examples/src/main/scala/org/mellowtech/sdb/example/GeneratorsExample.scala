package org.mellowtech.sdb.example

import java.util.concurrent.TimeUnit

import com.google.common.base.Stopwatch
import org.mellowtech.sdb.Generators

/**
  * Created by msvens on 18/12/15.
  */
object GeneratorsExample extends App{

  val numItems = 100000000
  var curr = 0;

  val sGen: Iterator[String] = Generators[String]()

  curr = 0
  val sWatch = Stopwatch.createStarted()
  while(curr < numItems){
    sGen.next()
    curr += 1
  }
  sWatch.stop
  Console.println("millis string: "+(numItems / sWatch.elapsed(TimeUnit.MILLISECONDS)))

  var cGen: Iterator[Array[Char]] = Generators[Array[Char]](true)

  sWatch.reset()
  sWatch.start()
  curr = 0
  while(curr < numItems){
    cGen.next()
    curr += 1
  }
  sWatch.stop
  Console.println("millis mutable char array: "+(numItems / sWatch.elapsed(TimeUnit.MILLISECONDS)))


  cGen = Generators[Array[Char]](false)
  curr = 0;
  sWatch.reset()
  sWatch.start()
  while(curr < numItems){
    cGen.next()
    curr += 1
  }
  sWatch.stop
  Console.println("millis imutable char array: "+(numItems / sWatch.elapsed(TimeUnit.MILLISECONDS)))

  var bGen: Iterator[Array[Byte]] = Generators[Array[Byte]](true)
  curr = 0;
  sWatch.reset()
  sWatch.start()
  while(curr < numItems){
    bGen.next()
    curr += 1
  }
  sWatch.stop
  Console.println("millis mutable byte array: "+(numItems / sWatch.elapsed(TimeUnit.MILLISECONDS)))


  bGen = Generators[Array[Byte]](false)
  curr = 0
  sWatch.reset()
  sWatch.start()
  while(curr < numItems){
    bGen.next()
    curr += 1
  }
  sWatch.stop
  Console.println("millis imutable byte array: "+(numItems / sWatch.elapsed(TimeUnit.MILLISECONDS)))

  var iGen: Iterator[Int] = Generators[Int]()
  curr = 0
  sWatch.reset()
  sWatch.start()
  while(curr < numItems){
    iGen.next()
    curr += 1
  }
  sWatch.stop
  Console.println("millis ints: "+(numItems / sWatch.elapsed(TimeUnit.MILLISECONDS)))

  var lGen: Iterator[Long] = Generators[Long]()
  curr = 0
  sWatch.reset()
  sWatch.start()
  while(curr < numItems){
    lGen.next()
    curr += 1
  }
  sWatch.stop
  Console.println("millis longs: "+(numItems / sWatch.elapsed(TimeUnit.MILLISECONDS)))



}
