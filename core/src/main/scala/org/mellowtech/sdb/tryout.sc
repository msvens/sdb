import org.mellowtech.core.bytestorable.CBString
import org.mellowtech.core.collections.{DiscMap, DiscMapBuilder}
import org.mellowtech.sdb.model.COption


import org.mellowtech.sdb.{Generators, SDiscMapBuilder, SInt}

//val arr = new String("AAA").toCharArray

def fibRec(n: Int): Int = n match {
  case 0 => 0
  case 1 => 1
  case 2 => 1
  case _ => fibRec(n-1) + fibRec(n-2)
}

def fibIter(n: Int): Int = {
  var a = 0
  var b = 1
  var c = 0
  for(i <- 0 until n){
    c = a
    a = b
    b = c + a
  }
  a
}

fibRec(10)
fibIter(10)

