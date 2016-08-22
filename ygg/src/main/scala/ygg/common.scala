package ygg

import scalaz._, Ordering._
import ygg.data._
import java.nio.file._

package object common extends pkg.PackageTime with pkg.PackageAliases with pkg.PackageMethods {
  type CBF[-From, -Elem, +To] = scala.collection.generic.CanBuildFrom[From, Elem, To]

  implicit class jPathOps(private val p: jPath) {
    def slurpBytes(): Array[Byte] = Files readAllBytes p
    def slurpString(): String     = new String(slurpBytes, Utf8Charset)
  }

  implicit class ByteBufferOps(private val bb: ByteBuffer) {
    def read[A](implicit z: Codec[A]): A = z read bb
  }

  implicit class ScalaMapOps[K, V, CC[B] <: Traversable[B]](left: scMap[K, CC[V]]) {
    def cogroup[V1, That](right: scMap[K, CC[V1]])(implicit cbf: CBF[_, K -> Either3[V, CC[V] -> CC[V1], V1], That]): That = (
      new Cogrouped(left, right) build
    )
  }

  implicit def comparableOrder[A <: Comparable[A]]: Ord[A] =
    Ord.order[A]((x, y) => Cmp(x compareTo y))

  implicit def translateToScalaOrdering[A](implicit z: Ord[A]): scala.math.Ordering[A] = z.toScalaOrdering

  implicit class ScalazOrderOps[A](private val ord: Ord[A]) {
    def eqv(x: A, y: A): Boolean  = ord.order(x, y) == EQ
    def lt(x: A, y: A): Boolean   = ord.order(x, y) == LT
    def gt(x: A, y: A): Boolean   = ord.order(x, y) == GT
    def lte(x: A, y: A): Boolean  = !gt(x, y)
    def gte(x: A, y: A): Boolean  = !lt(x, y)
    def neqv(x: A, y: A): Boolean = !eqv(x, y)
  }

  implicit def ValidationFlatMapRequested[E, A](d: Validation[E, A]): ValidationFlatMap[E, A] =
    Validation.FlatMap.ValidationFlatMapRequested[E, A](d)

  implicit def bigDecimalOrder: Ord[BigDecimal] = Ord.order[BigDecimal]((x, y) => Cmp(x compare y))

  implicit class QuasarAnyOps[A](private val x: A) extends AnyVal {
    def |>[B](f: A => B): B       = f(x)
    def unsafeTap(f: A => Any): A = doto(x)(f)
  }

  implicit class LazyMapValues[A, B](source: Map[A, B]) {
    def lazyMapValues[C](f: B => C): Map[A, C] = new LazyMap[A, B, C](source, f)
  }

  implicit class BitSetOperations(private val bs: BitSet) extends AnyVal {
    def +(elem: Int)      = doto(bs.copy)(_ set elem)
    def &(other: BitSet)  = doto(bs.copy)(_ and other)
    def &~(other: BitSet) = doto(bs.copy)(_ andNot other)
    def |(other: BitSet)  = doto(bs.copy)(_ or other)

    def isEmpty(): Boolean =
      bs.nextSetBit(0) < 0

    def min(): Int = {
      val n = bs.nextSetBit(0)
      if (n < 0) abort("can't take min of empty set") else n
    }

    def max(): Int = {
      @tailrec
      def findBit(i: Int, last: Int): Int = {
        val j = bs.nextSetBit(i)
        if (j < 0) last else findBit(j + 1, j)
      }

      val ns = bs.getBits
      var i  = ns.length - 1
      while (i >= 0) {
        if (ns(i) != 0) return findBit(i * 64, -1)
        i -= 1
      }
      abort("can't find max of empty set")
    }

    def foreach(f: Int => Unit) = {
      var b = bs.nextSetBit(0)
      while (b >= 0) {
        f(b)
        b = bs.nextSetBit(b + 1)
      }
    }

    def toList: List[Int] = {
      @tailrec
      def loopBits(long: Long, bit: Int, base: Int, sofar: List[Int]): List[Int] = {
        if (bit < 0)
          sofar
        else if (((long >> bit) & 1) == 1)
          loopBits(long, bit - 1, base, (base + bit) :: sofar)
        else
          loopBits(long, bit - 1, base, sofar)
      }

      @tailrec
      def loopLongs(i: Int, longs: Array[Long], base: Int, sofar: List[Int]): List[Int] = {
        if (i < 0)
          sofar
        else
          loopLongs(i - 1, longs, base - 64, loopBits(longs(i), 63, base, sofar))
      }

      val last = bs.getBitsLength - 1
      loopLongs(last, bs.getBits, last * 64, Nil)
    }
  }
}
