/*
 * Copyright 2014–2016 SlamData Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ygg

import scalaz._, Scalaz._, Ordering._
import ygg.data._
import java.io.{ ByteArrayOutputStream, BufferedInputStream }
import java.nio.file._
import java.lang.Comparable
import java.math.MathContext.UNLIMITED

package object common extends quasar.Predef with pkg.PackageTime with pkg.PackageAliases {
  private val InputStreamBufferSize = 8192

  def slurpString(in: InputStream): String = new String(slurp(in), utf8Charset)
  def slurp(in: InputStream): Array[Byte]  = slurp(new BufferedInputStream(in))

  def slurp(in: BufferedInputStream): Array[Byte] = {
    val out = new ByteArrayOutputStream
    val buf = new Array[Byte](InputStreamBufferSize)
    def loop(): Array[Byte] = in read buf match {
      case -1 => out.toByteArray
      case n  => out.write(buf, 0, n) ; loop()
    }
    try loop() finally in.close()
  }
  def slurp(in: BufferedInputStream, len: Int): Array[Byte] = {
    val buf = new Array[Byte](len)
    def loop(remaining: Int): Array[Byte] = {
      if (remaining == 0) buf
      else in.read(buf, len - remaining, remaining) match {
        case -1 => buf
        case n  => loop(remaining - n)
      }
    }
    try loop(len) finally in.close()
  }


  def systemArraycopy(src: AnyRef, srcPos: Int, dest: AnyRef, destPos: Int, length: Int): Unit =
    java.lang.System.arraycopy(src, srcPos, dest, destPos, length)

  def emptyStreamT[A](): StreamT[Need, A]             = StreamT.empty[Need, A]
  def singleStreamT[A](value: => A): StreamT[Need, A] = value :: emptyStreamT[A]()
  def abort(msg: String): Nothing                     = throw new RuntimeException(msg)
  def newScratchDir(): jFile                          = Files.createTempDirectory("ygg").toFile

  def lp[T](label: String): T => Unit                 = (t: T) => println(label + ": " + t)
  def lpf[T](label: String)(f: T => Any): T => Unit   = (t: T) => println(label + ": " + f(t))

  def breakOut[From, T, To](implicit b: CBF[Nothing, T, To]): CBF[From, T, To] = scala.collection.breakOut[From, T, To](b)

  def warn[A](msg: String)(value: => A): A = {
    java.lang.System.err.println(msg)
    value
  }

  object decimal extends DecimalConstructors(UNLIMITED)

  implicit class jPathOps(private val p: jPath) {
    def slurpBytes(): Array[Byte] = Files readAllBytes p
    def slurpString(): String     = new String(slurpBytes, utf8Charset)
  }

  implicit class YggScalaVectorOps[A](private val xs: Vec[A]) {
    def reverse_:::(that: Vec[A]): Vec[A] = that.reverse ++ xs
    def :::(that: Vector[A]): Vec[A]      = that ++ xs
    def ::(head: A): Vec[A]               = head +: xs
    def shuffle: Vec[A]                   = scala.util.Random.shuffle(xs)
  }
  implicit class YggScalaMapOps[A, B](source: Map[A, B]) {
    def lazyMapValues[C](f: B => C): Map[A, C] = new LazyMap[A, B, C](source, f)
  }

  def cogroup[K, V, V1, CC[X] <: scIterable[X]](ls: scMap[K, CC[V]], rs: scMap[K, CC[V1]]): CoGroupResult[K, V, V1, CC] = Cogrouped(ls, rs).build

  implicit class YggByteBufferOps(private val bb: ByteBuffer) {
    def read[A](implicit z: Codec[A]): A = z read bb
  }
  implicit def ordToOrdering[A](implicit z: Ord[A]): scala.math.Ordering[A] = z.toScalaOrdering
  implicit def comparableOrder[A <: Comparable[A]]: Ord[A]                  = Ord order ((x, y) => Cmp(x compareTo y))
  implicit def bigDecimalOrder: Ord[BigDecimal]                             = Ord order ((x, y) => Cmp(x compare y))

  implicit class ScalazOrderOps[A](private val ord: Ord[A]) {
    private implicit def ordering: Ord[A] = ord
    def eqv(x: A, y: A): Boolean  = (x ?|? y) === EQ
    def lt(x: A, y: A): Boolean   = (x ?|? y) === LT
    def gt(x: A, y: A): Boolean   = (x ?|? y) === GT
    def lte(x: A, y: A): Boolean  = !gt(x, y)
    def gte(x: A, y: A): Boolean  = !lt(x, y)
    def neqv(x: A, y: A): Boolean = !eqv(x, y)
  }

  implicit def ValidationFlatMapRequested[E, A](d: Validation[E, A]): ValidationFlatMap[E, A] =
    Validation.FlatMap.ValidationFlatMapRequested[E, A](d)

  implicit class QuasarAnyOps[A](private val x: A) extends AnyVal {
    def |>[B](f: A => B): B       = f(x)
    def unsafeTap(f: A => Any): A = doto(x)(f)
  }

  implicit class EitherOps[A, B](private val x: Either[A, B]) extends AnyVal {
    def mapRight[C](f: B => C): Either[A, C] = x match {
      case Left(n)  => Left(n)
      case Right(n) => Right(f(n))
    }
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

package common {
  class DecimalConstructors(mc: java.math.MathContext) {
    def apply(d: java.math.BigDecimal): BigDecimal         = new BigDecimal(d, mc)
    def apply(d: String): BigDecimal                       = BigDecimal(d, mc)
    def apply(d: Int): BigDecimal                          = decimal(d.toLong)
    def apply(d: Long): BigDecimal                         = BigDecimal.decimal(d, mc)
    def apply(d: Double): BigDecimal                       = BigDecimal.decimal(d, mc)
    def apply(d: Float): BigDecimal                        = BigDecimal.decimal(d, mc)
    def apply(unscaledVal: BigInt, scale: Int): BigDecimal = BigDecimal(unscaledVal, scale, mc)
  }
}
