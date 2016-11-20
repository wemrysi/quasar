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

import scalaz._
import ygg.data._
import java.io.{ ByteArrayOutputStream, BufferedInputStream }
import java.nio.file._
import java.lang.Comparable

package object common extends quasar.Predef
        with ygg.pkg.PackageTime
        with ygg.pkg.PackageAliases
        with ygg.pkg.PackageMethods
        with matryoshka.Corecursive.ToCorecursiveOps
        with matryoshka.Recursive.ToRecursiveOps
        with matryoshka.FunctorT.ToFunctorTOps
        with quasar.RenderTreeT.ToRenderTreeTOps
        with quasar.RenderTree.ToRenderTreeOps
        // with quasar.contrib.matryoshka.ShowT.ToShowTOps
{
  private val InputStreamBufferSize = 8192

  def indent(spaces: Int, s: String): String = s.lines map ("  " + _) mkString "\n"

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

  object decimal extends DecimalConstructors(java.math.MathContext.UNLIMITED)

  implicit class PairOfOps[A](private val self: PairOf[A]) {
    def mapBoth[B](f: A => B): PairOf[B] = f(self._1) -> f(self._2)
  }

  implicit class ThrowValidationOps[A](private val self: Validation[Throwable, A]) {
    def orThrow: A = self.fold(throw _, x => x)
  }

  implicit class jPathOps(private val p: jPath) {
    def slurpBytes(): Array[Byte] = Files readAllBytes p
    def slurpString(): String     = new String(slurpBytes, utf8Charset)
  }
  implicit class jFileOps(private val f: jFile) {
    def slurpBytes(): Array[Byte] = f.toPath.slurpBytes()
    def slurpString(): String     = f.toPath.slurpString()
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

  def cogroup[K, V, V1, CC[X] <: scIterable[X]](ls: Map[K, CC[V]], rs: Map[K, CC[V1]]): Cogrouped.Result[K, V, V1, CC] = Cogrouped.Builder(ls, rs).build

  implicit class YggByteBufferOps(private val bb: ByteBuffer) {
    def read[A](implicit z: Codec[A]): A = z read bb
  }
  implicit def ordToOrdering[A](implicit z: Ord[A]): scala.math.Ordering[A] = z.toScalaOrdering
  implicit def comparableOrder[A <: Comparable[A]]: Ord[A]                  = Ord order ((x, y) => Cmp(x compareTo y))
  implicit def bigDecimalOrder: Ord[BigDecimal]                             = Ord order ((x, y) => Cmp(x compare y))

  implicit class ScalazOrderOps[A](private val ord: Ord[A]) {
    import Scalaz._, Ordering._
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
    def isEmpty: Boolean  = minBit < 0
    def min(): Int        = if (isEmpty) abort("can't take min of empty set") else minBit

    private def minBit: Int = bs nextSetBit 0

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

    def toList = toVector.toList
    def toVector: Vector[Int] = {
      val buf   = Vector.newBuilder[Int]
      val longs = bs.getBits
      var index = bs.getBitsLength - 1
      def base  = index * 64

      @tailrec def loopBits(long: Long, bit: Int): Unit = {
        if (bit >= 0) {
          if (((long >> bit) & 1) != 0)
            buf += (base + bit)

          loopBits(long, bit - 1)
        }
      }

      while (index >= 0) {
        loopBits(longs(index), 63)
        index -= 1
      }

      @tailrec def loopLongs(): Unit = {
        if (index >= 0) {
          loopBits(longs(index), 63)
          index -= 1
          loopLongs()
        }
      }

      loopLongs()
      buf.result.reverse
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
    def apply(d: scala.Float): BigDecimal                  = BigDecimal.decimal(d, mc)
    def apply(unscaledVal: BigInt, scale: Int): BigDecimal = BigDecimal(unscaledVal, scale, mc)
  }
}
