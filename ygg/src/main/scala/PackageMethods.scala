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

package ygg.pkg

import java.nio.file._
import java.math.MathContext.UNLIMITED
import scala.collection.{ mutable => scm, immutable => sci }
import java.io.{ ByteArrayOutputStream, BufferedInputStream }
import scalaz.{ Need, StreamT }

trait PackageMethods {
  self: ygg.common.`package`.type =>

  def Cmp(n: Int): Cmp                                = scalaz.Ordering fromInt n
  def emptyStreamT[A](): StreamT[Need, A]             = StreamT.empty[Need, A]
  def singleStreamT[A](value: => A): StreamT[Need, A] = value :: emptyStreamT[A]()

  def breakOut[From, T, To](implicit b: CBF[Nothing, T, To]): CBF[From, T, To] =
    scala.collection.breakOut[From, T, To](b)

  def scmSet[A](): scmSet[A]                                      = scm.HashSet[A]()
  def sciQueue[A](): sciQueue[A]                                  = sci.Queue[A]()
  def sciTreeMap[K: smOrdering, V](xs: (K, V)*): sciTreeMap[K, V] = sci.TreeMap[K, V](xs: _*)
  def jclass[A: CTag]: jClass                                     = ctag[A].runtimeClass.asInstanceOf[jClass]
  def jPath(path: String): jPath                                  = Paths get path
  def jclassLoader[A: CTag]: ClassLoader                          = jclass[A].getClassLoader
  def jResource[A: CTag](name: String): InputStream               = jResource(jclass[A], name)
  def jResource(c: jClass, name: String): InputStream             = c getResourceAsStream name
  def newScratchDir(): File                                       = Files.createTempDirectory("ygg").toFile
  def systemMillis(): Long                                        = java.lang.System.currentTimeMillis()
  def implicitly[A](implicit z: A): A                             = z
  def discard[A](value: A): Unit                                  = () // for avoiding "discarding non-Unit value" warnings
  def mutableQueue[A: Ord](xs: A*): scmPriorityQueue[A]           = scmPriorityQueue(xs: _*)

  def systemArraycopy(src: AnyRef, srcPos: Int, dest: AnyRef, destPos: Int, length: Int): Unit =
    java.lang.System.arraycopy(src, srcPos, dest, destPos, length)

  def warn[A](msg: String)(value: => A): A = {
    java.lang.System.err.println(msg)
    value
  }
  def ctag[A](implicit z: CTag[A]): CTag[A] = z

  def Utf8Charset: Charset                                           = java.nio.charset.Charset forName "UTF-8"
  def utf8Bytes(s: String): Array[Byte]                              = s getBytes Utf8Charset
  def uuid(s: String): UUID                                          = java.util.UUID fromString s
  def randomUuid(): UUID                                             = java.util.UUID.randomUUID
  def randomInt(end: Int): Int                                       = scala.util.Random.nextInt(end)
  def randomDouble(): Double                                         = scala.util.Random.nextDouble
  def randomBool(): Boolean                                          = scala.util.Random.nextBoolean
  def charBuffer(size: Int): CharBuffer                              = java.nio.CharBuffer.allocate(size)
  def charBuffer(xs: String): CharBuffer                             = java.nio.CharBuffer.wrap(xs)
  def byteBuffer(xs: Array[Byte]): ByteBuffer                        = java.nio.ByteBuffer.wrap(xs)
  def byteBuffer(xs: Array[Byte], offset: Int, len: Int): ByteBuffer = java.nio.ByteBuffer.wrap(xs, offset, len)
  def abort(msg: String): Nothing                                    = throw new RuntimeException(msg)
  def lp[T](label: String): T => Unit                                = (t: T) => println(label + ": " + t)
  def lpf[T](label: String)(f: T => Any): T => Unit                  = (t: T) => println(label + ": " + f(t))

  def doto[A](x: A)(f: A => Any): A = { f(x); x }

  def decimal(d: java.math.BigDecimal): BigDecimal         = new BigDecimal(d, UNLIMITED)
  def decimal(d: String): BigDecimal                       = BigDecimal(d, UNLIMITED)
  def decimal(d: Int): BigDecimal                          = decimal(d.toLong)
  def decimal(d: Long): BigDecimal                         = BigDecimal.decimal(d, UNLIMITED)
  def decimal(d: Double): BigDecimal                       = BigDecimal.decimal(d, UNLIMITED)
  def decimal(d: Float): BigDecimal                        = BigDecimal.decimal(d, UNLIMITED)
  def decimal(unscaledVal: BigInt, scale: Int): BigDecimal = BigDecimal(unscaledVal, scale, UNLIMITED)

  private final val InputStreamBufferSize = 8192

  def slurpString(in: InputStream): String = new String(slurp(in), Utf8Charset)
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

}
