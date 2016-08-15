package quasar
package precog

import java.nio.file._
import java.math.MathContext.UNLIMITED
import scala.collection.{ mutable => scm, immutable => sci }

trait PackageMethods {
  self: PackageAliases =>

  def scmSet[A](): scmSet[A]                                    = scm.HashSet[A]()
  def sciQueue[A](): sciQueue[A]                                = sci.Queue[A]()
  def sciTreeMap[K: Ordering, V](xs: (K, V)*): sciTreeMap[K, V] = sci.TreeMap[K, V](xs: _*)
  def jclass[A: CTag]: jClass                                   = ctag[A].runtimeClass.asInstanceOf[jClass]
  def jPath(path: String): jPath                                = Paths get path

  def warn[A](msg: String)(value: => A): A = {
    java.lang.System.err.println(msg)
    value
  }
  def ctag[A](implicit z: CTag[A]): CTag[A] = z

  def Utf8Charset: Charset                                               = java.nio.charset.Charset forName "UTF-8"
  def utf8Bytes(s: String): Array[Byte]                                  = s getBytes Utf8Charset
  def uuid(s: String): UUID                                              = java.util.UUID fromString s
  def randomUuid(): UUID                                                 = java.util.UUID.randomUUID
  def randomInt(end: Int): Int                                           = scala.util.Random.nextInt(end)
  def randomBool(): Boolean                                              = scala.util.Random.nextBoolean
  def ByteBufferWrap(xs: Array[Byte]): ByteBuffer                        = java.nio.ByteBuffer.wrap(xs)
  def ByteBufferWrap(xs: Array[Byte], offset: Int, len: Int): ByteBuffer = java.nio.ByteBuffer.wrap(xs, offset, len)
  def abort(msg: String): Nothing                                        = throw new RuntimeException(msg)
  def lp[T](label: String): T => Unit                                    = (t: T) => println(label + ": " + t)
  def lpf[T](label: String)(f: T => Any): T => Unit                      = (t: T) => println(label + ": " + f(t))

  def doto[A](x: A)(f: A => Any): A = { f(x) ; x }

  def arrayEq[@specialized A](a1: Array[A], a2: Array[A]): Boolean = {
    val len = a1.length
    if (len != a2.length) return false
    var i = 0
    while (i < len) {
      if (a1(i) != a2(i)) return false
      i += 1
    }
    true
  }

  implicit class jPathOps(private val p: jPath) {
    def slurpBytes(): Array[Byte] = Files readAllBytes p
  }


  def decimal(d: java.math.BigDecimal): BigDecimal         = new BigDecimal(d, UNLIMITED)
  def decimal(d: String): BigDecimal                       = BigDecimal(d, UNLIMITED)
  def decimal(d: Int): BigDecimal                          = decimal(d.toLong)
  def decimal(d: Long): BigDecimal                         = BigDecimal.decimal(d, UNLIMITED)
  def decimal(d: Double): BigDecimal                       = BigDecimal.decimal(d, UNLIMITED)
  def decimal(d: Float): BigDecimal                        = BigDecimal.decimal(d, UNLIMITED)
  def decimal(unscaledVal: BigInt, scale: Int): BigDecimal = BigDecimal(unscaledVal, scale, UNLIMITED)
}
