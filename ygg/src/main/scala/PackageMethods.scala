package ygg.pkg

import java.nio.file._
import java.math.MathContext.UNLIMITED
import scala.collection.{ mutable => scm, immutable => sci }
import java.io.{ ByteArrayOutputStream, BufferedInputStream }
import scalaz.{ Need, StreamT }

trait PackageMethods { self: PackageAliases =>

  def emptyStreamT[A](): StreamT[Need, A]             = StreamT.empty[Need, A]
  def singleStreamT[A](value: => A): StreamT[Need, A] = value :: emptyStreamT[A]()

  def scmSet[A](): scmSet[A]                                    = scm.HashSet[A]()
  def sciQueue[A](): sciQueue[A]                                = sci.Queue[A]()
  def sciTreeMap[K: Ordering, V](xs: (K, V)*): sciTreeMap[K, V] = sci.TreeMap[K, V](xs: _*)
  def jclass[A: CTag]: jClass                                   = ctag[A].runtimeClass.asInstanceOf[jClass]
  def jPath(path: String): jPath                                = Paths get path
  def jclassLoader[A: CTag]: ClassLoader                        = jclass[A].getClassLoader
  def jResource[A: CTag](name: String): InputStream             = jResource(jclass[A], name)
  def jResource(c: jClass, name: String): InputStream           = c getResourceAsStream name

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
