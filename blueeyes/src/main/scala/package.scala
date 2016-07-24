import scalaz._
import java.time.ZoneOffset.UTC

package object blueeyes extends quasar.precog.PackageTime with blueeyes.PackageAliases {
  type spec    = scala.specialized
  type switch  = scala.annotation.switch
  type tailrec = scala.annotation.tailrec

  // Temporary
  type BitSet             = com.precog.BitSet
  type RawBitSet          = Array[Int]
  val RawBitSet           = com.precog.util.RawBitSet
  type ByteBufferPoolS[A] = State[com.precog.util.ByteBufferPool -> List[ByteBuffer], A]

  val HNil            = shapeless.HNil
  val Iso             = shapeless.Generic

  def Utf8Charset: Charset                                               = java.nio.charset.Charset forName "UTF-8"
  def utf8Bytes(s: String): Array[Byte]                                  = s getBytes Utf8Charset
  def uuid(s: String): UUID                                              = java.util.UUID fromString s
  def randomUuid(): UUID                                                 = java.util.UUID.randomUUID
  def randomInt(end: Int): Int                                           = scala.util.Random.nextInt(end)
  def ByteBufferWrap(xs: Array[Byte]): ByteBuffer                        = java.nio.ByteBuffer.wrap(xs)
  def ByteBufferWrap(xs: Array[Byte], offset: Int, len: Int): ByteBuffer = java.nio.ByteBuffer.wrap(xs, offset, len)
  def abort(msg: String): Nothing                                        = throw new RuntimeException(msg)
  def decimal(d: String): BigDecimal                                     = BigDecimal(d, java.math.MathContext.UNLIMITED)
  def lp[T](label: String): T => Unit                                    = (t: T) => println(label + ": " + t)
  def lpf[T](label: String)(f: T => Any): T => Unit                      = (t: T) => println(label + ": " + f(t))

  def doto[A](x: A)(f: A => Unit): A = { f(x) ; x }

  implicit def comparableOrder[A <: Comparable[A]] : ScalazOrder[A] =
    scalaz.Order.order[A]((x, y) => ScalazOrdering.fromInt(x compareTo y))

  @inline implicit def ValidationFlatMapRequested[E, A](d: scalaz.Validation[E, A]): scalaz.ValidationFlatMap[E, A] =
    scalaz.Validation.FlatMap.ValidationFlatMapRequested[E, A](d)

  def futureMonad(ec: ExecutionContext): Monad[Future] = implicitly

  implicit def bigDecimalOrder: scalaz.Order[blueeyes.BigDecimal] =
    scalaz.Order.order((x, y) => Ordering.fromInt(x compare y))

  implicit class ScalaSeqOps[A](xs: scala.collection.Seq[A]) {
    def sortMe(implicit z: scalaz.Order[A]): Vector[A] =
      xs sortWith ((a, b) => z.order(a, b) == Ordering.LT) toVector
  }
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

  implicit class QuasarAnyOps[A](private val x: A) extends AnyVal {
    def |>[B](f: A => B): B = f(x)
  }

  implicit class LazyMapValues[A, B](source: Map[A, B]) {
    def lazyMapValues[C](f: B => C): Map[A, C] = new LazyMap[A, B, C](source, f)
  }

  implicit def bitSetOps(bs: BitSet): BitSetUtil.BitSetOperations = new BitSetUtil.BitSetOperations(bs)
}
