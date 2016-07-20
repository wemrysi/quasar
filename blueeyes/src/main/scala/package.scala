package object blueeyes extends blueeyesstubs.Platform {
  // scala stdlib
  type spec           = scala.specialized
  type tailrec        = scala.annotation.tailrec
  type switch         = scala.annotation.switch
  type ArrayBuffer[A] = scala.collection.mutable.ArrayBuffer[A]
  type ListBuffer[A]  = scala.collection.mutable.ListBuffer[A]
  val ArrayBuffer     = scala.collection.mutable.ArrayBuffer
  val ListBuffer      = scala.collection.mutable.ListBuffer
  type CTag[A]        = scala.reflect.ClassTag[A]

  // java stdlib
  type BufferedOutputStream = java.io.BufferedOutputStream
  type BufferedReader       = java.io.BufferedReader
  type ByteBuffer           = java.nio.ByteBuffer
  type File                 = java.io.File
  type FileInputStream      = java.io.FileInputStream
  type FileOutputStream     = java.io.FileOutputStream
  type IOException          = java.io.IOException
  type InputStream          = java.io.InputStream
  type InputStreamReader    = java.io.InputStreamReader
  type OutputStream         = java.io.OutputStream
  type OutputStreamWriter   = java.io.OutputStreamWriter
  type PrintStream          = java.io.PrintStream
  type CharBuffer           = java.nio.CharBuffer

  // precog
  type ProducerId = Int
  type SequenceId = Int

  // so many orders
  type ScalazOrder[A]       = scalaz.Order[A]
  type ScalazOrdering       = scalaz.Ordering
  val ScalazOrder           = scalaz.Order
  val ScalazOrdering        = scalaz.Ordering
  type ScalaMathOrdering[A] = scala.math.Ordering[A]
  type SpireOrder[A]        = spire.algebra.Order[A]

  // joda
  type DateTime = org.joda.time.DateTime
  type Instant  = org.joda.time.Instant
  type Period   = org.joda.time.Period

  // shapeless
  val Iso        = shapeless.Generic
  type Iso[T, L] = shapeless.Generic.Aux[T, L]
  val HNil       = shapeless.HNil

  // Can't overload in package objects in scala 2.9!
  def ByteBufferWrap(xs: Array[Byte]): ByteBuffer                         = java.nio.ByteBuffer.wrap(xs)
  def ByteBufferWrap2(xs: Array[Byte], offset: Int, len: Int): ByteBuffer = java.nio.ByteBuffer.wrap(xs, offset, len)
  def abort(msg: String): Nothing                                         = throw new RuntimeException(msg)
  def decimal(d: String): BigDecimal                                      = BigDecimal(d, java.math.MathContext.UNLIMITED)
  def lp[T](label: String): T => Unit                                     = (t: T) => println(label + ": " + t)
  def lpf[T](label: String)(f: T => Any): T => Unit                       = (t: T) => println(label + ": " + f(t))

  implicit def K[A](a: A): util.K[A]                          = new util.K(a)
  implicit def OptStr(s: Option[String]): util.OptStr         = new util.OptStr(s)
  implicit def IOClose[T <: java.io.Closeable]: util.Close[T] = new util.Close[T] { def close(a: T): Unit = a.close() }

  implicit def comparableOrder[A <: Comparable[A]] : ScalazOrder[A] =
    scalaz.Order.order[A]((x, y) => ScalazOrdering.fromInt(x compareTo y))

  @inline implicit def ValidationFlatMapRequested[E, A](d: scalaz.Validation[E, A]): scalaz.ValidationFlatMap[E, A] =
    scalaz.Validation.FlatMap.ValidationFlatMapRequested[E, A](d)

  type ->[+A, +B] = (A, B)
}

package blueeyes {
  final class FreshAtomicIdSource {
    private val source = new java.util.concurrent.atomic.AtomicLong
    def nextId() = source.getAndIncrement
    def nextIdBlock(n: Int): Long = {
      var nextId = source.get()
      while (!source.compareAndSet(nextId, nextId + n)) {
        nextId = source.get()
      }
      nextId
    }
  }

  object yggConfig {
    val idSource = new FreshAtomicIdSource

    def hashJoins = true

    def sortBufferSize    = 1000

    def maxSliceSize: Int = 10

    // This is a slice size that we'd like our slices to be at least as large as.
    def minIdealSliceSize: Int = maxSliceSize / 4

    // This is what we consider a "small" slice. This may affect points where
    // we take proactive measures to prevent problems caused by small slices.
    def smallSliceSize: Int = 3

    def maxSaneCrossSize: Long = 2400000000L // 2.4 billion
  }

  package util {
    trait Close[A] {
      def close(a: A): Unit
    }

    case class K[A](a: A) {
      def ->-(f: A => Any): A = { f(a); a }
    }

    case class OptStr(s: Option[String]) {
      def str(f: String => String) = s.map(f).getOrElse("")
    }
  }
}
