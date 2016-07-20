import scalaz._

package object blueeyes {
  // scala stdlib
  type ->[+A, +B]           = (A, B)
  type ArrayBuffer[A]       = scala.collection.mutable.ArrayBuffer[A]
  type BigDecimal           = scala.math.BigDecimal
  type CTag[A]              = scala.reflect.ClassTag[A]
  type Duration             = scala.concurrent.duration.Duration
  type ListBuffer[A]        = scala.collection.mutable.ListBuffer[A]
  type ScalaMathOrdering[A] = scala.math.Ordering[A] // so many orders
  type spec                 = scala.specialized
  type switch               = scala.annotation.switch
  type tailrec              = scala.annotation.tailrec
  val ArrayBuffer           = scala.collection.mutable.ArrayBuffer
  val BigDecimal            = scala.math.BigDecimal
  val Duration              = scala.concurrent.duration.Duration
  val ListBuffer            = scala.collection.mutable.ListBuffer

  // java stdlib
  type BufferedOutputStream = java.io.BufferedOutputStream
  type BufferedReader       = java.io.BufferedReader
  type ByteBuffer           = java.nio.ByteBuffer
  type CharBuffer           = java.nio.CharBuffer
  type ExecutionContext     = java.util.concurrent.ExecutorService
  type File                 = java.io.File
  type FileInputStream      = java.io.FileInputStream
  type FileOutputStream     = java.io.FileOutputStream
  type IOException          = java.io.IOException
  type InputStream          = java.io.InputStream
  type InputStreamReader    = java.io.InputStreamReader
  type LocalDateTime        = java.time.LocalDateTime
  type OutputStream         = java.io.OutputStream
  type OutputStreamWriter   = java.io.OutputStreamWriter
  type PrintStream          = java.io.PrintStream

  // other outside libs: scalaz, spire, shapeless, joda
  type DateTime       = org.joda.time.DateTime
  type Future[+A]     = scalaz.concurrent.Future[A]
  type Instant        = org.joda.time.Instant
  type Iso[T, L]      = shapeless.Generic.Aux[T, L]
  type Period         = org.joda.time.Period
  type ScalazOrder[A] = scalaz.Order[A]
  type ScalazOrdering = scalaz.Ordering
  type SpireOrder[A]  = spire.algebra.Order[A]
  type Task[+A]       = scalaz.concurrent.Task[A]
  val Future          = scalaz.concurrent.Future
  val HNil            = shapeless.HNil
  val Iso             = shapeless.Generic
  val ScalazOrder     = scalaz.Order
  val ScalazOrdering  = scalaz.Ordering

  // Temporary
  type BitSet             = com.precog.util.BitSet
  type RawBitSet          = Array[Int]
  val RawBitSet           = com.precog.util.RawBitSet
  type ByteBufferPoolS[A] = State[com.precog.util.ByteBufferPool -> List[ByteBuffer], A]

  // Can't overload in package objects in scala 2.9!
  def ByteBufferWrap(xs: Array[Byte]): ByteBuffer                         = java.nio.ByteBuffer.wrap(xs)
  def ByteBufferWrap2(xs: Array[Byte], offset: Int, len: Int): ByteBuffer = java.nio.ByteBuffer.wrap(xs, offset, len)
  def abort(msg: String): Nothing                                         = throw new RuntimeException(msg)
  def decimal(d: String): BigDecimal                                      = BigDecimal(d, java.math.MathContext.UNLIMITED)
  def lp[T](label: String): T => Unit                                     = (t: T) => println(label + ": " + t)
  def lpf[T](label: String)(f: T => Any): T => Unit                       = (t: T) => println(label + ": " + f(t))

  implicit def comparableOrder[A <: Comparable[A]] : ScalazOrder[A] =
    scalaz.Order.order[A]((x, y) => ScalazOrdering.fromInt(x compareTo y))

  @inline implicit def ValidationFlatMapRequested[E, A](d: scalaz.Validation[E, A]): scalaz.ValidationFlatMap[E, A] =
    scalaz.Validation.FlatMap.ValidationFlatMapRequested[E, A](d)

  def localDateTime(s: String): LocalDateTime          = java.time.LocalDateTime parse s
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

  implicit class LazyMapValues[A, B](source: Map[A, B]) {
    def lazyMapValues[C](f: B => C): Map[A, C] = new LazyMap[A, B, C](source, f)
  }
}

package blueeyes {
  /**
    * This object contains some methods to do faster iteration over primitives.
    *
    * In particular it doesn't box, allocate intermediate objects, or use a (slow)
    * shared interface with scala collections.
    */
  object Loop {
    @tailrec
    def range(i: Int, limit: Int)(f: Int => Unit) {
      if (i < limit) {
        f(i)
        range(i + 1, limit)(f)
      }
    }

    final def forall[@specialized A](as: Array[A])(f: A => Boolean): Boolean = {
      @tailrec def loop(i: Int): Boolean = i == as.length || f(as(i)) && loop(i + 1)

      loop(0)
    }
  }

  final class LazyMap[A, B, C](source: Map[A, B], f: B => C) extends Map[A, C] {
    private val m = new java.util.concurrent.ConcurrentHashMap[A, C]()

    def iterator: Iterator[(A, C)] = source.keysIterator map { a =>
      (a, apply(a))
    }

    def get(a: A): Option[C] = m get a match {
      case null =>
        source get a map { b =>
          val c = f(b)
          m.putIfAbsent(a, c)
          c
        }
      case x => Some(x)
    }

    def +[C1 >: C](kv: (A, C1)): Map[A, C1] = iterator.toMap + kv
    def -(a: A): Map[A, C]                  = iterator.toMap - a
  }

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

    def hashJoins         = true
    def sortBufferSize    = 1000
    def maxSliceSize: Int = 10

    // This is a slice size that we'd like our slices to be at least as large as.
    def minIdealSliceSize: Int = maxSliceSize / 4

    // This is what we consider a "small" slice. This may affect points where
    // we take proactive measures to prevent problems caused by small slices.
    def smallSliceSize: Int = 3

    def maxSaneCrossSize: Long = 2400000000L // 2.4 billion
  }
}
