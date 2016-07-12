package object blueeyes {
  // scala stdlib
  type CTag[A]        = scala.reflect.ClassTag[A]
  type spec           = scala.specialized
  type tailrec        = scala.annotation.tailrec
  type switch         = scala.annotation.switch
  type ArrayBuffer[A] = scala.collection.mutable.ArrayBuffer[A]
  type ListBuffer[A]  = scala.collection.mutable.ListBuffer[A]
  val ArrayBuffer     = scala.collection.mutable.ArrayBuffer
  val ListBuffer      = scala.collection.mutable.ListBuffer

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

  // akka in 2.9
  type Duration         = scala.concurrent.duration.Duration
  type ExecutionContext = scala.concurrent.ExecutionContext
  type Future[+A]       = scala.concurrent.Future[A]
  type Promise[A]       = scala.concurrent.Promise[A]

  val Await            = scala.concurrent.Await
  val ExecutionContext = scala.concurrent.ExecutionContext
  val Future           = scala.concurrent.Future
  val Promise          = scala.concurrent.Promise
  val Duration         = scala.concurrent.duration.Duration

  // precog
  type ProducerId = Int
  type SequenceId = Int

  // scalaz
  type ScalazOrder[A] = scalaz.Order[A]
  type ScalazOrdering = scalaz.Ordering
  val ScalazOrder    = scalaz.Order
  val ScalazOrdering = scalaz.Ordering
  type ScalaMathOrdering[A] = scala.math.Ordering[A]

  // joda
  type DateTime = org.joda.time.DateTime
  type Instant  = org.joda.time.Instant
  type Period   = org.joda.time.Period

  // shapeless
  val Iso  = shapeless.Iso
  val HNil = shapeless.HNil

  // configrity
  type Configuration = org.streum.configrity.Configuration

  // Can't overload in package objects in scala 2.9!
  def ByteBufferWrap(xs: Array[Byte]): ByteBuffer                         = java.nio.ByteBuffer.wrap(xs)
  def ByteBufferWrap2(xs: Array[Byte], offset: Int, len: Int): ByteBuffer = java.nio.ByteBuffer.wrap(xs, offset, len)
  def abort(msg: String): Nothing                                         = throw new RuntimeException(msg)
  def decimal(d: String): BigDecimal                                      = BigDecimal(d, java.math.MathContext.UNLIMITED)
  def lp[T](label: String): T => Unit                                     = (t: T) => println(label + ": " + t)
  def lpf[T](label: String)(f: T => Any): T => Unit                       = (t: T) => println(label + ": " + f(t))
  def doto[A](value: A)(f: A => Unit): A                                  = { f(value) ; value }

  implicit def K[A](a: A): util.K[A]                          = new util.K(a)
  implicit def OptStr(s: Option[String]): util.OptStr         = new util.OptStr(s)
  implicit def IOClose[T <: java.io.Closeable]: util.Close[T] = new util.Close[T] { def close(a: T): Unit = a.close() }
}

package blueeyes {
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
