package object blueeyes {
  // scala stdlib
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
  type ActorRef         = akka.actor.ActorRef
  type ActorSystem      = akka.actor.ActorSystem
  type Duration         = akka.util.Duration
  type ExecutionContext = akka.dispatch.ExecutionContext
  type Future[+A]       = akka.dispatch.Future[A]
  type Promise[A]       = akka.dispatch.Promise[A]
  type Timeout          = akka.util.Timeout

  val ActorSystem      = akka.actor.ActorSystem
  val AkkaProps        = akka.actor.Props
  val Await            = akka.dispatch.Await
  val ExecutionContext = akka.dispatch.ExecutionContext
  val Future           = akka.dispatch.Future
  val Promise          = akka.dispatch.Promise
  val Timeout          = akka.util.Timeout
  val Duration         = akka.util.Duration

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
