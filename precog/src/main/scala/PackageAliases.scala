package quasar
package precog

import scala.collection.{ mutable => scm, immutable => sci }

trait PackageAliases {
  type RawBitSet = Array[Int]

  type =?>[-A, +B]          = scala.PartialFunction[A, B]
  type CTag[A]              = scala.reflect.ClassTag[A]
  type Try[+A]              = scala.util.Try[A]
  type jClass               = java.lang.Class[_]
  type jConcurrentMap[K, V] = java.util.concurrent.ConcurrentMap[K, V]
  type jPath                = java.nio.file.Path
  type scSet[A]             = scala.collection.Set[A]
  type scMap[K, V]          = scala.collection.Map[K, V]
  type sciQueue[+A]         = sci.Queue[A]
  type sciTreeMap[K, +V]    = scala.collection.immutable.TreeMap[K, V]
  type scmPriQueue[A]       = scm.PriorityQueue[A]
  type spec                 = scala.specialized
  type switch               = scala.annotation.switch
  type tailrec              = scala.annotation.tailrec

  // scala stdlib
  type ->[+A, +B]           = (A, B)
  type ArrayBuffer[A]       = scm.ArrayBuffer[A]
  type BigDecimal           = scala.math.BigDecimal
  type ListBuffer[A]        = scm.ListBuffer[A]
  type Regex                = scala.util.matching.Regex
  type ScalaMathOrdering[A] = scala.math.Ordering[A] // so many orders
  type scmMap[K, V]         = scm.Map[K, V]
  type scmSet[A]            = scm.Set[A]
  val ArrayBuffer           = scm.ArrayBuffer
  val BigDecimal            = scala.math.BigDecimal
  val ListBuffer            = scm.ListBuffer
  val scmMap                = scm.HashMap
  val scmSet                = scm.HashSet

  // java stdlib
  type AtomicInt            = java.util.concurrent.atomic.AtomicInteger
  type AtomicLong           = java.util.concurrent.atomic.AtomicLong
  type BufferedOutputStream = java.io.BufferedOutputStream
  type BufferedReader       = java.io.BufferedReader
  type ByteBuffer           = java.nio.ByteBuffer
  type CharBuffer           = java.nio.CharBuffer
  type Charset              = java.nio.charset.Charset
  type ExecutionContext     = java.util.concurrent.ExecutorService
  type File                 = java.io.File
  type FileInputStream      = java.io.FileInputStream
  type FileOutputStream     = java.io.FileOutputStream
  type IOException          = java.io.IOException
  type InputStream          = java.io.InputStream
  type InputStreamReader    = java.io.InputStreamReader
  type OutputStream         = java.io.OutputStream
  type OutputStreamWriter   = java.io.OutputStreamWriter
  type Properties           = java.util.Properties
  type PrintStream          = java.io.PrintStream
  type UUID                 = java.util.UUID
  type jMapEntry[K, V]      = java.util.Map.Entry[K, V]

  // other outside libs: scalaz, spire, shapeless, scodec
  type ByteVector     = scodec.bits.ByteVector
  type Future[+A]     = scalaz.concurrent.Future[A]
  type IO[A]          = scalaz.effect.IO[A]
  type Iso[T, L]      = shapeless.Generic.Aux[T, L]
  type ScalazOrder[A] = scalaz.Order[A]
  type ScalazOrdering = scalaz.Ordering
  type SpireOrder[A]  = spire.algebra.Order[A]
  type Task[+A]       = scalaz.concurrent.Task[A]
  type MoCo[M[+_]]    = scalaz.Monad[M] with scalaz.Comonad[M]
  type M[+A]          = scalaz.Need[A]

  val Future         = scalaz.concurrent.Future
  val ScalazOrder    = scalaz.Order
  val ScalazOrdering = scalaz.Ordering
  val IO             = scalaz.effect.IO
  val Try            = scala.util.Try
  val ScalaFailure   = scala.util.Failure
}
