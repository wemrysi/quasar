import scalaz._, Ordering._

import quasar.{ precog => p }

package object blueeyes extends p.PackageTime with p.PackageAliases with p.PackageMethods {
  // Temporary
  type BitSet             = quasar.precog.BitSet
  type JobId              = String
  type ByteBufferPoolS[A] = State[com.precog.util.ByteBufferPool -> List[ByteBuffer], A]

  val HNil = shapeless.HNil
  val Iso  = shapeless.Generic

  implicit def implicitScalaMapOps[A, B, CC[B] <: Traversable[B]](x: scMap[A, CC[B]]): ScalaMapOps[A, B, CC] =
    new ScalaMapOps(x)

  implicit def comparableOrder[A <: Comparable[A]] : ScalazOrder[A] =
    scalaz.Order.order[A]((x, y) => ScalazOrdering.fromInt(x compareTo y))

  implicit class ScalazOrderOps[A](private val ord: ScalazOrder[A]) {
    def eqv(x: A, y: A): Boolean  = ord.order(x, y) == EQ
    def lt(x: A, y: A): Boolean   = ord.order(x, y) == LT
    def gt(x: A, y: A): Boolean   = ord.order(x, y) == GT
    def lte(x: A, y: A): Boolean  = !gt(x, y)
    def gte(x: A, y: A): Boolean  = !lt(x, y)
    def neqv(x: A, y: A): Boolean = !eqv(x, y)
  }

  implicit def ValidationFlatMapRequested[E, A](d: Validation[E, A]): ValidationFlatMap[E, A] =
    Validation.FlatMap.ValidationFlatMapRequested[E, A](d)

  implicit def bigDecimalOrder: scalaz.Order[blueeyes.BigDecimal] =
    scalaz.Order.order((x, y) => Ordering.fromInt(x compare y))

  implicit class ScalaSeqOps[A](xs: scala.collection.Seq[A]) {
    def sortMe(implicit z: scalaz.Order[A]): Vector[A] =
      xs sortWith ((a, b) => z.order(a, b) == Ordering.LT) toVector
  }

  implicit class QuasarAnyOps[A](private val x: A) extends AnyVal {
    def |>[B](f: A => B): B = f(x)
    def unsafeTap(f: A => Any): A = doto(x)(f)
  }

  implicit class LazyMapValues[A, B](source: Map[A, B]) {
    def lazyMapValues[C](f: B => C): Map[A, C] = new LazyMap[A, B, C](source, f)
  }

  implicit def bitSetOps(bs: BitSet): BitSetUtil.BitSetOperations = new BitSetUtil.BitSetOperations(bs)
}
