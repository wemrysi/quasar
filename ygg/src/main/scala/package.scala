import scalaz._, Ordering._
import ygg.{ pkg => p }
import java.nio.file._

package object blueeyes extends p.PackageTime with p.PackageAliases with p.PackageMethods {
  type CBF[-From, -Elem, +To] = scala.collection.generic.CanBuildFrom[From, Elem, To]

  implicit class jPathOps(private val p: jPath) {
    def slurpBytes(): Array[Byte] = Files readAllBytes p
  }

  implicit class ScalaMapOps[K, V, CC[B] <: Traversable[B]](left: scMap[K, CC[V]]) {
    def cogroup[V1, That](right: scMap[K, CC[V1]])(implicit cbf: CBF[_, K -> Either3[V, CC[V] -> CC[V1], V1], That]): That = (
      new Cogrouped(left, right) build
    )
  }

  implicit def comparableOrder[A <: Comparable[A]]: Ord[A] =
    Ord.order[A]((x, y) => Cmp(x compareTo y))

  implicit def translateToScalaOrdering[A](implicit z: Ord[A]): scala.math.Ordering[A] = z.toScalaOrdering

  implicit class ScalazOrderOps[A](private val ord: Ord[A]) {
    def eqv(x: A, y: A): Boolean  = ord.order(x, y) == EQ
    def lt(x: A, y: A): Boolean   = ord.order(x, y) == LT
    def gt(x: A, y: A): Boolean   = ord.order(x, y) == GT
    def lte(x: A, y: A): Boolean  = !gt(x, y)
    def gte(x: A, y: A): Boolean  = !lt(x, y)
    def neqv(x: A, y: A): Boolean = !eqv(x, y)
  }

  implicit def ValidationFlatMapRequested[E, A](d: Validation[E, A]): ValidationFlatMap[E, A] =
    Validation.FlatMap.ValidationFlatMapRequested[E, A](d)

  implicit def bigDecimalOrder: Ord[BigDecimal] = Ord.order[BigDecimal]((x, y) => Cmp(x compare y))

  implicit class QuasarAnyOps[A](private val x: A) extends AnyVal {
    def |>[B](f: A => B): B       = f(x)
    def unsafeTap(f: A => Any): A = doto(x)(f)
  }

  implicit class LazyMapValues[A, B](source: Map[A, B]) {
    def lazyMapValues[C](f: B => C): Map[A, C] = new LazyMap[A, B, C](source, f)
  }

  implicit def bitSetOps(bs: BitSet): BitSetUtil.BitSetOperations = new BitSetUtil.BitSetOperations(bs)
}
