package ygg

import ygg.common._
import scalaz._, Scalaz._, Ordering._

package object table {
  type Identity    = Long
  type Identities  = Array[Identity]
  type ColumnMap   = Map[ColumnRef, Column]

  def prefixIdentityOrdering(ids1: Identities, ids2: Identities, prefixLength: Int): Cmp = {
    0 until prefixLength foreach { i =>
      ids1(i) ?|? ids2(i) match {
        case EQ  => ()
        case cmp => return cmp
      }
    }
    EQ
  }

  val IdentitiesOrder: Ord[Identities] = Ord order fullIdentityOrdering

  def prefixIdentityOrder(prefixLength: Int): Ord[Identities] =
    Ord order (prefixIdentityOrdering(_, _, prefixLength))

  def identityValueOrder[A: Ord](idOrder: Ord[Identities]): Ord[Identities -> A] =
    Ord order ((x, y) => idOrder.order(x._1, y._1) |+| (x._2 ?|? y._2))

  def fullIdentityOrdering(ids1: Identities, ids2: Identities): Cmp =
    prefixIdentityOrder(ids1.length min ids2.length)(ids1, ids2)

  def tupledIdentitiesOrder[A](ord: Ord[Identities]): Ord[Identities -> A] = ord contramap (_._1)
  def valueOrder[A](ord: Ord[A]): Ord[Identities -> A]                     = ord contramap (_._2)

  implicit def liftCF1(f: CF1): CF1Like = new CF1Like {
    def compose(f1: CF1) = f compose f1
    def andThen(f1: CF1) = f andThen f1
  }

  implicit def liftCF2(f: CF2) = new CF2Like {
    def applyl(cv: CValue) = CF1("builtin::liftF2::applyl")(f(Column const cv, _))
    def applyr(cv: CValue) = CF1("builtin::liftF2::applyl")(f(_, Column const cv))
    def andThen(f1: CF1)   = CF2("builtin::liftF2::andThen")((c1, c2) => f(c1, c2) flatMap f1.apply)
  }
}
