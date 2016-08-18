package quasar

import blueeyes._
import scalaz._, Scalaz._, Ordering._

package object ygg {
  type Identity   = Long
  type Identities = Array[Identity]

  def prefixIdentityOrdering(ids1: Identities, ids2: Identities, prefixLength: Int): Cmp = {
    var i = 0
    while (i < prefixLength) {
      longInstance.order(ids1(i), ids2(i)) match {
        case EQ  => i += 1
        case cmp => return cmp
      }
    }
    EQ
  }

  object IdentitiesOrder extends Ord[Identities] {
    def order(ids1: Identities, ids2: Identities): Cmp = fullIdentityOrdering(ids1, ids2)
  }

  def fullIdentityOrdering(ids1: Identities, ids2: Identities): Cmp =
    prefixIdentityOrdering(ids1, ids2, ids1.length min ids2.length)

  def prefixIdentityOrder(prefixLength: Int): Ord[Identities] =
    Ord order ((ids1, ids2) => prefixIdentityOrdering(ids1, ids2, prefixLength))

  def identityValueOrder[A](idOrder: Ord[Identities])(implicit ord: Ord[A]): Ord[Identities -> A] =
    Ord order ((x, y) => idOrder.order(x._1, y._1) |+| (x._2 ?|? y._2))

  def tupledIdentitiesOrder[A](ord: Ord[Identities]): Ord[Identities -> A] = ord contramap (_._1)
  def valueOrder[A](ord: Ord[A]): Ord[Identities -> A]                     = ord contramap (_._2)
}
