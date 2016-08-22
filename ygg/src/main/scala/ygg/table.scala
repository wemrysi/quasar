package ygg

import ygg.common._
import scalaz._, Scalaz._, Ordering._

package object table {
  type Identity   = Long
  type Identities = Array[Identity]
  type ColumnMap  = Map[ColumnRef, Column]

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

  val IdentitiesOrder: Ord[Identities] = Ord order fullIdentityOrdering

  def prefixIdentityOrder(prefixLength: Int): Ord[Identities] =
    Ord order (prefixIdentityOrdering(_, _, prefixLength))

  def identityValueOrder[A: Ord](idOrder: Ord[Identities]): Ord[Identities -> A] =
    Ord order ((x, y) => idOrder.order(x._1, y._1) |+| (x._2 ?|? y._2))

  def fullIdentityOrdering(ids1: Identities, ids2: Identities): Cmp =
    prefixIdentityOrder(ids1.length min ids2.length)(ids1, ids2)

  def tupledIdentitiesOrder[A](ord: Ord[Identities]): Ord[Identities -> A] = ord contramap (_._1)
  def valueOrder[A](ord: Ord[A]): Ord[Identities -> A]                     = ord contramap (_._2)
}
