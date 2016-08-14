package com.precog

import blueeyes._
import scalaz._, Scalaz._, Ordering._

package object yggdrasil {
  // only usage of commons-primitives
  type ArrayIntList = org.apache.commons.collections.primitives.ArrayIntList

  type Identity   = Long
  type Identities = Array[Identity]
  object Identities {
    val Empty = Vector.empty[Identity]
  }

  type SEvent = (Identities, SValue)

  object SEvent {
    @inline
    def apply(id: Identities, sv: SValue): SEvent = (id, sv)
  }

  def prefixIdentityOrdering(ids1: Identities, ids2: Identities, prefixLength: Int): ScalazOrdering = {
    var result: ScalazOrdering = EQ
    var i                      = 0
    while (i < prefixLength && (result eq EQ)) {
      result = longInstance.order(ids1(i), ids2(i))
      i += 1
    }

    result
  }

  def fullIdentityOrdering(ids1: Identities, ids2: Identities) = prefixIdentityOrdering(ids1, ids2, ids1.length min ids2.length)

  object IdentitiesOrder extends ScalazOrder[Identities] {
    def order(ids1: Identities, ids2: Identities) = fullIdentityOrdering(ids1, ids2)
  }

  def prefixIdentityOrder(prefixLength: Int): ScalazOrder[Identities] = {
    new ScalazOrder[Identities] {
      def order(ids1: Identities, ids2: Identities) = prefixIdentityOrdering(ids1, ids2, prefixLength)
    }
  }

  def tupledIdentitiesOrder[A](idOrder: ScalazOrder[Identities]): ScalazOrder[Identities -> A] =
    idOrder.contramap((_: (Identities, A))._1)

  def identityValueOrder[A](idOrder: ScalazOrder[Identities])(implicit ord: ScalazOrder[A]): ScalazOrder[Identities -> A] =
    new ScalazOrder[Identities -> A] {
      type IA = (Identities, A)
      def order(x: IA, y: IA): ScalazOrdering = {
        val idComp = idOrder.order(x._1, y._1)
        if (idComp == EQ) {
          ord.order(x._2, y._2)
        } else idComp
      }
    }

  def valueOrder[A](implicit ord: ScalazOrder[A]): ScalazOrder[Identities -> A] = new ScalazOrder[Identities -> A] {
    type IA = (Identities, A)
    def order(x: IA, y: IA): ScalazOrdering = {
      ord.order(x._2, y._2)
    }
  }
}
