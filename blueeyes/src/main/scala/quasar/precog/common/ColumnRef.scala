package quasar.precog
package common

import quasar.blueeyes._
import scalaz.syntax.semigroup._
import scalaz.syntax.order._

case class ColumnRef(selector: CPath, ctype: CType)

object ColumnRef {
  def identity(ctype: CType) = ColumnRef(CPath.Identity, ctype)

  implicit object order extends ScalazOrder[ColumnRef] {
    def order(r1: ColumnRef, r2: ColumnRef): ScalazOrdering = {
      (r1.selector ?|? r2.selector) |+| (r1.ctype ?|? r2.ctype)
    }
  }

  implicit val ordering: ScalaMathOrdering[ColumnRef] = order.toScalaOrdering
}
