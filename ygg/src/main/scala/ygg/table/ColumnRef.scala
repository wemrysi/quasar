package ygg.table

import ygg.common._
import scalaz.Scalaz._

case class ColumnRef(selector: CPath, ctype: CType)

object ColumnRef {
  object id {
    def apply(ctype: CType): ColumnRef = ColumnRef(CPath.Identity, ctype)
    def unapply(x: ColumnRef): Option[CType] = x match {
      case ColumnRef(CPath.Identity, tp) => Some(tp)
      case _                             => None
    }
  }

  implicit val columnRefOrder: Ord[ColumnRef] = Ord.orderBy(r => r.selector -> r.ctype)
}
