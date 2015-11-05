package quasar
package fs

import quasar.Predef._

import scalaz.Equal
import scalaz.std.anyVal._

final class Negative private (val value: Int) {
  override def equals(other: scala.Any) = other match {
    case Negative(a) => value == a
    case _ => false
  }
}

object Negative {
  def apply(n: Int): Option[Negative] =
    Some(n).filter(_ < 0).map(new Negative(_))
  def unapply(n: Negative) = Some(n.value)

  implicit val equal: Equal[Negative] = Equal.equalA
}