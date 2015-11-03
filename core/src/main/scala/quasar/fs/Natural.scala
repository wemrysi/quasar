package quasar
package fs

import quasar.Predef._

import scalaz._
import scalaz.std.anyVal._
import scalaz.Tags.{Multiplication => Mult}

final class Natural private[fs] (val value: Long) {
  def plus(other: Natural): Natural =
    new Natural(value + other.value)

  def + (other: Natural): Natural =
    plus(other)

  def times(other: Natural): Natural =
    new Natural(value * other.value)

  def * (other: Natural): Natural =
    times(other)

  def toInt: Int =
    value.toInt

  override def equals(other: scala.Any) = other match {
    case Natural(a) => value == a
    case _ => false
  }
}

object Natural {
  def apply(n: Long): Option[Natural] =
    Some(n).filter(_ >= 0).map(new Natural(_))

  def unapply(n: Natural): Option[Long] = Some(n.value)

  val _0: Natural = new Natural(0)
  val _1: Natural = new Natural(1)
  val _2: Natural = new Natural(2)
  val _3: Natural = new Natural(3)
  val _4: Natural = new Natural(4)
  val _5: Natural = new Natural(5)
  val _6: Natural = new Natural(6)
  val _7: Natural = new Natural(7)
  val _8: Natural = new Natural(8)
  val _9: Natural = new Natural(9)

  def fromPositive(n: Positive): Natural =
    new Natural(n.value)

  implicit val naturalAddition: Monoid[Natural] =
    Monoid.instance(_ + _, _0)

  implicit val naturalMultiplication: Monoid[Natural @@ Mult] =
    Monoid.instance(
      (x, y) => Mult(Mult.unwrap(x) * Mult.unwrap(y)),
      Mult(_1))

  implicit val equal: Equal[Natural] = Equal.equalBy(_.value)

  implicit val show: Show[Natural] = Show.shows(_.value.toString)
}
