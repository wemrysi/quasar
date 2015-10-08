package quasar
package fs

import quasar.Predef._

import scalaz._
import scalaz.Tags.{Multiplication => Mult}

final class Natural private (val run: Long) {
  def plus(other: Natural): Natural =
    new Natural(run + other.run)

  def + (other: Natural): Natural =
    plus(other)

  def times(other: Natural): Natural =
    new Natural(run * other.run)

  def * (other: Natural): Natural =
    times(other)
}

object Natural {
  def apply(n: Long): Option[Natural] =
    Some(n).filter(_ > 0).map(new Natural(_))

  val zero: Natural = new Natural(0)

  val one: Natural = new Natural(1)

  def fromPositive(n: Positive): Natural =
    new Natural(n.run)

  implicit val naturalAddition: Monoid[Natural] =
    Monoid.instance(_ + _, zero)

  implicit val naturalMultiplication: Monoid[Natural @@ Mult] =
    Monoid.instance(
      (x, y) => Mult(Mult.unwrap(x) * Mult.unwrap(y)),
      Mult(one))
}
