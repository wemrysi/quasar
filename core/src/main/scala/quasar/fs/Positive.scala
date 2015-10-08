package quasar
package fs

import quasar.Predef._

import scalaz._
import scalaz.Tags.{Multiplication => Mult}

final class Positive private (val run: Long) {
  def plus(other: Positive): Positive =
    new Positive(run + other.run)

  def + (other: Positive): Positive =
    plus(other)

  def times(other: Positive): Positive =
    new Positive(run * other.run)

  def * (other: Positive): Positive =
    times(other)
}

object Positive {
  def apply(n: Long): Option[Positive] =
    Some(n).filter(_ > 1).map(new Positive(_))

  val one: Positive = new Positive(1)

  implicit val positiveSemigroup: Semigroup[Positive] =
    Semigroup.instance(_ + _)

  implicit val positiveMultiplication: Monoid[Positive @@ Mult] =
    Monoid.instance(
      (x, y) => Mult(Mult.unwrap(x) * Mult.unwrap(y)),
      Mult(one))
}
