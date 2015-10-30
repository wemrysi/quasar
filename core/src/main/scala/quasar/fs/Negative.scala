package quasar
package fs

import quasar.Predef._

final class Negative private (val value: Int)

object Negative {
  def apply(n: Int): Option[Negative] =
    Some(n).filter(_ < 0).map(new Negative(_))
}