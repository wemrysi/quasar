package quasar
package fs

import quasar.Predef._

final class NonEmptyString private (val value: String)

object NonEmptyString {
  def apply(n: String): Option[NonEmptyString] =
    Some(n).filter(_.nonEmpty).map(new NonEmptyString(_))
}
