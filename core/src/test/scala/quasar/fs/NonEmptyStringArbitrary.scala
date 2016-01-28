package quasar.fs

import quasar.Predef._

import org.scalacheck.{Arbitrary, Gen}

trait NonEmptyStringArbitrary {
  import Arbitraries.genOption

  implicit def nonEmptyStringArbitrary: Arbitrary[NonEmptyString] =
    Arbitrary(genOption(
      Arbitrary.arbitrary[String].filter(_.nonEmpty) map (NonEmptyString(_))
    ))
}

object NonEmptyStringArbitrary extends NonEmptyStringArbitrary
