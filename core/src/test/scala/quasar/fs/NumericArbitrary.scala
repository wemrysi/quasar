package quasar.fs

import quasar.Predef._

import org.scalacheck.{Arbitrary, Gen}

trait NumericArbitrary {
  import Arbitraries.genOption

  implicit val positiveArbitrary: Arbitrary[Positive] =
    Arbitrary(genOption(Gen.choose(1, Short.MaxValue) map (Positive(_))))

  implicit val negativeArbitrary: Arbitrary[Negative] =
    Arbitrary(genOption(Gen.choose(Int.MinValue, -1) map (Negative(_))))

  implicit val naturalArbitrary: Arbitrary[Natural] =
    Arbitrary(genOption(Gen.choose(0, Short.MaxValue) map (Natural(_))))
}

object NumericArbitrary extends NumericArbitrary
