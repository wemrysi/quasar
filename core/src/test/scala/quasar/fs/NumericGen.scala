package quasar.fs

import org.scalacheck.Arbitrary
import quasar.Predef._

object NumericGen {
  // TODO: Replace use of get with `flatten` or `collect` once we upgrade scalacheck
  implicit val arbPositive = Arbitrary.apply(Arbitrary.arbitrary[Short].filter(_ > 0).map(long => Positive(long).get))
  implicit val arbNatural = Arbitrary.apply(Arbitrary.arbitrary[Short].filter(_ >= 0).map(long => Natural(long).get))
}
