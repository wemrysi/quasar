package quasar.fs

import org.scalacheck.{Arbitrary, Gen}
import quasar.Predef._

object NumericGen {
  implicit val arbPositive  = Arbitrary.apply(Gen.choose(1, Short.MaxValue).map(short => Positive(short).get))
  implicit val arbNatural   = Arbitrary.apply(Gen.choose(0, Short.MaxValue).map(long => Natural(long).get))
  implicit val arbNegative  = Arbitrary.apply(Gen.choose(Int.MinValue, -1).map(Negative(_).get))
}
