package quasar.fs

import org.scalacheck.{Gen, Arbitrary}
import pathy.scalacheck.PathyArbitrary._

trait PathArbitrary {

  implicit val arbitraryAPath: Arbitrary[APath] =
    Arbitrary(Gen.oneOf(Arbitrary.arbitrary[AFile], Arbitrary.arbitrary[ADir]))

  implicit val arbitraryRPath: Arbitrary[RPath] =
    Arbitrary(Gen.oneOf(Arbitrary.arbitrary[RFile], Arbitrary.arbitrary[RDir]))

}

object PathArbitrary extends PathArbitrary
