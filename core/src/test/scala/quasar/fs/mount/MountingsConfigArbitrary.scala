package quasar.fs.mount

import quasar.Predef.Map
import quasar.fs.{APath, PathArbitrary}

// NB: Something in here is needed by scalacheck's Arbitrary defs
//     for scala collections.
import scala.Predef._

import org.scalacheck.{Arbitrary, Gen}, Arbitrary.arbitrary

trait MountingsConfigArbitrary {
  import MountConfigArbitrary._, PathArbitrary._

  implicit val mountingsConfigArbitrary: Arbitrary[MountingsConfig2] =
    Arbitrary(arbitrary[Map[APath, MountConfig2]] map (MountingsConfig2(_)))
}

object MountingsConfigArbitrary extends MountingsConfigArbitrary
