package quasar.config

import quasar.fs.mount.{MountingsConfig2, MountingsConfigArbitrary}

import org.scalacheck.Arbitrary

trait CoreConfigArbitrary {
  import MountingsConfigArbitrary._

  implicit val coreConfigArbitrary: Arbitrary[CoreConfig] =
    Arbitrary(Arbitrary.arbitrary[MountingsConfig2] map (CoreConfig(_)))
}

object CoreConfigArbitrary extends CoreConfigArbitrary
