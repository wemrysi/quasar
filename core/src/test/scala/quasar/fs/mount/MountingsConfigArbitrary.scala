package quasar.fs.mount

import quasar.Predef.Map
import quasar.fs.{ADir, AFile}

// NB: Something in here is needed by scalacheck's Arbitrary defs
//     for scala collections.
import scala.Predef._
import scala.util.Either

import org.scalacheck.{Arbitrary, Gen}, Arbitrary.arbitrary
import pathy.scalacheck.PathyArbitrary._

trait MountingsConfigArbitrary {
  import MountConfigArbitrary._

  implicit val mountingsConfigArbitrary: Arbitrary[MountingsConfig2] =
    Arbitrary(arbitrary[Map[Either[ADir, AFile], MountConfig2]] map (m =>
      MountingsConfig2(m.map { case (p, c) => (p.merge, c) })
    ))
}

object MountingsConfigArbitrary extends MountingsConfigArbitrary
