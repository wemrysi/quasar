package quasar.fs.mount

import quasar.Predef._

import org.scalacheck.{Arbitrary, Gen}

trait ConnectionUriArbitrary {
  implicit val connectionUriArbitrary: Arbitrary[ConnectionUri] =
    Arbitrary(for {
      scheme <- Gen.alphaStr
      rest   <- Gen.alphaStr
    } yield ConnectionUri(s"$scheme:$rest"))
}

object ConnectionUriArbitrary extends ConnectionUriArbitrary
