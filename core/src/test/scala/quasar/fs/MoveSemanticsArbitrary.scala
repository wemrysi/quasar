package quasar.fs

import org.scalacheck.{Arbitrary, Gen}
import ManageFile.MoveSemantics

trait MoveSemanticsArbitrary {
  implicit val arbitraryMoveSemantics: Arbitrary[MoveSemantics] =
    Arbitrary(Gen.oneOf(MoveSemantics.Overwrite, MoveSemantics.FailIfExists, MoveSemantics.FailIfMissing))
}
