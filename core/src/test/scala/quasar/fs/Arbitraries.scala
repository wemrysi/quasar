package quasar.fs

import quasar.Predef.Option

import org.scalacheck.Gen

trait Arbitraries extends
  NumericArbitrary with
  FileSystemTypeArbitrary with
  NonEmptyStringArbitrary with
  PathArbitrary with
  InMemoryArbitrary with
  MoveSemanticsArbitrary

object Arbitraries extends Arbitraries {
  // TODO: Replace with built-in version when we update scalacheck
  def genOption[A](gen: Gen[Option[A]]): Gen[A] =
    gen flatMap (_.fold(Gen.fail[A])(Gen.const))
}
