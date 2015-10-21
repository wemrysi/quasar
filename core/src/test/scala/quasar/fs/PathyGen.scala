package quasar
package fs

import quasar.Predef._

import scalaz.syntax.functor._
import scalaz.std.list._
import org.scalacheck._
import pathy.Path._

object PathyGen {

  implicit val arbitraryFile: Arbitrary[RelFile[Sandboxed]] =
    Arbitrary(Gen.resize(10, genFile))

  implicit val arbitraryDir: Arbitrary[RelDir[Sandboxed]] =
    Arbitrary(Gen.resize(10, genDir))

  def genFile: Gen[RelFile[Sandboxed]] =
    for {
      d <- genDir
      s <- genSegment
    } yield d </> file(s)

  def genDir: Gen[RelDir[Sandboxed]] =
    Gen.frequency(
      (  1, Gen.const(currentDir[Sandboxed])),
      (100, Gen.nonEmptyListOf(genSegment)
        .map(_.foldLeft(currentDir)((d, s) => d </> dir(s)))))

  // TODO: Are these special characters MongoDB-specific?
  def genSegment: Gen[String] =
    Gen.nonEmptyListOf(Gen.frequency(
      (100, Arbitrary.arbitrary[Char]) ::
      "$./\\_~ *+-".toList.map(Gen.const).strengthL(10): _*))
      .map(_.mkString)
}
