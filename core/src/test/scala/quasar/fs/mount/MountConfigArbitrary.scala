package quasar.fs.mount

import quasar.Predef.{ArrowAssoc, Map}
import quasar.{Variables, VarName, VarValue}
import quasar.fp.prism._
import quasar.fs._, FileSystemTypeArbitrary._
import quasar.sql._, ExprArbitrary._

import org.scalacheck.{Arbitrary, Gen}

trait MountConfigArbitrary {
  import MountConfig2._, ConnectionUriArbitrary._

  implicit val mountConfigArbitrary: Arbitrary[MountConfig2] =
    Arbitrary(Gen.oneOf(genFileSystemConfig, genViewConfig))

  private def genFileSystemConfig: Gen[MountConfig2] =
    for {
      typ <- Arbitrary.arbitrary[FileSystemType]
      uri <- Arbitrary.arbitrary[ConnectionUri]
    } yield fileSystemConfig(typ, uri)

  private def genViewConfig: Gen[MountConfig2] = {
    val noVars = Variables.empty
    val aVar   = Variables(Map(VarName("foo") -> VarValue("bar")))
    for {
      expr <- Arbitrary.arbitrary[Expr]
      vars <- Gen.oneOf(noVars, aVar)
    } yield viewConfig(expr, vars)
  }
}

object MountConfigArbitrary extends MountConfigArbitrary
