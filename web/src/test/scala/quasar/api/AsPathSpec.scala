package quasar.api

import quasar.Predef._

import org.specs2.ScalaCheck
import org.specs2.mutable.Specification
import pathy.Path._
import pathy.scalacheck.PathyArbitrary._
import org.http4s.dsl.{Path => HttpPath}
import quasar.fs.{ADir, AFile}

class AsPathSpec extends Specification with ScalaCheck {
  "AsPathPath" should {
    "decode any Path we can throw at it" >> {
      "AbsFile" ! prop { file : AFile =>
        val httpPath = HttpPath(posixCodec.printPath(file))
        AsFilePath.unapply(httpPath) must_== Some(file)
      }
      "AbsDir" ! prop { dir : ADir =>
        val httpPath = HttpPath(posixCodec.printPath(dir))
        AsDirPath.unapply(httpPath) must_== Some(dir)
      }
    }
  }
}
