package quasar.config

import quasar.Predef._
import quasar.fp._
import quasar.fs.{Path => QPath}
import quasar.fs.mount.{ConnectionUri, MountConfig2, MountingsConfig2}
import quasar.physical.mongodb.fs.MongoDBFsType

import scala.util.Properties

import argonaut._, Argonaut._
import org.specs2.mutable._
import org.specs2.scalaz._
import pathy._, Path._
import scalaz._, concurrent.Task, Scalaz._

abstract class ConfigSpec[Config: CodecJson] extends Specification with DisjunctionMatchers {
  import FsPath._, ConfigError._

  sequential

  def configOps: ConfigOps[Config]
  def sampleConfig(uri: ConnectionUri): Config

  val host = "mongodb://foo:bar@mongo.example.com:12345"
  val dbName = "quasar-01"
  val testUri = ConnectionUri(s"$host/$dbName")
  val TestConfig = sampleConfig(testUri)

  def testConfigFile: Task[FsPath.Aux[Rel, File, Sandboxed]] =
    Task.delay(scala.util.Random.nextInt.toString)
      .map(i => Uniform(currentDir </> file(s"test-config-${i}.json")))

  def withTestConfigFile[A](f: FsPath.Aux[Rel, File, Sandboxed] => Task[A]): Task[A] = {
    import java.nio.file._

    def deleteIfExists(fp: FsPath[File, Sandboxed]): Task[Unit] =
      systemCodec
        .map(c => printFsPath(c, fp))
        .flatMap(s => Task.delay(Files.deleteIfExists(Paths.get(s))))
        .void

    testConfigFile >>= (fp => f(fp) onFinish κ(deleteIfExists(fp)))
  }

  val ConfigStr =
    s"""{
      |  "mountings": {
      |    "/": {
      |      "mongodb": {
      |        "connectionUri": "${testUri.value}"
      |      }
      |    }
      |  }
      |}""".stripMargin

  "fromString" should {
    "parse valid config" in {
      configOps.fromString(ConfigStr) must beRightDisjunction(TestConfig)
    }
  }

  "toString" should {
    "render same config" in {
      configOps.asString(TestConfig) must_== ConfigStr
    }
  }

  "toFile" should {
    "create loadable config" in {
      withTestConfigFile(fp =>
        configOps.toFile(TestConfig, Some(fp)) *>
        configOps.fromFile(fp).run
      ).run must beRightDisjunction(TestConfig)
    }
  }

  "fromFileOrDefaultPaths" should {
    "result in error when file not found" in {
      val (p, r) =
        withTestConfigFile(fp =>
          configOps.fromFileOrDefaultPaths(Some(fp)).run.map((fp, _))
        ).run
      r must beLeftDisjunction(fileNotFound(p))
    }
  }
}
