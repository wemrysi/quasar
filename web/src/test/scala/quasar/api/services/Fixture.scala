package quasar
package api
package services

import Predef._
import argonaut.{JsonObject, JsonNumber, Json, Argonaut}
import jawn.{FContext, Facade}
import org.http4s.{MediaType, Charset, EntityEncoder}
import org.http4s.headers.`Content-Type`

import pathy.Path._
import pathy.Path
import pathy.scalacheck.{RelFileOf, AbsDirOf, AbsFileOf}

import quasar.Data
import quasar.api.JsonFormat.{SingleArray, LineDelimited}
import quasar.api.JsonPrecision.{Precise, Readable}
import quasar.api.MessageFormat.JsonContentType
import quasar.fs.InMemory.InMemState
import quasar.fs.RPath
import quasar.DataGen._
import pathy.scalacheck.PathOf._

import org.scalacheck.{Arbitrary, Gen}

import scala.collection.mutable
import scalaz._, Scalaz._
import scalaz.scalacheck.ScalazArbitrary._
import scalaz.scalacheck.ScalaCheckBinding._

object Fixture {

  import posixCodec.printPath

  case class AlphaCharacters(value: String)

  object AlphaCharacters {
    implicit val arb: Arbitrary[AlphaCharacters] = Arbitrary(Gen.alphaStr.filter(_.nonEmpty).map(AlphaCharacters(_)))
    implicit val show: Show[AlphaCharacters] = Show.shows(_.value)
  }

  case class SingleFileFileSystem(fileOfCharacters: AbsFileOf[AlphaCharacters], contents: Vector[Data]) {
    def file = fileOfCharacters.path
    def path = printPath(file)
    def state = InMemState fromFiles Map(file -> contents)
    def parent = fileParent(file)
    def filename = fileName(file)
  }

  def segAt[B,T,S](index: Int, path: Path[B,T,S]): Option[RPath] = {
    scala.Predef.require(index >= 0)
    val list = pathy.Path.flatten(none,none,none,dir(_).some,file(_).some,path).toIList.unite
    list.drop(index).headOption
  }

  case class NonEmptyDir(
    dirOfCharacters: AbsDirOf[AlphaCharacters],
    filesInDir: NonEmptyList[(RelFileOf[AlphaCharacters], Vector[Data])]
  ) {
    def dir = dirOfCharacters.path
    def state = {
      val fileMapping = filesInDir.map{ case (relFile,data) => (dir </> relFile.path, data)}
      InMemState fromFiles fileMapping.toList.toMap
    }
    def relFiles = filesInDir.unzip._1.map(_.path)
    def ls = relFiles.map(segAt(0,_)).list.flatten.toSet.toList.sortBy((path: RPath) => printPath(path))
  }

  implicit val arbSingleFileFileSystem: Arbitrary[SingleFileFileSystem] = Arbitrary(
    (Arbitrary.arbitrary[AbsFileOf[AlphaCharacters]] |@|
     Arbitrary.arbitrary[Vector[Data]])(SingleFileFileSystem.apply))

  implicit val arbNonEmptyDir: Arbitrary[NonEmptyDir] = Arbitrary(
    (Arbitrary.arbitrary[AbsDirOf[AlphaCharacters]] |@|
     Arbitrary.arbitrary[NonEmptyList[(RelFileOf[AlphaCharacters], Vector[Data])]])(NonEmptyDir.apply))

  val jsonReadableLine = JsonContentType(Readable,LineDelimited)
  val jsonPreciseLine = JsonContentType(Precise,LineDelimited)
  val jsonReadableArray = JsonContentType(Readable,SingleArray)
  val jsonPreciseArray = JsonContentType(Precise,SingleArray)

  // See: https://github.com/non/jawn/pull/43
  implicit val bugFreeArgonautFacade: Facade[Json] =
    new Facade[Json] {
      def jnull() = Json.jNull
      def jfalse() = Json.jFalse
      def jtrue() = Json.jTrue
      def jnum(s: String) = Json.jNumber(JsonNumber.unsafeDecimal(s))
      def jint(s: String) = Json.jNumber(JsonNumber.unsafeDecimal(s))
      def jstring(s: String) = Json.jString(s)

      def singleContext() = new FContext[Json] {
        var value: Json = null
        def add(s: String) = { value = jstring(s) }
        def add(v: Json) = { value = v }
        def finish: Json = value
        def isObj: Boolean = false
      }

      def arrayContext() = new FContext[Json] {
        val vs = mutable.ListBuffer.empty[Json]
        def add(s: String) = { vs += jstring(s); () }
        def add(v: Json) = { vs += v; () }
        def finish: Json = Json.jArray(vs.toList)
        def isObj: Boolean = false
      }

      def objectContext() = new FContext[Json] {
        var key: String = null
        var vs = JsonObject.empty
        def add(s: String): Unit =
          if (key == null) { key = s } else { vs = vs + (key, jstring(s)); key = null }
        def add(v: Json): Unit =
        { vs = vs + (key, v); key = null }
        def finish = Json.jObject(vs)
        def isObj = true
      }
    }

  // Remove once version 0.8.4 or higher of jawn is realeased.
  implicit val normalJsonBugFreeDecoder = org.http4s.jawn.jawnDecoder(bugFreeArgonautFacade)

  sealed trait JsonType

  case class PreciseJson(value: Json) extends JsonType
  object PreciseJson {
    implicit val entityEncoder: EntityEncoder[PreciseJson] =
      EntityEncoder.encodeBy(`Content-Type`(jsonPreciseArray.mediaType, Charset.`UTF-8`)) { pJson =>
        org.http4s.argonaut.jsonEncoder.toEntity(pJson.value)
      }
  }

  case class ReadableJson(value: Json) extends JsonType
  object ReadableJson {
    implicit val entityEncoder: EntityEncoder[ReadableJson] =
      EntityEncoder.encodeBy(`Content-Type`(jsonReadableArray.mediaType, Charset.`UTF-8`)) { rJson =>
        org.http4s.argonaut.jsonEncoder.toEntity(rJson.value)
      }
  }

  implicit val readableLineDelimitedJson: EntityEncoder[List[ReadableJson]] =
    EntityEncoder.stringEncoder.contramap[List[ReadableJson]] { rJsons =>
      rJsons.map(rJson => Argonaut.nospace.pretty(rJson.value)).mkString("\n")
    }.withContentType(`Content-Type`(jsonReadableLine.mediaType, Charset.`UTF-8`))

  implicit val preciseLineDelimitedJson: EntityEncoder[List[PreciseJson]] =
    EntityEncoder.stringEncoder.contramap[List[PreciseJson]] { pJsons =>
      pJsons.map(pJson => Argonaut.nospace.pretty(pJson.value)).mkString("\n")
    }.withContentType(`Content-Type`(jsonPreciseLine.mediaType, Charset.`UTF-8`))

  case class Csv(value: String)
  object Csv {
    implicit val entityEncoder: EntityEncoder[Csv] =
      EntityEncoder.encodeBy(`Content-Type`(MediaType.`text/csv`, Charset.`UTF-8`)) { csv =>
        EntityEncoder.stringEncoder(Charset.`UTF-8`).toEntity(csv.value)
      }
  }
}
