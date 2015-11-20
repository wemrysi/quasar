package quasar
package api
package services

import Predef._
import argonaut.{JsonObject, JsonNumber, Json, Argonaut}
import jawn.{FContext, Facade}
import org.http4s.{MediaType, Charset, EntityEncoder}
import org.http4s.headers.`Content-Type`

import pathy.Path._

import quasar.Data
import quasar.api.JsonFormat.{SingleArray, LineDelimited}
import quasar.api.JsonPrecision.{Precise, Readable}
import quasar.api.MessageFormat.JsonContentType
import quasar.fs.InMemory.InMemState
import quasar.fs.NonEmptyString
import quasar.fs.SimplePathyGen._
import quasar.DataGen._
import quasar.fs.NumericGen._

import org.scalacheck.Arbitrary

import scala.collection.mutable
import scalaz._, Scalaz._
import scalaz.scalacheck.ScalazArbitrary._
import scalaz.scalacheck.ScalaCheckBinding._

object Fixture {

  import posixCodec.printPath

  case class SingleFileFileSystem(file: AbsFile[Sandboxed], contents: Vector[Data]) {
    def path = printPath(file)
    def state = InMemState fromFiles Map(file -> contents)
    // A better Pathy signature here would avoid this get which should never fail
    def parent = parentDir(file).get
    def filename = fileName(file)
  }

  case class NonEmptyDir(dir: AbsDir[Sandboxed], filesInDir: NonEmptyList[(NonEmptyString, Vector[Data])]) {
    def state = {
      val fileMapping = filesInDir.map{ case (fileName,data) => (dir </> file(fileName.value), data)}
      InMemState fromFiles fileMapping.toList.toMap
    }
  }

  implicit val arbSingleFileFileSystem: Arbitrary[SingleFileFileSystem] = Arbitrary(
    (Arbitrary.arbitrary[AbsFile[Sandboxed]] |@| Arbitrary.arbitrary[Vector[Data]])(SingleFileFileSystem.apply))

  implicit val arbNonEmptyDir: Arbitrary[NonEmptyDir] = Arbitrary(
    (Arbitrary.arbitrary[AbsDir[Sandboxed]] |@| Arbitrary.arbitrary[NonEmptyList[(NonEmptyString, Vector[Data])]])(NonEmptyDir.apply))

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
