/*
 * Copyright 2014–2017 SlamData Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package quasar.api.services.analyze

import slamdata.Predef._
import quasar.{Data, DataArbitrary, DataCodec}
import quasar.api._, ApiErrorEntityDecoder._, PathUtils._
import quasar.api.matchers._
import quasar.contrib.matryoshka.arbitrary._
import quasar.contrib.pathy._
import quasar.ejson.EJson
import quasar.ejson.implicits._
import quasar.fp._
import quasar.fp.free._
import quasar.fp.numeric._
import quasar.fs._, InMemory.InMemState
import quasar.main.analysis

import argonaut._, Argonaut._, JsonScalaz._
import eu.timepit.refined.auto._
import eu.timepit.refined.scalacheck.numeric._
import matryoshka._
import matryoshka.data.Fix
import matryoshka.implicits._
import org.http4s._
import org.http4s.argonaut._
import org.http4s.dsl._
import org.scalacheck._
import org.specs2.specification.core.Fragments
import pathy.argonaut.PosixCodecJson._
import pathy.scalacheck.PathyArbitrary._
import scalaz._, Scalaz._
import scalaz.concurrent.Task
import scalaz.stream.{process1, Process}
import spire.std.double._

final class SchemaServiceSpec extends quasar.Qspec with FileSystemFixture with Http4s {
  import SchemaServiceSpec._
  import DataArbitrary._

  type J = Fix[EJson]

  def stateForFile(file: AFile, contents: Vector[Data]) =
    InMemState.empty.copy(queryResps = Map(
      analysis.sampleQuery(file, schema.DefaultSampleSize) -> contents
    ))

  def nonPos(i: Int): Int =
    if (i > 0) -i else i

  def sstResponse(dataset: Vector[Data], cfg: analysis.CompressionSettings): Json =
    Process.emitAll(dataset)
      .pipe(analysis.extractSchema[J, Double](cfg))
      .map(sst => DataCodec.Precise.encode(sst.asEJson[J].cata[Data](Data.fromEJson)))
      .pipe(process1.stripNone)
      .toVector.headOption.getOrElse(Json.jNull)

  case class SmallPositive(p: Positive)

  implicit val smallPositiveArbitrary: Arbitrary[SmallPositive] =
    Arbitrary(chooseRefinedNum(1L: Positive, 10L: Positive) map (SmallPositive(_)))

  "respond with bad request when" >> {
    "no path given" >> {
      service(InMemState.empty)(Request())
        .flatMap(_.as[ApiError])
        .map(_ must beApiErrorWithMessage(
          BadRequest withReason "File path expected.",
          "path" := "/"))
        .unsafePerformSync
    }

    "directory path given" >> prop { dir: ADir =>
      service(InMemState.empty)(Request(uri = pathUri(dir)))
        .flatMap(_.as[ApiError])
        .map(_ must beApiErrorWithMessage(
          BadRequest withReason "File path expected.",
          "path" := dir))
        .unsafePerformSync
    }

    addFragments(Fragments(List("arrayMaxLength", "mapMaxSize", "stringMaxLength", "unionMaxSize") map { param =>
      s"non-positive $param given" >> prop { (file: AFile, i: Int) =>
        val ruri = pathUri(file) +? (param, nonPos(i).toString)

        service(InMemState.empty)(Request(uri = ruri))
          .flatMap(_.as[ApiError])
          .map(_ must beApiErrorWithMessage(
            BadRequest withReason "Invalid query parameter."))
          .unsafePerformSync
      }
    } : _*))
  }

  "successful response" >> {
    def shouldSucceed(file: AFile, x: Data, y: Data, cfg: analysis.CompressionSettings, req: Request) = {
      val dataset = Vector.fill(5)(x) ++ Vector.fill(5)(y)

      service(stateForFile(file, dataset))(req)
        .flatMap(_.as[Json])
        .map(_ must_= sstResponse(dataset, cfg))
        .unsafePerformSync
    }

    "includes the sst for the dataset as JSON-encoded EJson" >> prop { (file: AFile, x: Data, y: Data) =>
      shouldSucceed(file, x, y,
        analysis.CompressionSettings.Default,
        Request(uri = pathUri(file)))
    }

    "applies arrayMaxLength when specified" >> prop { (file: AFile, x: Data, y: Data, len: SmallPositive) =>
      val cfg = analysis.CompressionSettings.Default.copy(arrayMaxLength = len.p)
      val req = Request(uri = pathUri(file) +? ("arrayMaxLength", len.p.shows))
      shouldSucceed(file, x, y, cfg, req)
    }

    "applies mapMaxSize when specified" >> prop { (file: AFile, x: Data, y: Data, size: SmallPositive) =>
      val cfg = analysis.CompressionSettings.Default.copy(mapMaxSize = size.p)
      val req = Request(uri = pathUri(file) +? ("mapMaxSize", size.p.shows))
      shouldSucceed(file, x, y, cfg, req)
    }

    "applies stringMaxLength when specified" >> prop { (file: AFile, x: Data, y: Data, len: SmallPositive) =>
      val cfg = analysis.CompressionSettings.Default.copy(stringMaxLength = len.p)
      val req = Request(uri = pathUri(file) +? ("stringMaxLength", len.p.shows))
      shouldSucceed(file, x, y, cfg, req)
    }

    "applies unionMaxSize when specified" >> prop { (file: AFile, x: Data, y: Data, size: SmallPositive) =>
      val cfg = analysis.CompressionSettings.Default.copy(unionMaxSize = size.p)
      val req = Request(uri = pathUri(file) +? ("unionMaxSize", size.p.shows))
      shouldSucceed(file, x, y, cfg, req)
    }

    "has an empty body when file exists but is empty" >> prop { file: AFile =>
      service(stateForFile(file, Vector()))(Request(uri = pathUri(file)))
        .flatMap(_.as[String])
        .map(_ must beEmpty)
        .unsafePerformSync
    }
  }
}

object SchemaServiceSpec {
  type SchemaEff[A] = (QueryFile :\: FileSystemFailure :/: Task)#M[A]

  def runQueryFile(mem: InMemState): Task[QueryFile ~> ResponseOr] =
    InMemory.runStatefully(mem) map { eval =>
      liftMT[Task, ResponseT] compose eval compose InMemory.queryFile
    }

  def service(mem: InMemState): HttpService =
    Kleisli(req => runQueryFile(mem) flatMap { runQF =>
      schema.service[SchemaEff].toHttpService(
        runQF                              :+:
        failureResponseOr[FileSystemError] :+:
        liftMT[Task, ResponseT]
      )(req)
    })
}
