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

import slamdata.Predef.{-> => _, _}
import quasar.api._
import quasar.api.services._
import quasar.ejson.EJson
import quasar.ejson.implicits._
import quasar.fp.numeric._
import quasar.fs._
import quasar.main.analysis

import eu.timepit.refined.auto._
import matryoshka.data.Fix
import org.http4s.dsl._
import scalaz._, Scalaz._
import scalaz.concurrent.Task
import spire.std.double._

object schema {
  type J = Fix[EJson]

  val DefaultSampleSize: Positive = 1000L

  object MapMaxSize extends OptionalValidatingQueryParamDecoderMatcher[Positive]("mapMaxSize")
  object StringMaxLength extends OptionalValidatingQueryParamDecoderMatcher[Positive]("stringMaxLength")
  object UnionMaxSize extends OptionalValidatingQueryParamDecoderMatcher[Positive]("unionMaxSize")

  def service[S[_]](
    implicit
    Q : QueryFile.Ops[S],
    S0: Task :<: S,
    S1: FileSystemFailure :<: S
  ): QHttpService[S] =
    QHttpService {
      case req @ GET -> _        :?
        MapMaxSize(mapMax0)      +&
        StringMaxLength(strMax0) +&
        UnionMaxSize(unionMax0)  =>

        val mapMax =
          valueOrInvalid("mapMaxSize", mapMax0)
            .map(_ | analysis.CompressionSettings.DefaultMapMaxSize)

        val strMax =
          valueOrInvalid("stringMaxLength", strMax0)
            .map(_ | analysis.CompressionSettings.DefaultStringMaxLength)

        val unionMax =
          valueOrInvalid("unionMaxSize", unionMax0)
            .map(_ | analysis.CompressionSettings.DefaultUnionMaxSize)

        respond_((decodedFile(req.uri.path) |@| mapMax |@| strMax |@| unionMax) { (file, mmax, smax, umax) =>
          val compressionSettings = analysis.CompressionSettings(
            mapMaxSize      = mmax,
            stringMaxLength = smax,
            unionMaxSize    = umax)

          firstEJsonResponse(
            analysis.sample[S](file, DefaultSampleSize)
              .pipe(analysis.extractSchema[J, Double](compressionSettings))
              .map(_.asEJson[J]))
        })
    }
}
