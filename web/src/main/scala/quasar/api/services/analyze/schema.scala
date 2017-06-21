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
import quasar.{Data, queryPlan}
import quasar.api._, ToApiError.ops._
import quasar.api.services._
import quasar.api.services.query._
import quasar.ejson.EJson
import quasar.ejson.implicits._
import quasar.fp.numeric._
import quasar.fs._
import quasar.main.analysis

import eu.timepit.refined.auto._
import matryoshka.data.Fix
import matryoshka.implicits._
import org.http4s._
import org.http4s.dsl._
import org.http4s.headers.Accept
import scalaz._, Scalaz._
import scalaz.concurrent.Task
import scalaz.stream.Process
import spire.std.double._

object schema {
  type J = Fix[EJson]

  val DefaultSampleSize: Positive = 1000L

  object ArrayMaxLength extends OptionalValidatingQueryParamDecoderMatcher[Positive]("arrayMaxLength")
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
        ArrayMaxLength(arrMax0)  +&
        MapMaxSize(mapMax0)      +&
        StringMaxLength(strMax0) +&
        UnionMaxSize(unionMax0)  =>

        val arrMax =
          valueOrInvalid("arrayMaxLength", arrMax0)
            .map(_ | analysis.CompressionSettings.DefaultArrayMaxLength)

        val mapMax =
          valueOrInvalid("mapMaxSize", mapMax0)
            .map(_ | analysis.CompressionSettings.DefaultMapMaxSize)

        val strMax =
          valueOrInvalid("stringMaxLength", strMax0)
            .map(_ | analysis.CompressionSettings.DefaultStringMaxLength)

        val unionMax =
          valueOrInvalid("unionMaxSize", unionMax0)
            .map(_ | analysis.CompressionSettings.DefaultUnionMaxSize)

        respond_((arrMax |@| mapMax |@| strMax |@| unionMax) { (amax, mmax, smax, umax) =>
          val compressionSettings = analysis.CompressionSettings(
            arrayMaxLength  = amax,
            mapMaxSize      = mmax,
            stringMaxLength = smax,
            unionMaxSize    = umax)

          sampling[S](req) map (p => formattedDataResponse(
            MessageFormat.fromAccept(req.headers.get(Accept)),
            p.pipe(analysis.extractSchema[J, Double](compressionSettings))
              .map(sst => sst.asEJson[J].cata(Data.fromEJson))))
        })
    }

  def sampling[S[_]](req: Request)(
    implicit Q: QueryFile.Ops[S]
  ): ApiError \/ Process[FileSystemErrT[Free[S, ?], ?], Data] =
    parsedQueryRequest(req, none, none) flatMap { case (blob, basePath, off, lim) =>
      queryPlan(blob, requestVars(req), basePath, off, lim)
        .run.value.bimap(
          _.toApiError,
          _.fold(
            Process.emitAll,
            lp => Q.evaluate(analysis.sampleOf(lp, DefaultSampleSize))))
        .map(_.translate(QueryFile.Transforms[Free[S, ?]].dropPhases))
    }
}
