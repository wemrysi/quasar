/*
 * Copyright 2014–2018 SlamData Inc.
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

package quasar.impl.datasource

import slamdata.Predef._

import quasar.api.datasource.DatasourceError.{
  InitializationError,
  MalformedConfiguration
}
import quasar.api.datasource.DatasourceType
import quasar.api.resource.ResourcePath
import quasar.contrib.scalaz.MonadError_
import quasar.fp.ski.ι

import java.nio.file.{Paths, Path => JPath}

import argonaut.{DecodeJson, Json}
import cats.effect.Sync
import eu.timepit.refined.auto._
import pathy.Path
import scalaz.{\/, EitherT}
import scalaz.syntax.applicative._
import scalaz.syntax.foldable._
import shims._

package object local {
  val LocalType = DatasourceType("local", 1L)
  val LocalParsedType = DatasourceType("local-parsed", 1L)

  def toNio[F[_]: Sync](root: JPath, rp: ResourcePath): F[JPath] =
    Path.flatten("", "", "", ι, ι, rp.toPath).foldLeftM(root) { (p, n) =>
      if (n.isEmpty) p.point[F]
      else MonadError_[F, Throwable].unattempt_(\/.fromTryCatchNonFatal(p.resolve(n)))
    }

  def attemptConfig[F[_]: Sync, A: DecodeJson](config: Json, errorPrefix: String)
      : EitherT[F, InitializationError[Json], A] =
    EitherT.fromEither(config.as[A].toEither.point[F])
      .leftMap[InitializationError[Json]] {
        case (s, _) =>
          MalformedConfiguration(LocalType, config, errorPrefix + s)
      }

  def validatePath[F[_]: Sync](path: String, config: Json, errorPrefix: String)
      : EitherT[F, InitializationError[Json], JPath] =
    EitherT.fromEither(Sync[F].attempt(Sync[F].delay(Paths.get(path))))
      .leftMap[InitializationError[Json]](
        t => MalformedConfiguration(LocalType, config, errorPrefix + t.getMessage))
}
