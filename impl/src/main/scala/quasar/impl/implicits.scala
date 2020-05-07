/*
 * Copyright 2020 Precog Data
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

package quasar.impl

import quasar.api.datasource.DatasourceError.CreateError
import quasar.api.destination.DestinationError.{CreateError => DestCreateError}
import quasar.compile.SemanticErrors
import quasar.connector.ResourceError
import quasar.contrib.scalaz.MonadError_
import quasar.qscript.PlannerError
import quasar.impl.storage.StoreError
import quasar.sql.ParsingError

import argonaut.Json

object implicits {
  implicit def compilingMonadError[F[_]: MonadQuasarErr]: MonadError_[F, SemanticErrors] =
    MonadError_.facet[F](QuasarError.compiling)

  implicit def connectingMonadError[F[_]: MonadQuasarErr]: MonadError_[F, CreateError[Json]] =
    MonadError_.facet[F](QuasarError.connecting)

  implicit def evaluatingMonadError[F[_]: MonadQuasarErr]: MonadError_[F, ResourceError] =
    MonadError_.facet[F](QuasarError.evaluating)

  implicit def parsingMonadError[F[_]: MonadQuasarErr]: MonadError_[F, ParsingError] =
    MonadError_.facet[F](QuasarError.parsing)

  implicit def planningMonadError[F[_]: MonadQuasarErr]: MonadError_[F, PlannerError] =
    MonadError_.facet[F](QuasarError.planning)

  implicit def storingMonadError[F[_]: MonadQuasarErr]: MonadError_[F, StoreError] =
    MonadError_.facet[F](QuasarError.storing)

  implicit def pushingMonadError[F[_]: MonadQuasarErr]: MonadError_[F, DestCreateError[Json]] =
    MonadError_.facet[F](QuasarError.pushing)
}
