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

package quasar.impl.datasource.local

import quasar.api.{ResourceDiscoverySpec, ResourceName, ResourcePath}
import LocalDataSourceSpec._

import java.nio.file.Paths
import scala.concurrent.ExecutionContext.Implicits.global

import cats.effect.IO
import fs2.Stream
import scalaz.{~>, Foldable, Id, Monoid}, Id.Id
import shims._

final class LocalDataSourceSpec
    extends ResourceDiscoverySpec[IO, Stream[IO, ?]] {

  val discovery =
    LocalDataSource[IO, IO](Paths.get("./it/src/main/resources/tests"), 1024, global)

  val nonExistentPath =
    ResourcePath.root() / ResourceName("non") / ResourceName("existent")

  val run =
    λ[IO ~> Id](_.unsafeRunSync)
}

object LocalDataSourceSpec {
  implicit val unsafeStreamFoldable: Foldable[Stream[IO, ?]] =
    new Foldable[Stream[IO, ?]] with Foldable.FromFoldMap[Stream[IO, ?]] {
      def foldMap[A, M](fa: Stream[IO, A])(f: A => M)(implicit M: Monoid[M]) =
        fa.compile.fold(M.zero)((m, a) => M.append(m, f(a))).unsafeRunSync
    }
}
