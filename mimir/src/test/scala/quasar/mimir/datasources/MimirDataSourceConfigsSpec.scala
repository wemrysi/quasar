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

package quasar.mimir.datasources

import quasar.Disposable
import quasar.contrib.cats.effect.liftio._
import quasar.contrib.nio.file
import quasar.impl.datasources.DataSourceConfigsSpec
import quasar.mimir.{Precog, MimirCake}, MimirCake.Cake
import quasar.mimir.RValueGenerator._
import quasar.precog.common.RValue
import quasar.yggdrasil.vfs.ResourceError

import java.nio.file.Files
import java.util.UUID

import cats.effect.IO
import pathy.Path.{file => afile, _}
import scalaz.{~>, EitherT, Id}, Id.Id
import scalaz.std.string._
import scalaz.syntax.foldable._
import shims._

final class MimirDataSourceConfigsSpec extends DataSourceConfigsSpec[EitherT[IO, ResourceError, ?], RValue] {
  import MimirDataSourceConfigsSpec._

  lazy val P: Disposable[IO, Cake] = {
    val init = for {
      tmpPath <- IO(Files.createTempDirectory("mimir-datasource-configs-spec-"))
      cake <- Precog(tmpPath.toFile)
    } yield {
      cake
        .map(p => p: Cake)
        .onDispose(file.deleteRecursively[IO](tmpPath))
    }

    init.unsafeRunSync()
  }

  val dataSourceConfigs =
    EitherT.rightU[ResourceError](IO(UUID.randomUUID)) map { uuid =>
      MimirDataSourceConfigs[EitherT[IO, ResourceError, ?]](
        P.unsafeValue,
        rootDir </> dir("configs") </> afile(uuid.toString))
    }

  val run: EitherT[IO, ResourceError, ?] ~> Id =
    λ[EitherT[IO, ResourceError, ?] ~> Id](
      _.run
        .flatMap(_.fold(
          e => IO.raiseError(new ResourceErrorException(e)),
          IO.pure(_)))
        .unsafeRunSync())

  step(P.dispose.unsafeRunSync())
}

object MimirDataSourceConfigsSpec {
  final class ResourceErrorException(error: ResourceError)
    extends Exception(error.messages.intercalate(", "))
}
