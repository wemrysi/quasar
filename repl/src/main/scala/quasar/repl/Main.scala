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

package quasar
package repl

import slamdata.Predef._
import quasar.impl.external.ExternalConfig
import quasar.common.PhaseResults
import quasar.contrib.scalaz.{MonadError_, MonadTell_}
import quasar.run.{Quasar, QuasarError}

import scala.concurrent.ExecutionContext.Implicits.global

import cats.effect.IO
import fs2.{Stream, StreamApp}, StreamApp.ExitCode
import fs2.async.Ref
import scalaz._, Scalaz._
import shims._

object Main extends StreamApp[IO] {

  implicit val ignorePhaseResults: MonadTell_[IO, PhaseResults] =
    MonadTell_.ignore[IO, PhaseResults]

  implicit val ioQuasarError: MonadError_[IO, QuasarError] =
    MonadError_.facet[IO](QuasarError.throwableP)

  val quasarStream: Stream[IO, Quasar[IO, IO]] =
    for {
      basePath <- Paths.getBasePath[Stream[IO, ?]]
      dataDir = basePath.resolve(Paths.QuasarDataDirName)
      _ <- Paths.mkdirs[Stream[IO, ?]](dataDir)
      pluginDir = basePath.resolve(Paths.QuasarPluginsDirName)
      _ <- Paths.mkdirs[Stream[IO, ?]](pluginDir)
      q <- Quasar[IO](dataDir, ExternalConfig.PluginDirectory(pluginDir), global)
    } yield q

  def repl(q: Quasar[IO, IO]): IO[ExitCode] =
    for {
      ref <- Ref[IO, ReplState](ReplState.mk)
      repl <- Repl.mk[IO, IO](ref, q.dataSources, q.queryEvaluator)
      l <- repl.loop
    } yield l

  override def stream(args: List[String], requestShutdown: IO[Unit]): Stream[IO, ExitCode] =
    quasarStream >>= (q => Stream.eval(repl(q)))

}
