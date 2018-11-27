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
import quasar.concurrent.BlockingContext
import quasar.common.{PhaseResultCatsT, PhaseResultListen, PhaseResultTell}
import quasar.contrib.cats.writerT.{catsWriterTMonadListen_, catsWriterTMonadTell_}
import quasar.contrib.scalaz.MonadError_
import quasar.impl.DatasourceModule
import quasar.impl.external.{ExternalConfig, ExternalDatasources}
import quasar.impl.datasource.local.{LocalDatasourceModule, LocalParsedDatasourceModule}
import quasar.impl.external.ExternalConfig.PluginFiles
import quasar.impl.schema.SstEvalConfig
import quasar.mimir.Precog
import quasar.run.{MonadQuasarErr, Quasar, QuasarError}
import quasar.yggdrasil.vfs.contextShiftForS

import java.lang.Runtime
import java.nio.file.Path
import java.util.concurrent.Executors
import scala.concurrent.ExecutionContext

import cats.arrow.FunctionK
import cats.effect._
import cats.effect.concurrent.Ref
import eu.timepit.refined.auto._
import fs2.Stream
import optparse_applicative._
import optparse_applicative.types.{Failure, ParserPrefs, Success}
import scalaz._, Scalaz._
import shims._

object Main extends IOApp {

  final case class Args[F[_]](pluginFiles: F[Option[PluginFiles]])

  type IOT[A] = PhaseResultCatsT[IO, A]

  implicit val iotQuasarError: MonadError_[IOT, QuasarError] =
    MonadError_.facet[IOT](QuasarError.throwableP)

  implicit val iotTimer: Timer[IOT] = Timer.deriveWriterT

  def paths[F[_]](implicit F: Sync[F]): Stream[F, (Path, Path)] =
    for {
      basePath <- Stream.eval(Paths.getBasePath[F])
      dataDir = basePath.resolve(Paths.QuasarDataDirName)
      _ <- Stream.eval(Paths.mkdirs[F](dataDir))
      pluginDir = basePath.resolve(Paths.QuasarPluginsDirName)
      _ <- Stream.eval(Paths.mkdirs[F](pluginDir))
    } yield (dataDir, pluginDir)

  def quasarStream[F[_]: ConcurrentEffect: ContextShift: MonadQuasarErr: PhaseResultTell: Timer](
      pluginFiles: Option[PluginFiles],
      blockingPool: BlockingContext)
      : Stream[F, Quasar[F]] = {

    implicit val cpuEC =
      ExecutionContext.fromExecutor(
        Executors.newFixedThreadPool(
          Runtime.getRuntime.availableProcessors))

    for {
      (dataPath, pluginPath) <- paths[F]
      precog <- Precog.stream(dataPath.toFile, blockingPool).translate(λ[FunctionK[IO, F]](_.to[F]))
      evalCfg = SstEvalConfig(1000L, 2L, 250L)
      extCfg = pluginFiles.getOrElse(ExternalConfig.PluginDirectory(pluginPath))
      extMods <- ExternalDatasources[F](extCfg, blockingPool)
      mods =
        DatasourceModule.Lightweight(LocalParsedDatasourceModule) ::
        DatasourceModule.Lightweight(LocalDatasourceModule) ::
        extMods
      q <- Quasar[F](precog, mods, evalCfg)
    } yield q
  }

  def repl[F[_]: ConcurrentEffect: ContextShift: MonadQuasarErr: PhaseResultListen: PhaseResultTell: Timer](
      q: Quasar[F],
      blockingPool: BlockingContext)
      : F[ExitCode] =
    for {
      ref <- Ref.of[F, ReplState](ReplState.mk)
      repl <- Repl.mk[F](ref, q, blockingPool)
      l <- repl.loop
    } yield l

  override def run(args: List[String]): IO[ExitCode] = {
    val blockingPool = BlockingContext.cached("quasar-repl-blocking")

    def existingFile[F[_]: Sync](s: String) =
      for {
        p <- Paths.fromString(s)
        _ <- Paths.ensureFile(p)
      } yield p

    def pluginFilesParser[F[_]: Sync]: Parser[F[Option[PluginFiles]]] =
      many(strOption(short('P'), long("plugin"), metavar("FILE"), help("Plugin file to load (0 or more)"))).map(l =>
        if (l.isEmpty) none[PluginFiles].pure[F]
        else l.traverse(existingFile[F]).map(ps => Some(PluginFiles(ps))))

    val parser = pluginFilesParser[IO].map(Args(_))

    val prefs = ParserPrefs(multiSuffix = "", disambiguate = false, showHelpOnError = true, backtrack = true, columns = 80)

    execParserPure(prefs, info(parser <*> helper), args) match {
      case Success(opts) =>
        for {
          files <- opts.pluginFiles
          qs <- quasarStream[IOT](files, blockingPool)
            .evalMap(repl[IOT](_, blockingPool))
            .compile
            .last
            .run.map(_._2.getOrElse(ExitCode.Success))
        } yield qs

      case Failure(f) =>
        val (s, exitCode) = renderFailure(f, "repl")
        IO.delay(println(s)).as(ExitCode(exitCode.toInt))
    }

  }

}
