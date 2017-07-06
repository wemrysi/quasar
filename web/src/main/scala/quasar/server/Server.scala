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

package quasar.server

import slamdata.Predef._
import quasar.api.services._
import quasar.api.{redirectService, staticFileService, ResponseOr, ResponseT}
import quasar.cli.Cmd
import quasar.config._
import quasar.console.logErrors
import quasar.contrib.scopt._
import quasar.fp._
import quasar.fp.free._
import quasar.main._
import quasar.server.Http4sUtils.openBrowser

import org.http4s.HttpService
import org.http4s.server._
import org.http4s.server.syntax._
import scalaz.{Failure => _, _}
import Scalaz._
import scalaz.concurrent.Task

object Server {
  type ServiceStarter = (Int => Task[Unit]) => HttpService

  final case class WebCmdLineConfig(
    cmd: Cmd,
    staticContent: List[StaticContent],
    redirect: Option[String],
    port: Option[Int],
    configPath: Option[FsFile],
    openClient: Boolean) {
    def toCmdLineConfig: CmdLineConfig = CmdLineConfig(configPath, cmd)
  }

  object WebCmdLineConfig {
    def fromArgs(args: Vector[String]): MainTask[WebCmdLineConfig] =
      CliOptions.parser.safeParse(args, CliOptions.default)
        .flatMap(fromCliOptions(_))

    def fromCliOptions(opts: CliOptions): MainTask[WebCmdLineConfig] =
      (StaticContent.fromCliOptions("/files", opts) ⊛
        opts.config.fold(none[FsFile].point[MainTask])(cfg =>
          FsPath.parseSystemFile(cfg)
            .toRight(s"Invalid path to config file: $cfg")
            .map(some))) ((content, cfgPath) =>
        WebCmdLineConfig(
          opts.cmd, content.toList, content.map(_.loc), opts.port, cfgPath, opts.openClient))
  }

  def nonApiService(
    defaultPort: Int,
    reload: Int => Task[Unit],
    staticContent: List[StaticContent],
    redirect: Option[String]
  ): HttpService = {
    val staticRoutes = staticContent map {
      case StaticContent(loc, path) => loc -> staticFileService(path)
    }

    Router(staticRoutes ::: List(
      "/"            -> redirectService(redirect getOrElse "/welcome"),
      "/server/info" -> info.service,
      "/server/port" -> control.service(defaultPort, reload)
    ): _*)

  }

  def serviceStarter(
    defaultPort: Int,
    staticContent: List[StaticContent],
    redirect: Option[String],
    eval: CoreEff ~> QErrs_TaskM
  ): ServiceStarter = {
    import RestApi._

    val f: QErrs_Task ~> ResponseOr =
      liftMT[Task, ResponseT] :+:
        qErrsToResponseOr

    (reload: Int => Task[Unit]) =>
      finalizeServices(
        toHttpServices(liftMT[Task, ResponseT] :+: (foldMapNT(f) compose eval), coreServices[CoreEffIO]) ++
        additionalServices
      ) orElse nonApiService(defaultPort, reload, staticContent, redirect)
  }

  /**
    * Start the Quasar server and shutdown once the command line user presses "Enter"
    */
  def startServerUntilUserInput(quasarInter: CoreEff ~> QErrs_TaskM,
                            port: Int,
                  staticContent: List[StaticContent],
                  redirect: Option[String]
  ): Task[Unit] = {
    val starter = serviceStarter(defaultPort = port, staticContent, redirect, quasarInter)
    PortChangingServer.start(initialPort = port, starter).flatMap(_.shutdownOnUserInput)
  }

  /**
    * Start Quasar server
    * @return A `Task` that can be used to shutdown the server
    */
  def startServer(quasarInter: CoreEff ~> QErrs_TaskM,
                  port: Int,
                  staticContent: List[StaticContent],
                  redirect: Option[String]
  ): Task[Task[Unit]] = {
    val starter = serviceStarter(defaultPort = port, staticContent, redirect, quasarInter)
    PortChangingServer.start(initialPort = port, starter).map(_.shutdown)
  }

  def safeMain(args: Vector[String]): Task[Unit] =
    logErrors(for {
      webCmdLineCfg <- WebCmdLineConfig.fromArgs(args)
      _ <- initMetaStoreOrStart[WebConfig](
             webCmdLineCfg.toCmdLineConfig,
             (wCfg, quasarInter) => {
               val port = webCmdLineCfg.port | wCfg.server.port
               (startServerUntilUserInput(quasarInter, port, webCmdLineCfg.staticContent, webCmdLineCfg.redirect) <*
                openBrowser(port).whenM(webCmdLineCfg.openClient)).liftM[MainErrT]
             })
    } yield ())

  def main(args: Array[String]): Unit =
    safeMain(args.toVector).unsafePerformSync
}
