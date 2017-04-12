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
import quasar.cli.Cmd, Cmd._
import quasar.config._
import quasar.console.{logErrors, stderr}
import quasar.db.StatefulTransactor
import quasar.fp._, ski.κ
import quasar.fp.free._
import quasar.main._, metastore._
import quasar.metastore.Schema

import argonaut.DecodeJson
import org.http4s.HttpService
import org.http4s.server._
import org.http4s.server.syntax._
import scalaz.{Failure => _, _}
import Scalaz._
import scalaz.concurrent.Task

object Server {
  type ServiceStarter = (Port => Task[Unit]) => HttpService

  final case class QuasarConfig(
    cmd: Cmd,
    staticContent: List[StaticContent],
    redirect: Option[String],
    port: Option[Port],
    configPath: Option[FsFile],
    openClient: Boolean)

  object QuasarConfig {
    def fromArgs(args: Array[String]): MainTask[QuasarConfig] =
      CliOptions.parser.parse(args.toSeq, CliOptions.default)
        .cata(_.point[MainTask], MainTask.raiseError("couldn't parse options"))
        .flatMap(fromCliOptions)

    def fromCliOptions(opts: CliOptions): MainTask[QuasarConfig] =
      (StaticContent.fromCliOptions("/files", opts) ⊛
        opts.config.fold(none[FsFile].point[MainTask])(cfg =>
          FsPath.parseSystemFile(cfg)
            .toRight(s"Invalid path to config file: $cfg")
            .map(some))) ((content, cfgPath) =>
        QuasarConfig(
          opts.cmd, content.toList, content.map(_.loc), opts.port, cfgPath, opts.openClient))
  }

  /** Attempts to load the specified config file or one found at any of the
    * default paths. If an error occurs, it is logged to STDERR and the
    * default configuration is returned.
    */
  def loadConfigFile[C: DecodeJson](configFile: Option[FsFile])(implicit cfgOps: ConfigOps[C]): Task[C] = {
    import ConfigError._
    configFile.fold(cfgOps.fromDefaultPaths)(cfgOps.fromFile).run flatMap {
      case \/-(c) => c.point[Task]

      case -\/(FileNotFound(f)) => for {
        codec <- FsPath.systemCodec
        fstr  =  FsPath.printFsPath(codec, f)
        _     <- stderr(s"Configuration file '$fstr' not found, using default configuration.")
        cfg   <- cfgOps.default
      } yield cfg

      case -\/(MalformedConfig(_, rsn)) =>
        stderr(s"Error in configuration file, using default configuration: $rsn") *>
        cfgOps.default
    }
  }

  def nonApiService(
    initialPort: Port,
    reload: Port => Task[Unit],
    staticContent: List[StaticContent],
    redirect: Option[String]
  ): HttpService = {
    val staticRoutes = staticContent map {
      case StaticContent(loc, path) => loc -> staticFileService(path)
    }

    Router(staticRoutes ::: List(
      "/"            -> redirectService(redirect getOrElse "/welcome"),
      "/server/info" -> info.service,
      "/server/port" -> control.service(initialPort, reload)
    ): _*)

  }

  def service(
    initialPort: Port,
    staticContent: List[StaticContent],
    redirect: Option[String],
    eval: CoreEff ~> ResponseOr
  ): ServiceStarter = {
    import RestApi._

    (reload: Port => Task[Unit]) =>
      finalizeServices(
        toHttpServices(liftMT[Task, ResponseT] :+: eval, coreServices[CoreEffIO]) ++
        additionalServices
      ) orElse nonApiService(initialPort, reload, staticContent, redirect)
  }

  def durableService(
    qConfig: QuasarConfig,
    port: Port,
    metastoreCtx: MetaStoreCtx
  ): (ServiceStarter, Task[Unit]) = {
    val f: QErrsTCnxIO ~> ResponseOr =
      liftMT[Task, ResponseT]                                                   :+:
      (liftMT[Task, ResponseT] compose metastoreCtx.metastore.transactor.trans) :+:
      qErrsToResponseOr

    val serv = service(
      port,
      qConfig.staticContent,
      qConfig.redirect,
      foldMapNT(f) compose metastoreCtx.interp)

    (serv, metastoreCtx.closeMnts *> metastoreCtx.metastore.shutdown)
  }

  def launchServer(
    args: Array[String],
    builder: (QuasarConfig, Port, MetaStoreCtx) => (ServiceStarter, Task[Unit])
  )(implicit
    configOps: ConfigOps[WebConfig]
  ): Task[Unit] = {
    def start(tx: StatefulTransactor, port: Port, qCfg: QuasarConfig) =
      for {
        _         <- verifySchema(Schema.schema, tx.transactor)
        msCtx     <- metastoreCtx(tx)
        (srvc, _) =  builder(qCfg, port, msCtx)
        _         <- Http4sUtils.startAndWait(port, srvc, qCfg.openClient).liftM[MainErrT]
      } yield ()

    logErrors(for {
      qCfg  <- QuasarConfig.fromArgs(args)
      wCfg  <- loadConfigFile[WebConfig](qCfg.configPath).liftM[MainErrT]
      msCfg <- wCfg.metastore.cata(Task.now, MetaStoreConfig.configOps.default).liftM[MainErrT]
      tx    <- metastoreTransactor(msCfg)
      _     <- EitherT((qCfg.cmd match {
                 case Start =>
                   start(tx, qCfg.port | wCfg.server.port, qCfg) *> tx.shutdown.liftM[MainErrT]
                 case InitUpdateMetaStore =>
                   initUpdateMigrate(Schema.schema, tx.transactor, qCfg.configPath)
               }).run.onFinish(κ(tx.shutdown)))
    } yield ())
  }

  def main(args: Array[String]): Unit =
    launchServer(args, durableService).unsafePerformSync
}
