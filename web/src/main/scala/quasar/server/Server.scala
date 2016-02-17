/*
 * Copyright 2014–2016 SlamData Inc.
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

import quasar.Predef._
import quasar.{Mounter => _, _}
import quasar.api._, Http4sUtils._
import quasar.api.services.RestApi
import quasar.console._
import quasar.config._
import quasar.effect._
import quasar.fp._
import quasar.fs._
import quasar.fs.mount._
import quasar.physical.mongodb._
import quasar.physical.mongodb.fs.mongoDbFileSystemDef

import java.io.File
import java.lang.System

import argonaut.{CodecJson, EncodeJson}
import monocle.Lens
import org.http4s
import org.http4s.server.{Server => Http4sServer, HttpService, Router}
import org.http4s.server.syntax._
import org.http4s.server.blaze.BlazeBuilder
import scalaz.{Failure => _, Lens => _, _}
import scalaz.syntax.monad._
import scalaz.syntax.show._
import scalaz.syntax.either._
import scalaz.syntax.foldable._
import scalaz.syntax.std.option._
import scalaz.std.option._
import scalaz.std.list._
import scalaz.concurrent.Task

object Server {
  import QueryFile.ResultHandle
  import FileSystemDef.DefinitionResult
  import hierarchical._
  import Mounting.PathTypeMismatch

  type Builders = List[(Int, BlazeBuilder)]
  type Servers = List[(Int, Http4sServer)]
  type ServersErrors = List[(Int, Throwable \/ Http4sServer)]
  type Services = List[(String, HttpService)]

  /** Effect for FileSystemDefs */
  type FsDefEff[A]  = Coproduct[WorkflowExecErrF, Task, A]
  type FsDefEffM[A] = Free[FsDefEff, A]

  // FsDefEffM \/ MonotonicSeqF \/ ViewStateF \/ MountedResultHF \/ HFSFailureF
  type FsEff0[A] = Coproduct[MountedResultHF, HFSFailureF, A]
  type FsEff1[A] = Coproduct[ViewStateF, FsEff0, A]
  type FsEff2[A] = Coproduct[MonotonicSeqF, FsEff1, A]
  type FsEff[A]  = Coproduct[FsDefEffM, FsEff2, A]
  type FsEffM[A] = Free[FsEff, A]

  // HFSFailureF \/ WorkflowExecErrF \/ Task
  type FsErrsIO0[A] = Coproduct[WorkflowExecErrF, Task, A]
  type FsErrsIO[A]  = Coproduct[HFSFailureF, FsErrsIO0, A]
  type FsErrsIOM[A] = Free[FsErrsIO, A]

  // NB: Will eventually be the lcd of all physical filesystems, or we limit
  //     to a fixed set of effects that filesystems must interpret into.
  val mongoDbFs: FileSystemDef[FsDefEffM] =
    mongoDbFileSystemDef[FsDefEff]

  val evalFsDefEff: FsDefEff ~> FsErrsIOM =
    free.interpret2[WorkflowExecErrF, Task, FsErrsIOM](
      injectFT[WorkflowExecErrF, FsErrsIO],
      injectFT[Task, FsErrsIO])

  def fsEffToFsErrsIOM(
    seqRef: TaskRef[Long],
    viewHandlesRef: TaskRef[ViewHandles],
    mntResRef: TaskRef[Map[ResultHandle, (ADir, ResultHandle)]]
  ): FsEff ~> FsErrsIOM = {
    def injTask[E[_]](f: E ~> Task): E ~> FsErrsIOM =
      injectFT[Task, FsErrsIO].compose[E](f)

    free.interpret5[FsDefEffM, MonotonicSeqF, ViewStateF, MountedResultHF, HFSFailureF, FsErrsIOM](
      hoistFree(evalFsDefEff),
      injTask[MonotonicSeqF](Coyoneda.liftTF(MonotonicSeq.fromTaskRef(seqRef))),
      injTask[ViewStateF](Coyoneda.liftTF[ViewState, Task](KeyValueStore.fromTaskRef(viewHandlesRef))),
      injTask[MountedResultHF](Coyoneda.liftTF[MountedResultH, Task](KeyValueStore.fromTaskRef(mntResRef))),
      injectFT[HFSFailureF, FsErrsIO])
  }

  val evalMounter = EvaluatorMounter[FsDefEffM, FsEff](mongoDbFs)
  import evalMounter.{EvalFSRef, EvalFSRefF}

  type MountedFs[A]  = AtomicRef[Mounts[DefinitionResult[FsDefEffM]], A]
  type MountedFsF[A] = Coyoneda[MountedFs, A]

  // EvalFSRefF \/ FsDefEffM \/ MountedFsF \/ MountedViewsF
  type EvalEff0[A] = Coproduct[MountedFsF, MountedViewsF, A]
  type EvalEff1[A] = Coproduct[FsDefEffM, EvalEff0, A]
  type EvalEff[A]  = Coproduct[EvalFSRefF, EvalEff1, A]
  type EvalEffM[A] = Free[EvalEff, A]

  def evalEffToFsErrsIOM(
    evalRef: TaskRef[FileSystem ~> FsEffM],
    mntsRef: TaskRef[Mounts[DefinitionResult[FsDefEffM]]],
    viewsRef: TaskRef[Views]
  ): EvalEff ~> FsErrsIOM = {
    def injTask[E[_]](f: E ~> Task): E ~> FsErrsIOM =
      injectFT[Task, FsErrsIO].compose[E](f)

    free.interpret4[EvalFSRefF, FsDefEffM, MountedFsF, MountedViewsF, FsErrsIOM](
      injTask[EvalFSRefF](Coyoneda.liftTF[EvalFSRef, Task](AtomicRef.fromTaskRef(evalRef))),
      hoistFree(evalFsDefEff),
      injTask[MountedFsF](Coyoneda.liftTF[MountedFs, Task](AtomicRef.fromTaskRef(mntsRef))),
      injTask[MountedViewsF](Coyoneda.liftTF[MountedViews, Task](AtomicRef.fromTaskRef(viewsRef))))
  }

  type MntEff[A]  = Coproduct[EvalEffM, MountConfigsF, A]
  type MntEffM[A] = Free[MntEff, A]

  // TODO: For some reason, scalac complains of a diverging implicits when
  //       attempting to resolve this.
  implicit val mntEffFunctor =
    Coproduct.coproductFunctor[EvalEffM, MountConfigsF](implicitly, implicitly)

  val mounter: Mounting ~> MntEffM =
    Mounter[EvalEffM, MntEff](
      evalMounter.mount[EvalEff](_),
      evalMounter.unmount[EvalEff](_))

  def evalFromRef[S[_]: Functor](
    ref: TaskRef[FileSystem ~> FsEffM],
    f: FsEff ~> Free[S, ?]
  )(implicit S: Task :<: S): FileSystem ~> Free[S, ?] = {
    type F[A] = Free[S, A]
    new (FileSystem ~> F) {
      def apply[A](fs: FileSystem[A]) =
        injectFT[Task, S].apply(ref.read.map(free.foldMapNT[FsEff, F](f) compose _))
          .flatMap(_.apply(fs))
    }
  }

  final case class QuasarConfig(staticContent: List[StaticContent],
                                redirect: Option[String],
                                port: Option[Int],
                                configPath: Option[FsFile],
                                openClient: Boolean)

  type ApiEff0[A] = Coproduct[MountingF, FileSystem, A]
  type ApiEff1[A] = Coproduct[FileSystemFailureF, ApiEff0, A]
  type ApiEff[A]  = Coproduct[Task, ApiEff1, A]
  type ApiEffM[A] = Free[ApiEff, A]

  type ConfigsIO[A]  = Coproduct[MountConfigsF, Task, A]
  type ConfigsIOM[A] = Free[ConfigsIO, A]

  // FileSystemFailureF \/ WorkflowExecErrF \/ HFSFailureF \/ ConfigsIO
  type CfgsErrsIO0[A] = Coproduct[HFSFailureF, ConfigsIO, A]
  type CfgsErrsIO1[A] = Coproduct[WorkflowExecErrF, CfgsErrsIO0, A]
  type CfgsErrsIO[A]  = Coproduct[FileSystemFailureF, CfgsErrsIO1, A]
  type CfgsErrsIOM[A] = Free[CfgsErrsIO, A]

  // TODO: Include failure effects in output coproduct
  val evalApi: Task[ApiEff ~> CfgsErrsIOM] =
    for {
      startSeq   <- Task.delay(scala.util.Random.nextInt.toLong)
      seqRef     <- TaskRef(startSeq)
      viewHRef   <- TaskRef[ViewHandles](Map())
      mntedRHRef <- TaskRef(Map[ResultHandle, (ADir, ResultHandle)]())
      evalFsRef  <- TaskRef(Empty.fileSystem[FsEffM])
      mntsRef    <- TaskRef(Mounts.empty[DefinitionResult[FsDefEffM]])
      viewsRef   <- TaskRef(Views.empty)
    } yield {
      val f: FsEff ~> FsErrsIOM = fsEffToFsErrsIOM(seqRef, viewHRef, mntedRHRef)
      val g: EvalEff ~> FsErrsIOM = evalEffToFsErrsIOM(evalFsRef, mntsRef, viewsRef)

      val translateFsErrs: FsErrsIOM ~> CfgsErrsIOM =
        free.foldMapNT[FsErrsIO, CfgsErrsIOM](free.interpret3[HFSFailureF, WorkflowExecErrF, Task, CfgsErrsIOM](
          injectFT[HFSFailureF, CfgsErrsIO],
          injectFT[WorkflowExecErrF, CfgsErrsIO],
          injectFT[Task, CfgsErrsIO]))

      val liftTask: Task ~> CfgsErrsIOM =
        injectFT[Task, CfgsErrsIO]

      val mnt: MntEff ~> CfgsErrsIOM =
        free.interpret2[EvalEffM, MountConfigsF, CfgsErrsIOM](
          translateFsErrs.compose[EvalEffM](free.foldMapNT(g)),
          injectFT[MountConfigsF, CfgsErrsIO])

      val mounting: MountingF ~> CfgsErrsIOM =
        Coyoneda.liftTF[Mounting, CfgsErrsIOM](free.foldMapNT(mnt) compose mounter)

      free.interpret4[Task, FileSystemFailureF, MountingF, FileSystem, CfgsErrsIOM](
        liftTask,
        injectFT[FileSystemFailureF, CfgsErrsIO],
        mounting,
        translateFsErrs compose evalFromRef(evalFsRef, f))
    }

  def configsAsState: ConfigsIO ~> Task = {
    type ST[A] = StateT[Task, Map[APath, MountConfig2], A]

    val toState: MountConfigs ~> ST =
      KeyValueStore.toState[StateT[Task, ?, ?]](Lens.id[Map[APath, MountConfig2]])

    val interpret: ConfigsIO ~> ST =
      free.interpret2[MountConfigsF, Task, ST](
        Coyoneda.liftTF(toState),
        liftMT[Task, StateT[?[_], Map[APath, MountConfig2], ?]])

    evalNT[Task, Map[APath, MountConfig2]](Map()) compose interpret
  }

  def configsWithPersistence[C: EncodeJson](
    ref: TaskRef[C],
    loc: Option[FsFile]
  )(implicit configOps: ConfigOps[C]): ConfigsIO ~> Task =
    free.interpret2[MountConfigsF, Task, Task](
      Coyoneda.liftTF[MountConfigs, Task](writeConfig(configOps)(ref, loc)),
      NaturalTransformation.refl)

  def cfgsErrsIOToMainTask(evalCfgsIO: ConfigsIO ~> Task): CfgsErrsIOM ~> MainTask = {
    val f = free.interpret4[FileSystemFailureF, WorkflowExecErrF, HFSFailureF, ConfigsIO, Task](
      Coyoneda.liftTF[FileSystemFailure, Task](Failure.toTaskFailure[FileSystemError]),
      Coyoneda.liftTF[WorkflowExecErr, Task](Failure.toTaskFailure[WorkflowExecutionError]),
      Coyoneda.liftTF[HFSFailure, Task](Failure.toTaskFailure[HierarchicalFileSystemError]),
      evalCfgsIO)

    val g = new (CfgsErrsIO ~> MainTask) {
      def apply[A](a: CfgsErrsIO[A]) =
        EitherT(f(a).attempt).leftMap(_.getMessage)
    }

    hoistFree(g)
  }

  def cfgsErrsIOToResponseOr(evalCfgsIO: ConfigsIO ~> Task): CfgsErrsIOM ~> ResponseOr = {
    val f = free.interpret4[FileSystemFailureF, WorkflowExecErrF, HFSFailureF, ConfigsIO, ResponseOr](
      Coyoneda.liftTF[FileSystemFailure, ResponseOr](failureResponseOr[FileSystemError]),
      Coyoneda.liftTF[WorkflowExecErr, ResponseOr](failureResponseOr[WorkflowExecutionError]),
      Coyoneda.liftTF[HFSFailure, ResponseOr](failureResponseOr[HierarchicalFileSystemError]),
      liftMT[Task, ResponseT] compose evalCfgsIO)

    hoistFree(f: CfgsErrsIO ~> ResponseOr)
  }

  def mountAll(mc: MountingsConfig2): Free[ApiEff, String \/ Unit] = {
    val mnt = Mounting.Ops[ApiEff]
    type N[A] = EitherT[mnt.F, String, A]

    def toN(v: mnt.M[PathTypeMismatch \/ Unit]): N[Unit] =
      EitherT[mnt.F, String, Unit](
        v.fold(_.shows.left, _.fold(_.shows.left, _.right)))

    mc.toMap.toList.traverse_ { case (p, cfg) => toN(mnt.mount(p, cfg)) }.run
  }

  def configuration(args: Array[String]): MainTask[QuasarConfig] = for {
    opts    <- CliOptions.parser.parse(args, CliOptions.default)
                .cata(_.point[MainTask], MainTask.raiseError("couldn't parse options"))
    content <- StaticContent.fromCliOptions("/files", opts)
    redirect = content.map(_.loc)
    cfgPath <- opts.config.fold(none[FsFile].point[MainTask])(cfg =>
                  FsPath.parseSystemFile(cfg)
                    .toRight(s"Invalid path to config file: $cfg")
                    .map(some))
  } yield QuasarConfig(content.toList, redirect, opts.port, cfgPath, opts.openClient)

  def nonApiService(
    initialPort: Int,
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
      "/server/port" -> control.service(initialPort, reload)
    ): _*)
  }

  def startWebServer(
    initialPort: Int,
    staticContent: List[StaticContent],
    redirect: Option[String],
    openClient: Boolean,
    eval: ApiEff ~> ResponseOr
  ): Task[Unit] = {
    import RestApi._

    val produceSvc = (reload: Int => Task[Unit]) =>
      finalizeServices(eval)(
        coreServices[ApiEff],
        additionalServices
      ) orElse nonApiService(initialPort, reload, staticContent, redirect)

    startAndWait(initialPort, produceSvc, openClient)
  }

  def main(args: Array[String]): Unit = {
    implicit val configOps: ConfigOps[WebConfig] = WebConfig
    val exec: MainTask[Unit] = for {
      qConfig     <- configuration(args)
      config      <- WebConfig.get(qConfig.configPath).liftM[MainErrT]
                    // TODO: Find better way to do this
      updConfig   = config.copy(server = config.server.copy(qConfig.port.getOrElse(config.server.port)))
      api         <- evalApi.liftM[MainErrT]
      stateApi    =  cfgsErrsIOToMainTask(configsAsState) compose api
      _           <- mountAll(config.mountings) foldMap stateApi
      cfgRef      <- TaskRef(config).liftM[MainErrT]
      durableApi  =  cfgsErrsIOToResponseOr(configsWithPersistence(cfgRef, qConfig.configPath)) compose api
      _           <- startWebServer(
                       updConfig.server.port,
                       qConfig.staticContent,
                       qConfig.redirect,
                       qConfig.openClient,
                       durableApi
                     ).liftM[MainErrT]
    } yield ()

    logErrors(exec).run
  }
}
