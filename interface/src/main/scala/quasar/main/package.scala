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

package quasar

import slamdata.Predef._
import quasar.cli.Cmd
import quasar.config.{ConfigOps, FsFile, ConfigError, CoreConfig, MetaStoreConfig}
import quasar.contrib.pathy._
import quasar.contrib.scalaz.catchable._
import quasar.contrib.scalaz.eitherT._
import quasar.effect._
import quasar.db.{DbConnectionConfig, Schema}
import quasar.fp._, ski._
import quasar.fp.free._
import quasar.fs._
import quasar.fs.mount._
import quasar.fs.mount.hierarchical._
import quasar.fs.mount.module.Module
import quasar.physical._, couchbase.Couchbase
import quasar.main.config.loadConfigFile
import quasar.main.metastore._
import quasar.metastore._

import scala.util.control.NonFatal

import doobie.imports.{ConnectionIO, Transactor}
import doobie.syntax.connectionio._
import eu.timepit.refined.auto._
import monocle.Lens
import pathy.Path.posixCodec
import scalaz.{Failure => _, Lens => _, _}, Scalaz._
import scalaz.concurrent.Task

/** Concrete effect types and their interpreters that implement the quasar
  * functionality.
  */
package object main {
  import FileSystemDef.DefinitionResult
  import QueryFile.ResultHandle

  type MainErrT[F[_], A] = EitherT[F, String, A]
  type MainTask[A]       = MainErrT[Task, A]
  val MainTask           = MonadError[EitherT[Task, String, ?], String]

  /** The physical filesystems currently supported. */
  val physicalFileSystems: FileSystemDef[PhysFsEffM] = IList(
    Couchbase.definition translate injectFT[Task, PhysFsEff],
    marklogic.MarkLogic(
      readChunkSize  = 10000L,
      writeChunkSize = 10000L
    ).definition translate injectFT[Task, PhysFsEff],
    mimir.Mimir.definition translate injectFT[Task, PhysFsEff],
    mongodb.fs.definition[PhysFsEff],
    mongodb.fs.qscriptDefinition[PhysFsEff],
    postgresql.fs.definition[PhysFsEff],
    skeleton.Skeleton.definition translate injectFT[Task, PhysFsEff],
    sparkcore.fs.hdfs.definition[PhysFsEff],
    sparkcore.fs.local.definition[PhysFsEff]
  ).fold

  /** A "terminal" effect, encompassing failures and other effects which
    * we may want to interpret using more than one implementation.
    */
  type QEffIO[A]  = Coproduct[Task, QEff, A]
  type QEff[A]     = Coproduct[MetaStoreLocation, QEff0, A]
  type QEff0[A]    = Coproduct[Mounting, QErrs, A]

  /** All possible types of failure in the system (apis + physical). */
  type QErrs[A]    = Coproduct[PhysErr, CoreErrs, A]

  object QErrs {
    def toCatchable[F[_]: Catchable]: QErrs ~> F =
      Failure.toRuntimeError[F, PhysicalError]             :+:
      Failure.toRuntimeError[F, Module.Error]              :+:
      Failure.toRuntimeError[F, Mounting.PathTypeMismatch] :+:
      Failure.toRuntimeError[F, MountingError]             :+:
      Failure.toRuntimeError[F, FileSystemError]
  }

  type MetaStoreRef[A] = AtomicRef[quasar.metastore.MetaStore, A]

  /** Effect comprising the core Quasar apis. */
  type CoreEffIO[A] = Coproduct[Task, CoreEff, A]
  type CoreEff[A]   = (MetaStoreLocation :\: Module :\: Mounting :\: Analyze :\: QueryFile :\: ReadFile :\: WriteFile :\: ManageFile :/: CoreErrs)#M[A]

  object CoreEff {
    def runFs[S[_]](
      hfsRef: TaskRef[AnalyticalFileSystem ~> HierarchicalFsEffM]
    )(
      implicit
      S0: Task :<: S,
      S1: Mounting :<: S,
      S2: PhysErr :<: S,
      S3: MountingFailure :<: S,
      S4: PathMismatchFailure :<: S,
      S5: FileSystemFailure :<: S,
      S6: Module.Failure    :<: S,
      S7: MetaStoreLocation :<: S
    ): Task[CoreEff ~> Free[S, ?]] = {
      def moduleInter(fs: AnalyticalFileSystem ~> Free[S,?]): Module ~> Free[S, ?] = {
        val wtv: Coproduct[Mounting, AnalyticalFileSystem, ?] ~> Free[S,?] = injectFT[Mounting, S] :+: fs
        flatMapSNT(wtv) compose Module.impl.default[Coproduct[Mounting, AnalyticalFileSystem, ?]]
      }
      CompositeFileSystem.interpreter[S](hfsRef) map { compFs =>
        injectFT[MetaStoreLocation, S]                       :+:
        moduleInter(compFs)                             :+:
        injectFT[Mounting, S]                           :+:
        (compFs compose Inject[Analyze, AnalyticalFileSystem])  :+:
        (compFs compose Inject[QueryFile, AnalyticalFileSystem])  :+:
        (compFs compose Inject[ReadFile, AnalyticalFileSystem])   :+:
        (compFs compose Inject[WriteFile, AnalyticalFileSystem])  :+:
        (compFs compose Inject[ManageFile, AnalyticalFileSystem]) :+:
        injectFT[Module.Failure, S]                     :+:
        injectFT[PathMismatchFailure, S]                :+:
        injectFT[MountingFailure, S]                    :+:
        injectFT[FileSystemFailure, S]
      }
    }
  }

  /** The types of failure from core apis. */
  type CoreErrs[A]   = Coproduct[Module.Failure, CoreErrs1, A]
  type CoreErrs1[A]  = Coproduct[PathMismatchFailure, CoreErrs0, A]
  type CoreErrs0[A]  = Coproduct[MountingFailure, FileSystemFailure, A]


  //---- FileSystems ----

  /** A FileSystem supporting views and physical filesystems mounted at various
    * points in the hierarchy.
    */
  object CompositeFileSystem {
    /** Interprets FileSystem given a TaskRef containing a hierarchical
      * FileSystem interpreter.
      *
      * TODO: The TaskRef is used as a communications channel so that
      *       the part of the system that deals with mounting can make
      *       new interpreters available to the part of the system that
      *       needs to interpret FileSystem operations.
      *
      *       This would probably be better served with a
      *       `Process[Task, FileSystem ~> HierarchicalFsEffM]` to allow
      *       for more flexible production of interpreters.
      */
    def interpreter[S[_]](
      hfsRef: TaskRef[AnalyticalFileSystem ~> HierarchicalFsEffM]
    )(implicit
      S0: Task :<: S,
      S1: PhysErr :<: S,
      S2: Mounting :<: S,
      S3: MountingFailure :<: S,
      S4: PathMismatchFailure :<: S
    ): Task[AnalyticalFileSystem ~> Free[S, ?]] =
      for {
        startSeq   <- Task.delay(scala.util.Random.nextInt.toLong)
        seqRef     <- TaskRef(startSeq)
        viewHRef   <- TaskRef[ViewState.ViewHandles](Map())
        mntedRHRef <- TaskRef(Map[ResultHandle, (ADir, ResultHandle)]())
      } yield {
        val hierarchicalFs: AnalyticalFileSystem ~> Free[S, ?] =
          HierarchicalFsEff.dynamicFileSystem(
            hfsRef,
            HierarchicalFsEff.interpreter[S](seqRef, mntedRHRef))

        type V[A] = (
              ViewState
          :\: MonotonicSeq
          :\: Mounting
          :\: MountingFailure
          :\: PathMismatchFailure
          :/: AnalyticalFileSystem
        )#M[A]

        val compFs: V ~> Free[S, ?] =
          injectFT[Task, S].compose(KeyValueStore.impl.fromTaskRef(viewHRef)) :+:
          injectFT[Task, S].compose(MonotonicSeq.fromTaskRef(seqRef))         :+:
          injectFT[Mounting, S]                                               :+:
          injectFT[MountingFailure, S]                                        :+:
          injectFT[PathMismatchFailure, S]                                    :+:
          hierarchicalFs

        flatMapSNT(compFs) compose flatMapSNT(transformIn[AnalyticalFileSystem, V, Free[V, ?]](module.analyticalFileSystem[V], liftFT)) compose view.analyticalFileSystem[V]
      }
  }

  /** The effects required by hierarchical FileSystem operations. */
  type HierarchicalFsEffM[A] = Free[HierarchicalFsEff, A]
  type HierarchicalFsEff[A]  = Coproduct[PhysFsEffM, HierarchicalFsEff0, A]
  type HierarchicalFsEff0[A] = Coproduct[MountedResultH, MonotonicSeq, A]

  object HierarchicalFsEff {
    def interpreter[S[_]](
      seqRef: TaskRef[Long],
      mntResRef: TaskRef[Map[ResultHandle, (ADir, ResultHandle)]]
    )(implicit
      S0: Task :<: S,
      S1: PhysErr :<: S
    ): HierarchicalFsEff ~> Free[S, ?] = {
      val injTask = injectFT[Task, S]

      foldMapNT(liftFT compose PhysFsEff.inject[S])              :+:
      injTask.compose(KeyValueStore.impl.fromTaskRef(mntResRef)) :+:
      injTask.compose(MonotonicSeq.fromTaskRef(seqRef))
    }

    /** A dynamic `FileSystem` evaluator formed by internally fetching an
      * interpreter from a `TaskRef`, allowing for the behavior to change over
      * time as the ref is updated.
      */
    def dynamicFileSystem[S[_]](
      ref: TaskRef[AnalyticalFileSystem ~> HierarchicalFsEffM],
      hfs: HierarchicalFsEff ~> Free[S, ?]
    )(implicit
      S: Task :<: S
    ): AnalyticalFileSystem ~> Free[S, ?] =
      new (AnalyticalFileSystem ~> Free[S, ?]) {
        def apply[A](fs: AnalyticalFileSystem[A]) =
          lift(ref.read.map(free.foldMapNT(hfs) compose _))
            .into[S]
            .flatMap(_ apply fs)
      }
  }

  /** Effects that physical filesystems are permitted. */
  type PhysFsEffM[A] = Free[PhysFsEff, A]
  type PhysFsEff[A]  = Coproduct[Task, PhysErr, A]

  object PhysFsEff {
    def inject[S[_]](implicit S0: Task :<: S, S1: PhysErr :<: S): PhysFsEff ~> S =
      S0 :+: S1

    /** Replace non-fatal failed `Task`s with a PhysicalError. */
    def reifyNonFatalErrors[S[_]](implicit S0: Task :<: S, S1: PhysErr :<: S): Task ~> Free[S, ?] =
      λ[Task ~> Free[S, ?]](t => Free.roll(S0(t map (_.point[Free[S, ?]]) handle {
        case NonFatal(ex: Exception) => Failure.Ops[PhysicalError, S].fail(unhandledFSError(ex))
      })))
  }


  //--- Mounting ---

  /** Provides the mount handlers to update the hierarchical
    * filesystem whenever a mount is added or removed.
    */
  val mountHandler = MountRequestHandler[PhysFsEffM, HierarchicalFsEff](
    physicalFileSystems translate flatMapSNT(
      PhysFsEff.reifyNonFatalErrors[PhysFsEff] :+: injectFT[PhysErr, PhysFsEff]))

  import mountHandler.HierarchicalFsRef

  type MountedFsRef[A] = AtomicRef[Mounts[DefinitionResult[PhysFsEffM]], A]

  /** Effects required for mounting. */
  type MountEffM[A] = Free[MountEff, A]
  type MountEff[A]  = Coproduct[PhysFsEffM, MountEff0, A]
  type MountEff0[A] = Coproduct[HierarchicalFsRef, MountedFsRef, A]

  object MountEff {
    def interpreter[S[_]](
      hrchRef: TaskRef[AnalyticalFileSystem ~> HierarchicalFsEffM],
      mntsRef: TaskRef[Mounts[DefinitionResult[PhysFsEffM]]]
    )(implicit
      S0: Task :<: S,
      S1: PhysErr :<: S
    ): MountEff ~> Free[S, ?] = {
      val injTask = injectFT[Task, S]

      foldMapNT(liftFT compose PhysFsEff.inject[S])   :+:
      injTask.compose(AtomicRef.fromTaskRef(hrchRef)) :+:
      injTask.compose(AtomicRef.fromTaskRef(mntsRef))
    }
  }

  object KvsMounter {
    /** A `Mounting` interpreter that uses a `KeyValueStore` to store
      * `MountConfig`s.
      *
      * TODO: We'd like to not have to expose the `Mounts` `TaskRef`, but
      *       currently need to due to how we initialize the system using
      *       one mounting interpreter that updates these refs, but doesn't
      *       persist configs, and then switch to another that does persist
      *       configs post-initialization.
      *
      *       This should be unnecessary once we switch to lazy, on-demand
      *       mounting.
      *
      * @param cfgsImpl  a `KeyValueStore` interpreter to `Task`
      * @param hrchFsRef the current hierarchical FileSystem interpreter, updated whenever mounts change
      * @param mntdFsRef the current mapping of directories to filesystem definitions,
      *                  updated whenever mounts change
      */
    def interpreter[F[_], S[_]](
      cfgsImpl: MountConfigs ~> F,
      hrchFsRef: TaskRef[AnalyticalFileSystem ~> HierarchicalFsEffM],
      mntdFsRef: TaskRef[Mounts[DefinitionResult[PhysFsEffM]]]
    )(implicit
      S0: F :<: S,
      S1: Task :<: S,
      S2: PhysErr :<: S
    ): Mounting ~> Free[S, ?] = {
      type G[A] = Coproduct[MountConfigs, MountEffM, A]

      val f: G ~> Free[S, ?] =
        injectFT[F, S].compose(cfgsImpl) :+:
        free.foldMapNT(MountEff.interpreter[S](hrchFsRef, mntdFsRef))

      val mounter: Mounting ~> Free[G, ?] =
        quasar.fs.mount.Mounter.kvs[MountEffM, G](
          mountHandler.mount[MountEff](_),
          mountHandler.unmount[MountEff](_))

      free.foldMapNT(f) compose mounter
    }

    def ephemeralMountConfigs[F[_]: Monad]: MountConfigs ~> F = {
      type S = Map[APath, MountConfig]
      evalNT[F, S](Map()) compose KeyValueStore.impl.toState[StateT[F, S, ?]](Lens.id[S])
    }
  }

  /** Mount all the mounts defined in the given configuration, returning
    * the paths that failed to mount along with the reasons why.
    */
  def attemptMountAll[S[_]](
    config: MountingsConfig
  )(implicit
    S: Mounting :<: S
  ): Free[S, Map[APath, String]] = {
    import Mounting.PathTypeMismatch
    import Failure.{mapError, toError}

    type T0[A] = Coproduct[MountingFailure, S, A]
    type T[A]  = Coproduct[PathMismatchFailure, T0, A]
    type Errs  = PathTypeMismatch \/ MountingError
    type M[A]  = EitherT[Free[S, ?], Errs, A]

    val mounting = Mounting.Ops[T]

    val runErrs: T ~> M =
      (toError[M, Errs] compose mapError[PathTypeMismatch, Errs](\/.left)) :+:
      (toError[M, Errs] compose mapError[MountingError, Errs](\/.right))   :+:
      (liftMT[Free[S, ?], EitherT[?[_], Errs, ?]] compose liftFT[S])

    val attemptMount: ((APath, MountConfig)) => Free[S, Map[APath, String]] = {
      case (path, cfg) =>
        mounting.mount(path, cfg).foldMap(runErrs).run map {
          case \/-(_)      => Map.empty
          case -\/(-\/(e)) => Map(path -> e.shows)
          case -\/(\/-(e)) => Map(path -> e.shows)
        }
    }

    config.toMap.toList foldMapM attemptMount
  }

  /** Prints a warning about the mount failure to the console. */
  val logFailedMount: ((APath, String)) => Task[Unit] = {
    case (path, err) => console.stderr(
      s"Warning: Failed to mount '${posixCodec.printPath(path)}' because '$err'."
    )
  }

  private def closeFileSystem(dr: DefinitionResult[PhysFsEffM]): Task[Unit] = {
    val transform: PhysFsEffM ~> Task =
      foldMapNT(reflNT[Task] :+: (Failure.toCatchable[Task, Exception] compose Failure.mapError[PhysicalError, Exception](_.cause)))

    dr.translate(transform).close
  }

  private def closeAllFsMounts(mnts: Mounts[DefinitionResult[PhysFsEffM]]): Task[Unit] =
    mnts.traverse_(closeFileSystem(_).attemptNonFatal.void)

  type QErrs_Task[A] = Coproduct[Task, QErrs, A]
  type QErrs_TaskM[A] = Free[QErrs_Task, A]

  object QErrs_Task {
    def toMainTask: QErrs_TaskM ~> MainTask = {
      foldMapNT(liftMT[Task, MainErrT] :+: QErrs.toCatchable[MainTask])
    }
  }

  type QErrsCnxIO[A]  = Coproduct[ConnectionIO, QErrs, A]
  type QErrsTCnxIO[A] = Coproduct[Task, QErrsCnxIO, A]
  type QErrs_CnxIO_Task_MetaStoreLoc[A] = Coproduct[MetaStoreLocation, QErrsTCnxIO, A]
  type QErrs_CnxIO_Task_MetaStoreLocM[A] = Free[QErrs_CnxIO_Task_MetaStoreLoc, A]

  object QErrsCnxIO {
    def qErrsToMainErrT[F[_]: Catchable: Monad]: QErrs ~> MainErrT[F, ?] =
      liftMT[F, MainErrT].compose(QErrs.toCatchable[F])

    def toMainTask(transactor: Transactor[Task]): QErrsCnxIOM ~> MainTask = {
      val f: QErrsCnxIOM ~> MainErrT[ConnectionIO, ?] =
        foldMapNT(liftMT[ConnectionIO, MainErrT] :+: qErrsToMainErrT[ConnectionIO])

      Hoist[MainErrT].hoist(transactor.trans) compose f
    }
  }

  type QErrsCnxIOM[A]  = Free[QErrsCnxIO, A]
  type QErrsTCnxIOM[A] = Free[QErrsTCnxIO, A]

  object QErrsTCnxIO {
    def toMainTask(transactor: Transactor[Task]): QErrsTCnxIOM ~> MainTask = {
      val f: QErrsTCnxIOM ~> MainErrT[ConnectionIO, ?] =
        foldMapNT(
          (liftMT[ConnectionIO, MainErrT] compose taskToConnectionIO) :+:
            liftMT[ConnectionIO, MainErrT]                              :+:
            QErrsCnxIO.qErrsToMainErrT[ConnectionIO])

      Hoist[MainErrT].hoist(transactor.trans) compose f
    }
  }

  /** Initialize the Quasar FileSystem assuming all defaults */
  val initializeFS: MainTask[QuasarFS] = initializeFS_(None)

  /** Initialize the Quasar FileSytem using the configuration
    * found in `configPath` if it's a `Some`
    */
  def initializeFS_(configPath: Option[FsFile]): MainTask[QuasarFS] =
    for {
      cfgFile      <- loadConfigFile[CoreConfig](configPath).liftM[MainErrT]
      metastoreCfg <- cfgFile.metastore.cata(Task.now, MetaStoreConfig.configOps.default).liftM[MainErrT]
      quasarFS     <- initializeFSWith(metastoreCfg.database)
    } yield quasarFS

  def initializeFSWith(db: DbConnectionConfig): MainTask[QuasarFS] =
    for {
      metastore <- metastoreTransactor(db)
      metaRef   <- TaskRef(metastore).liftM[MainErrT]
      quasarFS  <- initializeFSImpl(metaRef, db.isInMemory)
    } yield quasarFS

  def initializeFSImpl(metaRef: TaskRef[MetaStore], initialize: Boolean): MainTask[QuasarFS] =
    for {
      metastore  <- metaRef.read.liftM[MainErrT]
      _          <- if (initialize)
                      initUpdateMigrate(quasar.metastore.Schema.schema, metastore.trans.transactor, None)
                    else Task.now(()).liftM[MainErrT]
      _          <- verifySchema(quasar.metastore.Schema.schema, metastore.trans.transactor).leftMap(_.message)
      hfsRef     <- TaskRef(Empty.analyticalFileSystem[HierarchicalFsEffM]).liftM[MainErrT]
      mntdRef    <- TaskRef(Mounts.empty[DefinitionResult[PhysFsEffM]]).liftM[MainErrT]

      ephmralMnt =  KvsMounter.interpreter[Task, QErrsTCnxIO](
        KvsMounter.ephemeralMountConfigs[Task], hfsRef, mntdRef) andThen
        mapSNT(absorbTask)                                       andThen
        QErrsCnxIO.toMainTask(metastore.trans.transactor)

      mountsCfg  <- MetaStoreAccess.fsMounts
        .map(MountingsConfig(_))
        .transact(metastore.trans.transactor)
        .liftM[MainErrT]

      // TODO: Still need to expose these in the HTTP API, see SD-1131
      failedMnts <- attemptMountAll[Mounting](mountsCfg) foldMap ephmralMnt
      _          <- failedMnts.toList.traverse_(logFailedMount).liftM[MainErrT]

      runCore    <- CoreEff.runFs[QEffIO](hfsRef).liftM[MainErrT]
    } yield {
      val f: QEffIO ~> QErrs_CnxIO_Task_MetaStoreLocM =
        injectFT[Task, QErrs_CnxIO_Task_MetaStoreLoc]               :+:
        injectFT[MetaStoreLocation, QErrs_CnxIO_Task_MetaStoreLoc]  :+:
        jdbcMounter[QErrs_CnxIO_Task_MetaStoreLoc](hfsRef, mntdRef) :+:
        injectFT[QErrs, QErrs_CnxIO_Task_MetaStoreLoc]

      val connectionIOToTask: ConnectionIO ~> Task =
        λ[ConnectionIO ~> Task](io => metaRef.read.flatMap(t => t.trans.transactor.trans(io)))
      val g: QErrs_CnxIO_Task_MetaStoreLoc ~> QErrs_TaskM =
        (injectFT[Task, QErrs_Task] compose MetaStoreLocation.impl.default[QErrs_Task](metaRef)) :+:
         injectFT[Task, QErrs_Task]                                                              :+:
        (injectFT[Task, QErrs_Task] compose connectionIOToTask)                                  :+:
         injectFT[QErrs, QErrs_Task]

      QuasarFS(
        foldMapNT(g) compose foldMapNT(f) compose runCore,
        (mntdRef.read >>= closeAllFsMounts _) *> metaRef.read.flatMap(_.trans.shutdown))
    }

  final case class CmdLineConfig(configPath: Option[FsFile], cmd: Cmd)

  /** Either initialize the metastore or execute the start depending
    * on what command is provided by the user in the command line arguments
    */
  def initMetaStoreOrStart[C: argonaut.DecodeJson](config: CmdLineConfig, start: (C, CoreEff ~> QErrs_TaskM) => MainTask[Unit])(implicit
    configOps: ConfigOps[C]
  ): MainTask[Unit] = {
    for {
      cfg  <- loadConfigFile[C](config.configPath).liftM[MainErrT]
      _     <- config.cmd match {
        case Cmd.Start =>
          for {
            quasarFs <- initializeFS_(config.configPath)
            _        <- start(cfg, quasarFs.interp).ensuring(κ(quasarFs.shutdown.liftM[MainErrT]))
          } yield ()

        case Cmd.InitUpdateMetaStore =>
          for {
            msCfg <- configOps.metaStoreConfig(cfg).cata(Task.now, MetaStoreConfig.configOps.default).liftM[MainErrT]
            trans <- metastoreTransactor(msCfg.database)
            _     <- initUpdateMigrate(quasar.metastore.Schema.schema, trans.transactor, config.configPath).ensuring(κ(trans.shutdown.liftM[MainErrT]))
          } yield ()
      }
    } yield ()
  }

  /** Initialize or update MetaStore Schema and migrate mounts from config file
    */
  def initUpdateMigrate[A](
    schema: Schema[A], tx: Transactor[Task], cfgFile: Option[FsFile]
  ): MainTask[Unit] =
    for {
      j  <- EitherT(ConfigOps.jsonFromFile(cfgFile).fold(
              e => ConfigError.fileNotFound.getOption(e).cata(κ(none.right), e.shows.left),
              _.some.right))
      jʹ <- metastore.initUpdateMetaStore(schema, tx, j)
      _  <- EitherT.right(jʹ.traverse_(ConfigOps.jsonToFile(_, cfgFile)))
    } yield ()
}
