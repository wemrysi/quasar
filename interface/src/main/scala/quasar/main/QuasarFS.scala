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

package quasar.main

import slamdata.Predef._
import quasar._
import quasar.contrib.pathy._
import quasar.db.DbConnectionConfig
import quasar.fp.free._
import quasar.fp.numeric._
import quasar.fs._
import quasar.fs.mount._
import quasar.fs.mount.module.Module
import quasar.sql._

import matryoshka.data.Fix
import pathy.Path
import pathy.Path._
import scalaz._
import scalaz.concurrent.Task
import scalaz.stream.Process

/**
  * The Quasar Filesystem. Contains the `CoreEff` that can be used to interpret most
  * operations. Also has convenience methods for executing common operation within `Task`.
  * The `shutdown` task should be called once you are done with it.
  * @param interp
  * @param shutdown Trigger the underlying connector drivers to shutdown cleanly.
  */
final case class QuasarFS(interp: CoreEff ~> QErrs_TaskM, shutdown: Task[Unit]) {
  private val taskInter: CoreEff ~> Task =
    foldMapNT(NaturalTransformation.refl[Task] :+: QErrs.toCatchable[Task]) compose interp

  def getCurrentMetastore: Task[DbConnectionConfig] =
    MetaStoreLocation.Ops[CoreEff].get.foldMap(taskInter)

  def attemptChangeMetastore(newConnection: DbConnectionConfig, initialize: Boolean): Task[String \/ Unit] =
    MetaStoreLocation.Ops[CoreEff].set(newConnection, initialize).foldMap(taskInter)

  def getMount(path: APath): Task[Option[MountConfig]] =
    Mounting.Ops[CoreEff].lookupConfig(path).run.foldMap(taskInter)

  def moveMount[T](src: Path[Abs,T,Sandboxed], dst: Path[Abs,T,Sandboxed]): Task[Unit] =
     Mounting.Ops[CoreEff].remount[T](src, dst).foldMap(taskInter)

  def mount(path: APath, mountConfig: MountConfig, replaceIfExists: Boolean): Task[Unit] =
    Mounting.Ops[CoreEff].mountOrReplace(path, mountConfig, replaceIfExists).foldMap(taskInter)

  def mountView(file: AFile, scopedExpr: ScopedExpr[Fix[Sql]], vars: Variables): Task[Unit] =
    Mounting.Ops[CoreEff].mountView(file, scopedExpr, vars).foldMap(taskInter)

  def mountModule(dir: ADir, statements: List[Statement[Fix[Sql]]]): Task[Unit] =
    Mounting.Ops[CoreEff].mountModule(dir, statements).foldMap(taskInter)

  def mountFileSystem(
    loc: ADir,
    typ: FileSystemType,
    uri: ConnectionUri
  ): Task[Unit] =
    Mounting.Ops[CoreEff].mountFileSystem(loc, typ, uri).foldMap(taskInter)

  def invoke(func: AFile, args: Map[String, String], offset: Natural, limit: Option[Positive]): Process[Task, Data] =
    Module.Ops[CoreEff].invokeFunction_(func, args, offset, limit).translate(foldMapNT(taskInter))
}