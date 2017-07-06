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
import quasar.contrib.pathy.APath
import quasar.db.DbConnectionConfig
import quasar.fp.free._
import quasar.fs.mount.MountConfig
import quasar.main.api.{MetaStoreApi, MountApi}

import pathy.Path
import pathy.Path._
import scalaz._, concurrent.Task

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

  type CoreEff_Task[A] = Coproduct[Task, CoreEff, A]

  private val taskInter0: CoreEff_Task ~> Task =
    NaturalTransformation.refl[Task] :+: taskInter

  def getCurrentMetastore: Task[DbConnectionConfig] =
    MetaStoreApi.getCurrentMetastore[CoreEff].foldMap(taskInter)

  def attemptChangeMetastore(newConnection: DbConnectionConfig, initialize: Boolean): Task[String \/ Unit] =
    MetaStoreApi.attemptChangeMetastore[CoreEff_Task](newConnection, initialize).foldMap(taskInter0)

  def getMount(path: APath): Task[Option[MountConfig]] =
    MountApi.getMount[CoreEff](path).run.foldMap(taskInter)

  def moveMount[T](src: Path[Abs,T,Sandboxed], dst: Path[Abs,T,Sandboxed]): Task[Unit] =
    MountApi.moveMount[CoreEff, T](src, dst).foldMap(taskInter)

  def mount(path: APath, mountConfig: MountConfig, replaceIfExists: Boolean): Task[Unit] =
    MountApi.mount[CoreEff](path, mountConfig, replaceIfExists).foldMap(taskInter)
}