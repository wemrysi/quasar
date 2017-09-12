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

package quasar.physical.rdbms

import scala.Predef.implicitly
import matryoshka.{BirecursiveT, Delay, EqualT, RecursiveT, ShowT}
import quasar.connector.BackendModule
import quasar.contrib.pathy.{AFile, APath}
import quasar.contrib.scalaz.MonadReader_
import quasar.effect.uuid.GenUUID
import quasar.effect.{KeyValueStore, MonotonicSeq}
import quasar.fp.{:/:, :\:}
import quasar.fp.free._
import quasar.fs.MonadFsErr
import quasar.fs.ReadFile.ReadHandle
import quasar.fs.mount.BackendDef.{DefErrT, DefinitionError}
import quasar.fs.mount.ConnectionUri
import quasar.physical.rdbms.fs.{RdbmsReadFile, RdbmsWriteFile, SqlReadCursor}
import quasar.qscript.{::/::, ::\::, EquiJoin, ExtractPath, Injectable, QScriptCore, QScriptTotal, ShiftedRead, Unicoalesce, Unirewrite}
import quasar.fs.WriteFile.WriteHandle
import quasar.physical.rdbms.common.{Config, TablePath}
import quasar.physical.rdbms.fs.RdbmsManageFile
import quasar.physical.rdbms.jdbc.JdbcConnectionInfo
import quasar.{RenderTree, RenderTreeT}
import slamdata.Predef._

import doobie.hikari.hikaritransactor.HikariTransactor
import doobie.imports.ConnectionIO
import scalaz._
import Scalaz._
import scalaz.concurrent.Task

trait Rdbms extends BackendModule with RdbmsReadFile with RdbmsWriteFile with RdbmsManageFile with Interpreter {

  type Eff[A] = (
      ConnectionIO :\:
      MonotonicSeq :\:
      GenUUID :\:
      KeyValueStore[ReadHandle, SqlReadCursor, ?] :/:
      KeyValueStore[WriteHandle, TablePath, ?]
  )#M[A]

  type QS[T[_[_]]] = QScriptCore[T, ?] :\: EquiJoin[T, ?] :/: Const[ShiftedRead[AFile], ?]
  type Repr        = String // TODO define best Repr for a SQL query (Doobie Fragment?)
  type M[A]        = Free[Eff, A]

  type Config = common.Config

  def FunctorQSM[T[_[_]]] = Functor[QSM[T, ?]]
  def DelayRenderTreeQSM[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT] =
    implicitly[Delay[RenderTree, QSM[T, ?]]]
  def ExtractPathQSM[T[_[_]]: RecursiveT]                               = ExtractPath[QSM[T, ?], APath]
  def QSCoreInject[T[_[_]]]                                             = implicitly[QScriptCore[T, ?] :<: QSM[T, ?]]
  def MonadM                                                            = Monad[M]
  def UnirewriteT[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT]    = implicitly[Unirewrite[T, QS[T]]]
  def UnicoalesceCap[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT] = Unicoalesce.Capture[T, QS[T]]

  implicit def qScriptToQScriptTotal[T[_[_]]]: Injectable.Aux[
    QSM[T, ?],
    QScriptTotal[T, ?]] =
    ::\::[QScriptCore[T, ?]](::/::[T, EquiJoin[T, ?], Const[ShiftedRead[AFile], ?]])

  def parseConfig(uri: ConnectionUri): DefErrT[Task, Config] =
    EitherT(Task.delay(parseConnectionUri(uri).map(Config.apply)))


  def compile(cfg: Config): DefErrT[Task, (M ~> Task, Task[Unit])] = {
    val xa = HikariTransactor[Task](
      cfg.connInfo.driverClassName,
      cfg.connInfo.url,
      cfg.connInfo.userName,
      cfg.connInfo.password.getOrElse("")
    )
    val close = xa.flatMap(_.configure(_.close()))
    (interp(xa) ∘ (i => (foldMapNT[Eff, Task](i), close))).liftM[DefErrT]
  }

  lazy val MR                   = MonadReader_[Backend, Config]
  lazy val ME                   = MonadFsErr[Backend]

  def plan[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT](
      cp: T[QSM[T, ?]]): Backend[Repr] = {
    ???
  } // TODO

  def QueryFileModule: QueryFileModule = ??? // TODO

  def parseConnectionUri(uri: ConnectionUri): \/[DefinitionError, JdbcConnectionInfo]
}