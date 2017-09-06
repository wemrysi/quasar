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
package physical.mongodb

import slamdata.Predef._
import quasar.common._
import quasar.connector._
import quasar.contrib.pathy._
import quasar.fp.numeric._
import quasar.fs._
import quasar.fs.mount._
import quasar.physical.mongodb.workflow._
import quasar.qscript._

import java.time.Instant
import matryoshka._
import scalaz._, Scalaz._
import scalaz.concurrent.Task
import scala.Predef.implicitly

object MongoDb extends BackendModule {

  type QS[T[_[_]]] = fs.MongoQScriptCP[T]

  implicit def qScriptToQScriptTotal[T[_[_]]]: Injectable.Aux[QSM[T, ?], QScriptTotal[T, ?]] =
    ::\::[QScriptCore[T, ?]](::/::[T, EquiJoin[T, ?], Const[ShiftedRead[AFile], ?]])

  type Repr = Crystallized[WorkflowF]

  type M[A] = fs.MongoM[A]

  def FunctorQSM[T[_[_]]] = Functor[QSM[T, ?]]
  def DelayRenderTreeQSM[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT] = implicitly[Delay[RenderTree, QSM[T, ?]]]
  def ExtractPathQSM[T[_[_]]: RecursiveT] = ExtractPath[QSM[T, ?], APath]
  def QSCoreInject[T[_[_]]] = implicitly[QScriptCore[T, ?] :<: QSM[T, ?]]
  def MonadM = Monad[M]
  def UnirewriteT[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT] = implicitly[Unirewrite[T, QS[T]]]
  def UnicoalesceCap[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT] = Unicoalesce.Capture[T, QS[T]]

  type Config = fs.MongoConfig

  def parseConfig(uri: ConnectionUri): BackendDef.DefErrT[Task, Config] =
    fs.parseConfig(uri)

  def compile(cfg: Config): BackendDef.DefErrT[Task, (M ~> Task, Task[Unit])] =
    fs.compile(cfg)

  val Type = FileSystemType("mongodb")

  def doPlan[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT,
      N[_]: Monad: MonadFsErr: PhaseResultTell]
      (qs: T[QSM[T, ?]], ctx: fs.QueryContext[N], execTime: Instant): N[Repr] =
      MongoDbPlanner.planExecTime[T, N](qs, ctx, execTime)

  // TODO[scalaz]: Shadow the scalaz.Monad.monadMTMAB SI-2712 workaround
  import EitherT.eitherTMonad

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  def plan[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT](
      qs: T[QSM[T, ?]]): Backend[Repr] =
    for {
      ctx <- fs.QueryContext.queryContext[T](qs)
      _ <- fs.QueryContext.checkPathsExist(qs)
      execTime <- fs.queryfile.queryTime.liftM[PhaseResultT].liftM[FileSystemErrT]
      p <- doPlan[T, Backend](qs, ctx, execTime)
    } yield p

  private type PhaseRes[A] = PhaseResultT[ConfiguredT[M, ?], A]

  private val effToConfigured: fs.Eff ~> Configured =
    λ[fs.Eff ~> Configured](eff => Free.liftF(eff).liftM[ConfiguredT])

  private val effToPhaseRes: fs.Eff ~> PhaseRes =
    λ[Configured ~> PhaseRes](_.liftM[PhaseResultT]) compose effToConfigured

  private def toEff[C[_], A](c: C[A])(implicit inj: C :<: fs.Eff): fs.Eff[A] = inj(c)

  def toBackend[C[_], A](c: C[FileSystemError \/ A])(implicit inj: C :<: fs.Eff): Backend[A] =
    EitherT(c).mapT(x => effToPhaseRes(toEff(x)))

  def toBackendP[C[_], A](c: C[(PhaseResults, FileSystemError \/ A)])(implicit inj: C :<: fs.Eff): Backend[A] =
    EitherT(WriterT(effToConfigured(toEff(c))))

  def toConfigured[C[_], A](c: C[A])(implicit inj: C :<: fs.Eff): Configured[A] =
    effToConfigured(toEff(c))

  override val QueryFileModule = fs.queryfile

  override val ReadFileModule = fs.readfile

  override val WriteFileModule = fs.writefile

  override val ManageFileModule = fs.managefile
}
