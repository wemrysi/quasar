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

package quasar.physical.marklogic

import quasar.SKI.κ
import quasar.{PhaseResultW, PlannerErrT}
import quasar.fp.{freeCata, freeCataM, interpret, interpretM, ShowT}
import quasar.qscript._
import quasar.physical.marklogic.xquery.XQuery

import matryoshka.Recursive
import scalaz.{Const, Free}
import scalaz.{EitherT, Scalaz}, Scalaz._

package object qscript {
  type Planning[A] = PlannerErrT[PhaseResultW, A]

  type MarkLogicPlanner[QS[_]] = Planner[QS, XQuery]

  object MarkLogicPlanner {
    implicit def qScriptCore[T[_[_]]: Recursive: ShowT]: MarkLogicPlanner[QScriptCore[T, ?]] =
      new QScriptCorePlanner[T]

    implicit def sourcedPathable[T[_[_]]: Recursive: ShowT]: MarkLogicPlanner[SourcedPathable[T, ?]] =
      new SourcedPathablePlanner[T]

    implicit def constDeadEnd: MarkLogicPlanner[Const[DeadEnd, ?]] =
      new DeadEndPlanner

    implicit def constRead: MarkLogicPlanner[Const[Read, ?]] =
      new ReadPlanner

    implicit def projectBucket[T[_[_]]]: MarkLogicPlanner[ProjectBucket[T, ?]] =
      new ProjectBucketPlanner[T]

    implicit def thetajoin[T[_[_]]]: MarkLogicPlanner[ThetaJoin[T, ?]] =
      new ThetaJoinPlanner[T]

    implicit def equiJoin[T[_[_]]]: MarkLogicPlanner[EquiJoin[T, ?]] =
      new EquiJoinPlanner[T]
  }

  def mapFuncXQuery[T[_[_]]: Recursive: ShowT](fm: FreeMap[T], src: XQuery): XQuery =
    planMapFunc(fm)(κ(src))

  def planMapFunc[T[_[_]]: Recursive: ShowT, A](
    freeMap: Free[MapFunc[T, ?], A])(
    recover: A => XQuery
  ): XQuery =
    freeCata(freeMap)(interpret(recover, MapFuncPlanner[T]))

  def rebaseXQuery[T[_[_]]: Recursive: ShowT](fqs: FreeQS[T], src: XQuery): Planning[XQuery] = {
    import MarkLogicPlanner._
    // TODO[scalaz]: Shadow the scalaz.Monad.monadMTMAB SI-2712 workaround
    import EitherT.eitherTMonad
    freeCataM(fqs)(interpretM(κ(src.point[Planning]), Planner[QScriptTotal[T, ?], XQuery].plan))
  }
}
