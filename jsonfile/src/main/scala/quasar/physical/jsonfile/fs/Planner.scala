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

package quasar.physical.jsonfile.fs

import scala.Predef.???
import quasar._
import quasar.qscript._
import matryoshka._
import quasar.contrib.matryoshka._
import scalaz._

trait Planner[F[_], QS[_]] {
  /** QS[QRep] => PlannerErrT[PhaseResultT[F, ?], QRep] */
  def plan: AlgebraM[Planner.PlannerT[F, ?], QS, QRep]
}

object Planner {
  type PlannerT[F[_], A] = PlannerErrT[PhaseResultT[F, ?], A]

  def apply[F[_], QS[_]](implicit z: Planner[F, QS]): Planner[F, QS]      = z
  def make[F[_], QS[_]](f: QS[QRep] => PlannerT[F, QRep]): Planner[F, QS] = new Planner[F, QS] { val plan = f }

  implicit def qScriptCore[F[_]: Applicative, T[_[_]]: Recursive: ShowT]: Planner[F, QScriptCore[T, ?]] = ???
  implicit def constDeadEnd[F[_]: Applicative]: Planner[F, Const[DeadEnd, ?]]                           = ???
  implicit def constRead[F[_]: Applicative]: Planner[F, Const[Read, ?]]                                 = ???
  implicit def constShiftedRead[F[_]: Applicative]: Planner[F, Const[ShiftedRead, ?]]                   = ???
  implicit def projectBucket[F[_]: Applicative, T[_[_]]]: Planner[F, ProjectBucket[T, ?]]               = ???
  implicit def thetajoin[F[_]: Applicative, T[_[_]]: Recursive: ShowT]: Planner[F, ThetaJoin[T, ?]]     = ???
  implicit def equiJoin[F[_]: Applicative, T[_[_]]]: Planner[F, EquiJoin[T, ?]]                         = ???

  implicit def coproduct[F[_], Q1[_], Q2[_]](implicit Q1: Planner[F, Q1], Q2: Planner[F, Q2]): Planner[F, Coproduct[Q1, Q2, ?]] =
    make[F, Coproduct[Q1, Q2, ?]](_.run.fold(Q1.plan, Q2.plan))
}
