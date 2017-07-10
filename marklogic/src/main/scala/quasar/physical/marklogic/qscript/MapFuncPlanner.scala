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

package quasar.physical.marklogic.qscript

import quasar.physical.marklogic.xquery._
import quasar.qscript._

import matryoshka._
import scalaz._

abstract class MapFuncPlanner[F[_], MF[_], T[_[_]]] {
  def plan: AlgebraM[F, MF, XQuery]
}

object MapFuncPlanner {

  def apply[F[_], MF[_], T[_[_]]](implicit ev: MapFuncPlanner[F, MF, T]): MapFuncPlanner[F, MF, T] = ev

  implicit def coproduct[F[_], G[_], H[_], T[_[_]]: RecursiveT](
    implicit G: MapFuncPlanner[F, G, T], H: MapFuncPlanner[F, H, T]
  ): MapFuncPlanner[F, Coproduct[G, H, ?], T] =
    new MapFuncPlanner[F, Coproduct[G, H, ?], T] {
      def plan: AlgebraM[F, Coproduct[G, H, ?], XQuery] =
        _.run.fold(G.plan, H.plan)
    }

  implicit def mapFuncCore[M[_]: Monad: QNameGenerator: PrologW: MonadPlanErr, FMT, T[_[_]]: RecursiveT](
    implicit
    SP: StructuralPlanner[M, FMT]
  ): MapFuncPlanner[M, MapFuncCore[T, ?], T] =
    new MapFuncCorePlanner[M, FMT, T]

  implicit def mapFuncDerived[M[_]: Monad: QNameGenerator: PrologW: MonadPlanErr, FMT, T[_[_]]: BirecursiveT](
    implicit
    CP: MapFuncPlanner[M, MapFuncCore[T, ?], T]
  ): MapFuncPlanner[M, MapFuncDerived[T, ?], T] =
    new MapFuncDerivedPlanner[M, FMT, T]

}
