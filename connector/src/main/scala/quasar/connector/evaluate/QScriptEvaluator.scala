/*
 * Copyright 2020 Precog Data
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

package quasar.connector.evaluate

import quasar.{RenderTree, RenderTreeT}
import quasar.common.PhaseResultTell
import quasar.common.phase
import quasar.contrib.iota._
import quasar.fp.idPrism
import quasar.qscript._
import quasar.qscript.rewrites.{
  NormalizeQScript,
  NormalizeQScriptFreeMap,
  ThetaToEquiJoin
}

import cats.{Functor, Monad}
import cats.syntax.applicative._
import cats.syntax.flatMap._
import cats.syntax.functor._

import iotaz.{CopK, TListK}

import matryoshka.{Hole => _, _}
import matryoshka.implicits._

/** Provides for evaluating QScript to a result. */
abstract class QScriptEvaluator[
    T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT,
    F[_]: Monad: MonadPlannerErr: PhaseResultTell,
    R] {

  type QSEd[A] = QScriptEducated[T, A]
  type QSNorm[A] = QScriptNormalized[T, A]

  /** QScript used by this evaluator in planning. */
  type QS[U[_[_]]] <: TListK
  type QSM[A] = CopK[QS[T], A]

  /** Executable representation. */
  type Repr

  def QSMFunctor: Functor[QSM]
  def RenderTQSM: RenderTree[T[QSM]]

  /** Returns the result of executing the `Repr`. */
  def execute(repr: Repr): F[R]

  /** Returns the executable representation of the provided QScript. */
  def plan(cp: T[QSM]): F[Repr]

  /** Rewrites the qscript for optimal evaluation. */
  def optimize(norm: T[QSNorm]): F[T[QSM]]

  def evaluate(qsr: T[QSEd]): F[R] =
    for {
      equiJoin <- toEquiJoin(qsr).pure[F]
      _ <- phase[F][T[QSNorm]]("QScript (EquiJoin)", equiJoin)

      normQS <- NormalizeQScript[T](equiJoin).pure[F]
      _ <- phase[F][T[QSNorm]]("QScript (Normalized QScript)", normQS)

      normMF <- NormalizeQScriptFreeMap(normQS).pure[F]
      _ <- phase[F][T[QSNorm]]("QScript (Normalized FreeMap)", normMF)

      optimized <- optimize(normMF)
      interpreted <- phase[F][T[QSM]]("QScript (Optimized)", optimized)

      repr <- plan(interpreted)
      result <- execute(repr)
    } yield result

  ////

  private def toEquiJoin(qs: T[QSEd]): T[QSNorm] = {
    val J = ThetaToEquiJoin[T, QSEd, QSNorm]
    qs.transCata[T[QSNorm]](J.rewrite[J.G](idPrism.reverseGet))
  }

  private final implicit def _QSMFunctor: Functor[QSM] = QSMFunctor
  private final implicit def _RenderTQSM: RenderTree[T[QSM]] = RenderTQSM
}
