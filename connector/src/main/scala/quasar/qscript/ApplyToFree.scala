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

package quasar.qscript

import quasar.fp.ski.ι

import matryoshka._
import matryoshka.data._
import matryoshka.implicits._
import matryoshka.patterns._

import simulacrum.typeclass

import scalaz._

@typeclass trait ApplyToCoEnv[F[_]] {
  type IT[F[_]]

  // TODO abstract `Hole`
  type CoEnvF[F[_]] = CoEnv[Hole, F, Free[F, Hole]]

  def run[A](alg: CoEnvF[QScriptTotal[IT, ?]] => CoEnvF[QScriptTotal[IT, ?]])
    : F[A] => F[A]
}

object ApplyToCoEnv {

  type Aux[T[_[_]], F[_]] = ApplyToCoEnv[F] { type IT[F[_]] = T[F] }

  type CoEnvF[F[_]] = CoEnv[Hole, F, Free[F, Hole]]

  def applyToFreeQS[T[_[_]]](alg: CoEnvF[QScriptTotal[T, ?]] => CoEnvF[QScriptTotal[T, ?]])
      : FreeQS[T] => FreeQS[T] =
    _.transCata[FreeQS[T]](alg)

  implicit def const[T[_[_]], A]: ApplyToCoEnv.Aux[T, Const[A, ?]] =
    new ApplyToCoEnv[Const[A, ?]] {
      type IT[F[_]] = T[F]

      def run[B](alg: CoEnvF[QScriptTotal[IT, ?]] => CoEnvF[QScriptTotal[IT, ?]])
          : Const[A, B] => Const[A, B] = ι
    }

  implicit def coproduct[T[_[_]], F[_], G[_]]
    (implicit F: ApplyToCoEnv.Aux[T, F], G: ApplyToCoEnv.Aux[T, G])
      : ApplyToCoEnv.Aux[T, Coproduct[F, G, ?]] =
    new ApplyToCoEnv[Coproduct[F, G, ?]] {
      type IT[F[_]] = T[F]

      def run[A](alg: CoEnvF[QScriptTotal[IT, ?]] => CoEnvF[QScriptTotal[IT, ?]])
          : Coproduct[F, G, A] => Coproduct[F, G, A] =
        cp => Coproduct(cp.run.bimap(F.run(alg)(_), G.run(alg)(_)))
    }

  implicit def qscriptCore[T[_[_]]]: ApplyToCoEnv.Aux[T, QScriptCore[T, ?]] =
    new ApplyToCoEnv[QScriptCore[T, ?]] {
      type IT[F[_]] = T[F]

      def run[A](alg: CoEnvF[QScriptTotal[IT, ?]] => CoEnvF[QScriptTotal[IT, ?]])
          : QScriptCore[IT, A] => QScriptCore[IT, A] = _ match {
        case Union(src, left, right) =>
          Union(src,
            applyToFreeQS[IT](alg)(left),
            applyToFreeQS[IT](alg)(right))
        case Subset(src, from, op, count) =>
          Subset(src,
            applyToFreeQS[IT](alg)(from),
            op,
            applyToFreeQS[IT](alg)(count))
        case qs => qs
      }
    }

  implicit def projectBucket[T[_[_]]]: ApplyToCoEnv.Aux[T, ProjectBucket[T, ?]] =
    new ApplyToCoEnv[ProjectBucket[T, ?]] {
      type IT[F[_]] = T[F]

      def run[A](alg: CoEnvF[QScriptTotal[IT, ?]] => CoEnvF[QScriptTotal[IT, ?]])
          : ProjectBucket[IT, A] => ProjectBucket[IT, A] = ι
    }

  implicit def thetaJoin[T[_[_]]]: ApplyToCoEnv.Aux[T, ThetaJoin[T, ?]] =
    new ApplyToCoEnv[ThetaJoin[T, ?]] {
      type IT[F[_]] = T[F]

      def run[A](alg: CoEnvF[QScriptTotal[IT, ?]] => CoEnvF[QScriptTotal[IT, ?]])
          : ThetaJoin[IT, A] => ThetaJoin[IT, A] = {
        case ThetaJoin(src, left, right, key, func, combine) =>
          ThetaJoin(src,
            applyToFreeQS[IT](alg)(left),
            applyToFreeQS[IT](alg)(right),
            key,
            func,
            combine)
      }
    }

  implicit def equiJoin[T[_[_]]]: ApplyToCoEnv.Aux[T, EquiJoin[T, ?]] =
    new ApplyToCoEnv[EquiJoin[T, ?]] {
      type IT[F[_]] = T[F]

      def run[A](alg: CoEnvF[QScriptTotal[IT, ?]] => CoEnvF[QScriptTotal[IT, ?]])
          : EquiJoin[IT, A] => EquiJoin[IT, A] = {
        case EquiJoin(src, left, right, key, func, combine) =>
          EquiJoin(src,
            applyToFreeQS[IT](alg)(left),
            applyToFreeQS[IT](alg)(right),
            key,
            func,
            combine)
      }
    }
}
