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

package quasar.qscript

import quasar.Predef._
import quasar.Planner.PlannerError
import quasar.fp._
import quasar.fs.{ADir, FileSystemError}
import quasar.qscript.ConvertPath.{ListContents, Pathed, postPathify}

import matryoshka._, TraverseT.ops._
import matryoshka.patterns.CoEnv
import scalaz._, Scalaz._
import simulacrum.typeclass

@typeclass trait StaticPath[F[_]] {
  type IT[F[_]]
  type G[A]

  def pathifyƒ[M[_]: Monad](g: ListContents[M])
      : AlgebraM[EitherT[M, FileSystemError, ?], F, IT[G] \/ IT[Pathable[IT, ?]]]

  def toRead[M[_]: Monad, F[_]: Traverse, G[_]: Functor]
    (g: ListContents[M])
    (implicit
      TC: Corecursive[IT],
      TR: Recursive[IT],
      F: Pathable[IT, ?] :<: F,
      R: Const[Read, ?] :<: G,
      QC: QScriptCore[IT, ?] :<: G,
      FG: F :<: G,
      FI: G :<: QScriptTotal[IT, ?],
      CP: ConvertPath.Aux[IT, Pathable[IT, ?], F])
      : IT[Pathable[IT, ?]] => EitherT[M, FileSystemError, IT[G]] = {
    implicit val pathedTraverse: Traverse[Pathed[F, ?]] =
      Traverse[List].compose(Traverse[CoEnv[ADir, F, ?]])

    _.transCataM[EitherT[M, FileSystemError, ?], Pathed[F, ?]](CP.convertPath[M](g)) >>=
      (TraverseT[IT].transCataM[EitherT[M, PlannerError, ?], Pathed[F, ?], G](_)(postPathify[IT, M, F, G](g)).leftMap(FileSystemError.qscriptPlanningFailed(_)))
    }
}

object StaticPath extends LowPriorityStaticPathInstances {
  type Aux[T[_[_]], F[_], H[_]] = StaticPath[F] {
    type IT[F[_]] = T[F]
    type G[A] = H[A]
  }

  implicit def pathable[T[_[_]]: Corecursive, F[_]: Traverse, H[_]: Functor]
    (implicit Path: F :<: Pathable[T, ?], PF: Pathable[T, ?] :<: H)
      : StaticPath.Aux[T, F, H] =
    new StaticPath[F] {
      type IT[F[_]] = T[F]
      type G[A] = H[A]

      def pathifyƒ[M[_]: Monad](g: ListContents[M])
          // AlgebraM[FileSystemError \/ ?, F, T[G] \/ T[Pathable[T, ?]]]
          : F[T[G] \/ T[Pathable[T, ?]]] => EitherT[M, FileSystemError, T[G] \/ T[Pathable[T, ?]]] =
        fa => EitherT(fa.sequence.bimap(qt => PF.inj(Path.inj(fa.as(qt))).embed, Path(_).embed).right.point[M])
    }

  implicit def coproduct[T[_[_]], H[_]: Traverse, I[_]: Traverse, J[_]]
    (implicit FS: StaticPath.Aux[T, H, J], GS: StaticPath.Aux[T, I, J])
      : StaticPath.Aux[T, Coproduct[H, I, ?], J] =
    new StaticPath[Coproduct[H, I, ?]] {
      type IT[F[_]] = T[F]
      type G[A] = J[A]

      def pathifyƒ[M[_]: Monad](g: ListContents[M])
          : AlgebraM[EitherT[M, FileSystemError, ?], Coproduct[H, I, ?], T[G] \/ T[Pathable[T, ?]]] =
        _.run.fold(FS.pathifyƒ(g), GS.pathifyƒ(g))
    }
}

sealed trait LowPriorityStaticPathInstances {
  implicit def inject
    [T[_[_]]: Recursive: Corecursive, F[_]: Traverse, H[_]: Traverse]
    (implicit
      FH:                 F :<: H,
      R:     Const[Read, ?] :<: H,
      QC: QScriptCore[T, ?] :<: H,
      PF:    Pathable[T, ?] :<: H,
      FI: H :<: QScriptTotal[T, ?],
      CP: ConvertPath.Aux[T, Pathable[T, ?], H])
      : StaticPath.Aux[T, F, H] =
    new StaticPath[F] {
      type IT[F[_]] = T[F]
      type G[A] = H[A]

      def pathifyƒ[M[_]: Monad](g: ListContents[M])
          : AlgebraM[EitherT[M, FileSystemError, ?], F, T[G] \/ T[Pathable[T, ?]]] =
        _.traverse(_.fold(
          qt => EitherT(qt.right[FileSystemError].point[M]),
          toRead[M, G, G](g)))
          .map(x => FH.inj(x).embed.left)
    }
}
