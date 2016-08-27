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
import quasar.fp._
import quasar.fs.{ADir, FileSystemError}
import quasar.qscript.ConvertPath.{ListContents, Pathed, postPathify}

import matryoshka._, TraverseT.ops._
import matryoshka.patterns.CoEnv
import scalaz._, Scalaz._
import simulacrum.typeclass

@typeclass trait StaticPath[F[_]] {
  type IT[F[_]]

  def pathifyƒ[M[_]: Monad, G[_]: Traverse](g: ListContents[M])(
    implicit TC: Corecursive[IT],
             TR: Recursive[IT],
             PF: Pathable[IT, ?] ~> G,
             QC: QScriptCore[IT, ?] :<: G,
             FG: F ~> G,
             FI: G :<: QScriptTotal[IT, ?],
             F: Traverse[F],
             CP: ConvertPath.Aux[IT, Pathable[IT, ?], G]):
      AlgebraM[EitherT[M, FileSystemError, ?], F, IT[QScriptTotal[IT, ?]] \/ IT[Pathable[IT, ?]]]

  def toRead[M[_]: Monad, F[_]: Traverse, G[_]: Functor]
    (g: ListContents[M])
    (implicit
      TC: Corecursive[IT],
      TR: Recursive[IT],
      F: Pathable[IT, ?] ~> F,
      R: Const[Read, ?] :<: G,
      FG: F ~> G,
      DE: Const[DeadEnd, ?] :<: G,
      SP: SourcedPathable[IT, ?] :<: G,
      QC: QScriptCore[IT, ?] :<: G,
      FI: G :<: QScriptTotal[IT, ?],
      CP: ConvertPath.Aux[IT, Pathable[IT, ?], F])
      : IT[Pathable[IT, ?]] => EitherT[M, FileSystemError, IT[G]] = {
    implicit val pathedTraverse: Traverse[Pathed[F, ?]] =
      Traverse[List].compose(Traverse[CoEnv[ADir, F, ?]])

    _.transCataM[EitherT[M, FileSystemError, ?], Pathed[F, ?]](CP.convertPath[M](g)) >>=
      (TraverseT[IT].transCataM[EitherT[M, FileSystemError, ?], Pathed[F, ?], G](_)(postPathify[IT, M, F, G](g)))
    }
}

object StaticPath extends LowPriorityStaticPathInstances {
  type Aux[T[_[_]], F[_]] = StaticPath[F] { type IT[F[_]] = T[F] }

  implicit def pathable[T[_[_]], F[_]](implicit Path: F :<: Pathable[T, ?]):
      StaticPath.Aux[T, F] =
    new StaticPath[F] {
      type IT[F[_]] = T[F]

      def pathifyƒ[M[_]: Monad, G[_]: Traverse](g: ListContents[M])(
        implicit TC: Corecursive[T],
                 TR: Recursive[T],
                 PF: Pathable[IT, ?] ~> G,
                 QC: QScriptCore[IT, ?] :<: G,
                 F: F ~> G,
                 FI: G :<: QScriptTotal[T, ?],
                 T: Traverse[F],
                 CP: ConvertPath.Aux[IT, Pathable[IT, ?], G]):
          // AlgebraM[FileSystemError \/ ?, F, T[QScriptTotal[T, ?]] \/ T[Pathable[T, ?]]]
          F[T[QScriptTotal[T, ?]] \/ T[Pathable[T, ?]]] => EitherT[M, FileSystemError, T[QScriptTotal[T, ?]] \/ T[Pathable[T, ?]]] =
        fa => EitherT(fa.sequence.bimap(qt => FI(F(fa.as(qt))).embed, Path(_).embed).right.point[M])
    }

  implicit def coproduct[T[_[_]], H[_]: Traverse, I[_]: Traverse](
    implicit FS: StaticPath.Aux[T, H], GS: StaticPath.Aux[T, I]):
      StaticPath.Aux[T, Coproduct[H, I, ?]] =
    new StaticPath[Coproduct[H, I, ?]] {
      type IT[F[_]] = T[F]

      def pathifyƒ[M[_]: Monad, G[_]: Traverse](g: ListContents[M])(
        implicit TC: Corecursive[T],
                 TR: Recursive[T],
                 PF: Pathable[IT, ?] ~> G,
                 QC: QScriptCore[IT, ?] :<: G,
                 F: Coproduct[H, I, ?] ~> G,
                 FI: G :<: QScriptTotal[T, ?],
                 T: Traverse[Coproduct[H, I, ?]],
                 CP: ConvertPath.Aux[IT, Pathable[IT, ?], G]):
          AlgebraM[EitherT[M, FileSystemError, ?], Coproduct[H, I, ?], T[QScriptTotal[T, ?]] \/ T[Pathable[T, ?]]] =
        _.run.fold(
          FS.pathifyƒ(g)(Monad[M], Traverse[G], TC, TR, PF, QC, F.compose(Inject[H, Coproduct[H, I, ?]]), FI, Traverse[H], CP),
          GS.pathifyƒ(g)(Monad[M], Traverse[G], TC, TR, PF, QC, F.compose(Inject[I, Coproduct[H, I, ?]]), FI, Traverse[I], CP))
    }
}

sealed trait LowPriorityStaticPathInstances {
  implicit def default[T[_[_]], F[_]]: StaticPath.Aux[T, F] =
    new StaticPath[F] {
      type IT[F[_]] = T[F]

      def pathifyƒ[M[_]: Monad, G[_]: Traverse](g: ListContents[M])(
        implicit TC: Corecursive[T],
                 TR: Recursive[T],
                 PF: Pathable[IT, ?] ~> G,
                 QC: QScriptCore[IT, ?] :<: G,
                 F: F ~> G,
                 FI: G :<: QScriptTotal[T, ?],
                 T: Traverse[F],
                 CP: ConvertPath.Aux[IT, Pathable[IT, ?], G]):
          AlgebraM[EitherT[M, FileSystemError, ?], F, T[QScriptTotal[T, ?]] \/ T[Pathable[T, ?]]]=
        _.traverse(_.fold(
          qt => EitherT(qt.right[FileSystemError].point[M]),
          toRead[M, G, QScriptTotal[T, ?]](g)))
          .map(x => FI(F(x)).embed.left)
    }
}
