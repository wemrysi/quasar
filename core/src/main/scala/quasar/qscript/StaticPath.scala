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
import quasar.fs.FileSystemError

import matryoshka._, Recursive.ops._, TraverseT.ops._
import matryoshka.patterns.CoEnv
import pathy.Path._
import scalaz._, Scalaz._
import simulacrum.typeclass

@typeclass trait StaticPath[F[_]] {
  type IT[F[_]]

  def pathifyƒ[M[_]: Monad, G[_]: Traverse](g: ConvertPath.ListContents[M])(
    implicit TC: Corecursive[IT],
             TR: Recursive[IT],
             PF: Pathable[IT, ?] ~> G,
             QC: QScriptCore[IT, ?] :<: G,
             FG: F ~> G,
             FI: G :<: QScriptTotal[IT, ?],
             F: Traverse[F],
             CP: ConvertPath.Aux[IT, Pathable[IT, ?], G]):
      AlgebraM[EitherT[M, FileSystemError, ?], F, IT[QScriptTotal[IT, ?]] \/ IT[Pathable[IT, ?]]]

  def readFile[M[_]: Monad, F[_], G[_]: Functor](
    f: ConvertPath.ListContents[M])(
    implicit TC: Corecursive[IT], R: Const[Read, ?] :<: G, QC: QScriptCore[IT, ?] :<: G, F: F ~> G):
      CoEnv[AbsDir[Sandboxed], F, IT[G]] => M[List[G[IT[G]]]] =
    _.run.fold(allDescendents[M, G](f).apply(_), fa => List(F(fa)).point[M])

  /** Applied after the backend-supplied function, to convert the files into
    * reads and combine them as necessary.
    */
  def postPathify[M[_]: Monad, F[_], G[_]: Functor](
    f: ConvertPath.ListContents[M])(
    implicit TC: Corecursive[IT],
             TR: Recursive[IT],
             R: Const[Read, ?] :<: G,
             FG: F ~> G,
             DE: Const[DeadEnd, ?] :<: G,
             SP: SourcedPathable[IT, ?] :<: G,
             QC: QScriptCore[IT, ?] :<: G, 
             FI: G :<: QScriptTotal[IT, ?]):
      AlgebraicTransformM[IT, EitherT[M, FileSystemError, ?], Pathed[F, ?], G] =
    p => {
      val newP = p.traverseM(readFile[M, F, G](f).apply)
      EitherT(newP ∘ {
        case h :: t =>
          t.foldRight(
            h)(
            (elem, acc) => SP.inj(Union(DE.inj(Const[DeadEnd, IT[G]](Root)).embed, elem.embed.cata((g: G[Free[QScriptTotal[IT, ?], Hole]]) => Free.roll[QScriptTotal[IT, ?], Hole](FI.inj(g))), acc.embed.cata((g: G[Free[QScriptTotal[IT, ?], Hole]]) => Free.roll[QScriptTotal[IT, ?], Hole](FI.inj(g)))))).right
        case Nil => FileSystemError.readFailed("Nil", "found no files").left
      })
    }

  def toRead[M[_]: Monad, F[_]: Traverse, G[_]: Functor](
    g: ConvertPath.ListContents[M])(
    implicit TC: Corecursive[IT],
      TR: Recursive[IT],
      F: Pathable[IT, ?] ~> F,
      R: Const[Read, ?] :<: G,
      FG: F ~> G,
      DE: Const[DeadEnd, ?] :<: G,
      SP: SourcedPathable[IT, ?] :<: G,
      QC: QScriptCore[IT, ?] :<: G,
      FI: G :<: QScriptTotal[IT, ?],
      CP: ConvertPath.Aux[IT, Pathable[IT, ?], F]):
      IT[Pathable[IT, ?]] => EitherT[M, FileSystemError, IT[G]] =
    _.transCataM[EitherT[M, FileSystemError, ?], Pathed[F, ?]](CP.convertPath[M](g)) >>=
      (TraverseT[IT].transCataM[EitherT[M, FileSystemError, ?], Pathed[F, ?], G](_)(postPathify[M, F, G](g)))

  def wrapDir[F[_]: Functor](
    name: String, d: F[IT[F]])(
    implicit TC: Corecursive[IT],
             QC: QScriptCore[IT, ?] :<: F):
      F[IT[F]] =
    QC.inj(Map(d.embed, Free.roll(MapFuncs.MakeMap(MapFuncs.StrLit(name), HoleF))))

  def allDescendents[M[_]: Monad, F[_]: Functor](
    listContents: ConvertPath.ListContents[M])(
    implicit TC: Corecursive[IT],
             R: Const[Read, ?] :<: F,
             QC: QScriptCore[IT, ?] :<: F):
      AbsDir[Sandboxed] => M[List[F[IT[F]]]] =
    dir => listContents(dir).run.flatMap(_.fold(
      κ(List.empty[F[IT[F]]].point[M]),
      _.toList.traverseM(_.fold(
        d => allDescendents[M, F](listContents).apply(dir </> dir1(d)) ∘ (_ ∘ (wrapDir(d.value, _))),
        f => List(wrapDir[F](f.value, R.inj(Const[Read, IT[F]](Read(dir </> file1(f)))))).point[M]))))
}

object StaticPath extends LowPriorityStaticPathInstances {
  type Aux[T[_[_]], F[_]] = StaticPath[F] { type IT[F[_]] = T[F] }

  implicit def pathable[T[_[_]], F[_]](implicit Path: F :<: Pathable[T, ?]):
      StaticPath.Aux[T, F] =
    new StaticPath[F] {
      type IT[F[_]] = T[F]

      def pathifyƒ[M[_]: Monad, G[_]: Traverse](g: ConvertPath.ListContents[M])(
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

      def pathifyƒ[M[_]: Monad, G[_]: Traverse](g: ConvertPath.ListContents[M])(
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

      def pathifyƒ[M[_]: Monad, G[_]: Traverse](g: ConvertPath.ListContents[M])(
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
