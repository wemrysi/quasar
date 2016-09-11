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
import quasar.Planner.NoFilesFound
import quasar.fp._
import quasar.fs._
import quasar.qscript.MapFuncs._

import matryoshka._, Recursive.ops._, FunctorT.ops._
import pathy.Path.{dir1, file1, FileName, rootDir}
import scalaz._, Scalaz._, \&/._

trait DiscoverPath[IN[_]] {
  type IT[F[_]]
  type OUT[A]

  def discoverPath[M[_]: Monad](g: DiscoverPath.ListContents[M])
      : AlgebraM[EitherT[M, FileSystemError, ?], IN, List[ADir] \&/ IT[OUT]]
}

object DiscoverPath extends DiscoverPathInstances {
  type Aux[T[_[_]], IN[_], F[_]] = DiscoverPath[IN] {
    type IT[F[_]] = T[F]
    type OUT[A] = F[A]
  }

  def apply[T[_[_]], IN[_], OUT[_]](implicit ev: DiscoverPath.Aux[T, IN, OUT]) =
    ev
}

abstract class DiscoverPathInstances extends DiscoverPathInstances0 {
  def convertBranch[T[_[_]]: Recursive: Corecursive, M[_]: Monad, OUT[_]: Functor]
    (src: List[ADir] \&/ T[OUT], branch: FreeQS[T])
    (f: ListContents[M])
    (implicit
      R:     Const[Read, ?] :<: OUT,
      QC: QScriptCore[T, ?] :<: OUT,
      FI: Injectable.Aux[OUT, QScriptTotal[T, ?]])
      : EitherT[M, FileSystemError, FreeQS[T]] =
    freeCataM[EitherT[M, FileSystemError, ?], QScriptTotal[T, ?], Hole, List[ADir] \&/ T[QScriptTotal[T, ?]]](
      branch)(
      interpretM(
        κ((src ∘ (_.transCata(FI.inject[T[QScriptTotal[T, ?]]]))).point[EitherT[M, FileSystemError, ?]]),
        DiscoverPath[T, QScriptTotal[T, ?], QScriptTotal[T, ?]].discoverPath(f))) >>=
      (unionAll[T, M, QScriptTotal[T, ?]](f).apply(_) ∘ (_.convertTo[Free[?[_], Hole]]))


  def convertBranchingOp
    [T[_[_]]: Recursive: Corecursive, M[_]: Monad, OUT[_]: Functor]
    (src: List[ADir] \&/ T[OUT], lb: FreeQS[T], rb: FreeQS[T], f: ListContents[M])
    (op: (T[OUT], FreeQS[T], FreeQS[T]) => OUT[T[OUT]])
    (implicit
      R:     Const[Read, ?] :<: OUT,
      QC: QScriptCore[T, ?] :<: OUT,
      FI: Injectable.Aux[OUT, QScriptTotal[T, ?]])
      : EitherT[M, FileSystemError, List[ADir] \&/ T[OUT]] =
    (convertBranch(src, lb)(f) ⊛ convertBranch(src, rb)(f))((l, r) =>
      \&/-(op(QC.inj(Unreferenced[T, T[OUT]]()).embed, l, r).embed))

  def fileType[M[_]: Monad](listContents: ListContents[M]):
      (ADir, String) => OptionT[M, ADir \/ AFile] =
    (dir, name) => listContents(dir).toOption >>=
      (cont => OptionT((cont.find(_.fold(_.value ≟ name, _.value ≟ name)) ∘
        (_.bimap(dir </> dir1(_), dir </> file1(_)))).point[M]))

  // real instances

  implicit def root[T[_[_]], F[_]]: DiscoverPath.Aux[T, Const[DeadEnd, ?], F] =
    new DiscoverPath[Const[DeadEnd, ?]] {
      type IT[F[_]] = T[F]
      type OUT[A] = F[A]

      def discoverPath[M[_]: Monad](g: ListContents[M]) =
        κ(-\&/[List[ADir], T[OUT]](List(rootDir)).point[EitherT[M, FileSystemError, ?]])
    }

  implicit def projectBucket[T[_[_]]: Recursive: Corecursive, F[_]: Functor]
    (implicit
      R:       Const[Read, ?] :<: F,
      QC:   QScriptCore[T, ?] :<: F,
      PB: ProjectBucket[T, ?] :<: F,
      FI: Injectable.Aux[F, QScriptTotal[T, ?]])
      : DiscoverPath.Aux[T, ProjectBucket[T, ?], F] =
    new DiscoverPath[ProjectBucket[T, ?]] {
      type IT[F[_]] = T[F]
      type OUT[A] = F[A]

      def discoverPath[M[_]: Monad](g: ListContents[M]) = {
        // FIXME: `value` must be `HoleF`.
        case BucketField(src, value, StrLit(field)) =>
          src.fold(
            dirs => EitherT.right[M, FileSystemError, List[ADir \/ T[OUT]]](dirs.traverseM(fileType(g).apply(_, field).fold(
              df => List(df ∘ (file => R.inj(Const[Read, T[OUT]](Read(file))).embed)),
              Nil))) >>= {
              case Nil => ??? // ERROR
              case h :: t => t.foldRight(h.fold(d => -\&/(List(d)), \&/-(_)))((elem, acc) =>
                elem.fold(
                  d => acc match {
                    case This(ds) => -\&/(d :: ds)
                    case That(qs) => Both(List(d), qs)
                    case Both(ds, qs) => Both(d :: ds, qs)
                  },
                  f => acc match {
                    case This(ds) => Both(ds, f)
                    case That(qs) => That(union(NonEmptyList(f, qs)))
                    case Both(ds, qs) => Both(ds, union(NonEmptyList(f, qs)))
                  }
                )).point[EitherT[M, FileSystemError, ?]]
            },
            out => \&/-(PB.inj(BucketField(out, value, StrLit(field))).embed).point[EitherT[M, FileSystemError, ?]],
            (_, _) => ???)
        case x => x.traverse(unionAll(g)) ∘ (in => \&/-(PB.inj(in).embed))
      }
    }

  implicit def qscriptCore[T[_[_]]: Recursive: Corecursive, F[_]: Functor]
    (implicit
      R:     Const[Read, ?] :<: F,
      QC: QScriptCore[T, ?] :<: F,
      FI: Injectable.Aux[F, QScriptTotal[T, ?]])
      : DiscoverPath.Aux[T, QScriptCore[T, ?], F] =
    new DiscoverPath[QScriptCore[T, ?]] {
      type IT[F[_]] = T[F]
      type OUT[A] = F[A]

      def discoverPath[M[_]: Monad](g: ListContents[M]) = {
        case Union(src, lb, rb) if !src.isThat =>
          convertBranchingOp(src, lb, rb, g)((s, l, r) =>
            QC.inj(Union(s, l, r)))
        case Take(src, lb, rb) if !src.isThat =>
          convertBranchingOp(src, lb, rb, g)((s, l, r) =>
            QC.inj(Take(s, l, r)))
        case Drop(src, lb, rb) if !src.isThat =>
          convertBranchingOp(src, lb, rb, g)((s, l, r) =>
            QC.inj(Drop(s, l, r)))

        case x => x.traverse(unionAll(g)) ∘ (in => \&/-(QC.inj(in).embed))
      }
    }

  // branch handling

  implicit def thetaJoin[T[_[_]]: Recursive: Corecursive, F[_]: Functor]
    (implicit
      R:     Const[Read, ?] :<: F,
      QC: QScriptCore[T, ?] :<: F,
      TJ: ThetaJoin[T, ?] :<: F,
      FI: Injectable.Aux[F, QScriptTotal[T, ?]])
      : DiscoverPath.Aux[T, ThetaJoin[T, ?], F] =
    new DiscoverPath[ThetaJoin[T, ?]] {
      type IT[F[_]] = T[F]
      type OUT[A] = F[A]

      def discoverPath[M[_]: Monad](g: ListContents[M]) = {
        case ThetaJoin(src, lb, rb, on, jType, combine) if !src.isThat =>
          convertBranchingOp(src, lb, rb, g)((s, l, r) =>
            TJ.inj(ThetaJoin(s, l, r, on, jType, combine)))
      }
    }

  implicit def equiJoin[T[_[_]]: Recursive: Corecursive, F[_]: Functor]
    (implicit
      R:     Const[Read, ?] :<: F,
      QC: QScriptCore[T, ?] :<: F,
      EJ: EquiJoin[T, ?] :<: F,
      FI: Injectable.Aux[F, QScriptTotal[T, ?]])
      : DiscoverPath.Aux[T, EquiJoin[T, ?], F] =
    new DiscoverPath[EquiJoin[T, ?]] {
      type IT[F[_]] = T[F]
      type OUT[A] = F[A]

      def discoverPath[M[_]: Monad](g: ListContents[M]) = {
        case EquiJoin(src, lb, rb, lk, rk, jType, combine) if !src.isThat =>
          convertBranchingOp(src, lb, rb, g)((s, l, r) =>
            EJ.inj(EquiJoin(s, l, r, lk, rk, jType, combine)))
      }
    }

  implicit def coproduct[T[_[_]], F[_], G[_], H[_]]
    (implicit F: DiscoverPath.Aux[T, F, H], G: DiscoverPath.Aux[T, G, H])
      : DiscoverPath.Aux[T, Coproduct[F, G, ?], H] =
    new DiscoverPath[Coproduct[F, G, ?]] {
      type IT[F[_]] = T[F]
      type OUT[A] = H[A]

      def discoverPath[M[_]: Monad](g: ListContents[M]) =
        _.run.fold(F.discoverPath(g), G.discoverPath(g))
    }
}

abstract class DiscoverPathInstances0 {
  type ListContents[M[_]] = ADir => EitherT[M, FileSystemError, Set[PathSegment]]

  // TODO: Move to scalaz, or at least quasar.fp
  def -\&/[A, B](a: A): These[A, B] = This(a)
  def \&/-[A, B](b: B): These[A, B] = That(b)

  def union[T[_[_]]: Recursive: Corecursive, OUT[_]: Functor]
    (elems: NonEmptyList[T[OUT]])
    (implicit
      QC: QScriptCore[T, ?] :<: OUT,
      FI: Injectable.Aux[OUT, QScriptTotal[T, ?]])
      : T[OUT] =
    elems.foldRight1(
      (elem, acc) => QC.inj(Union(QC.inj(Unreferenced[T, T[OUT]]()).embed,
        elem.cata[Free[QScriptTotal[T, ?], Hole]](g => Free.roll(FI.inject(g))),
        acc.cata[Free[QScriptTotal[T, ?], Hole]](g => Free.roll(FI.inject(g))))).embed)

  def makeRead[T[_[_]], F[_]]
    (dir: ADir, file: FileName)
    (implicit R: Const[Read, ?] :<: F):
      F[T[F]] =
    R.inj(Const[Read, T[F]](Read(dir </> file1(file))))

  def wrapDir[T[_[_]]: Corecursive, F[_]: Functor]
    (name: String, d: F[T[F]])
    (implicit QC: QScriptCore[T, ?] :<: F)
      : F[T[F]] =
    QC.inj(Map(d.embed, Free.roll(MakeMap(StrLit(name), HoleF))))

  def allDescendents[T[_[_]]: Corecursive, M[_]: Monad, F[_]: Functor](
    listContents: ListContents[M])(
    implicit R: Const[Read, ?] :<: F,
             QC: QScriptCore[T, ?] :<: F):
      ADir => M[List[F[T[F]]]] =
    dir => listContents(dir).run.flatMap(_.fold(
      κ(List.empty[F[T[F]]].point[M]),
      ps => ISet.fromList(ps.toList).toList.traverseM(_.fold(
        d => allDescendents[T, M, F](listContents).apply(dir </> dir1(d)) ∘ (_ ∘ (wrapDir(d.value, _))),
        f => List(wrapDir[T, F](f.value, makeRead(dir, f))).point[M]))))

  def unionDirs[T[_[_]]: Corecursive, M[_]: Monad, OUT[_]: Functor]
    (g: ListContents[M])
    (implicit R: Const[Read, ?] :<: OUT, QC: QScriptCore[T, ?] :<: OUT)
      : List[ADir] => M[Option[NonEmptyList[T[OUT]]]] =
    dirs => dirs.traverseM(allDescendents[T, M, OUT](g)) ∘ (_ ∘ (_.embed) match {
      case Nil    => None
      case h :: t => NonEmptyList.nel(h, t.toIList).some
    })

  def unionAll[T[_[_]]: Recursive: Corecursive, M[_]: Monad, OUT[_]: Functor]
    (g: ListContents[M])
    (implicit
      R:     Const[Read, ?] :<: OUT,
      QC: QScriptCore[T, ?] :<: OUT,
      FI: Injectable.Aux[OUT, QScriptTotal[T, ?]])
      : List[ADir] \&/ T[OUT] => EitherT[M, FileSystemError, T[OUT]] =
    _.fold(
      ds => EitherT(unionDirs[T, M, OUT](g).apply(ds) ∘ (_.fold[FileSystemError \/ T[OUT]](
        FileSystemError.qscriptPlanningFailed(NoFilesFound(ds)).left)(
        union(_).right))),
      _.point[EitherT[M, FileSystemError, ?]],
      (ds, qs) => EitherT.right(unionDirs[T, M, OUT](g).apply(ds) ∘ (_.fold(qs)(d => union(qs <:: d)))))

  implicit def inject[T[_[_]]: Recursive: Corecursive, IN[_]: Traverse, F[_]: Functor]
    (implicit
      R:     Const[Read, ?] :<: F,
      QC: QScriptCore[T, ?] :<: F,
      IN:                IN :<: F,
      FI: Injectable.Aux[F, QScriptTotal[T, ?]])
      : DiscoverPath.Aux[T, IN, F] =
    new DiscoverPath[IN] {
      type IT[F[_]] = T[F]
      type OUT[A] = F[A]

      def discoverPath[M[_]: Monad](g: ListContents[M]) =
        _.traverse(unionAll(g)) ∘ (in => \&/-(IN.inj(in).embed))
    }
}
