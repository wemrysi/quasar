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
import quasar.contrib.pathy._
import quasar.fp._
import quasar.fs._
import quasar.qscript.MapFuncs._

import matryoshka._, Recursive.ops._, FunctorT.ops._
import pathy.Path.{dir1, file1, refineType}
import scalaz._, Scalaz._

/** Converts any {Shifted}Read containing a directory to a union of all the
  * files in that directory.
  */
trait ExpandDirs[IN[_]] {
  type IT[F[_]]
  type OUT[A]

  def expandDirs[M[_]: MonadFsErr, F[_]: Functor]
    (OutToF: OUT ~> F, g: DiscoverPath.ListContents[M])
      : IN[IT[F]] => M[F[IT[F]]]
}

object ExpandDirs extends ExpandDirsInstances {
  type Aux[T[_[_]], IN[_], F[_]] = ExpandDirs[IN] {
    type IT[F[_]] = T[F]
    type OUT[A] = F[A]
  }

  def apply[T[_[_]], IN[_], OUT[_]](implicit ev: ExpandDirs.Aux[T, IN, OUT]) =
    ev
}

abstract class ExpandDirsInstances {
  def applyToBranch[T[_[_]]: Recursive: Corecursive, M[_]: MonadFsErr]
    (listContents: DiscoverPath.ListContents[M], branch: FreeQS[T])
      : M[FreeQS[T]] =
    freeTransCataM(branch)(liftCoM(ExpandDirs[T, QScriptTotal[T, ?], QScriptTotal[T, ?]].expandDirs(coenvPrism.reverseGet, listContents)))

  def union[T[_[_]]: Recursive: Corecursive, OUT[_]: Functor]
    (elems: NonEmptyList[OUT[T[OUT]]])
    (implicit
      QC: QScriptCore[T, ?] :<: OUT,
      FI: Injectable.Aux[OUT, QScriptTotal[T, ?]])
      : OUT[T[OUT]] =
    elems.foldRight1(
      (elem, acc) => QC.inj(Union(QC.inj(Unreferenced[T, T[OUT]]()).embed,
        elem.embed.cata[Free[QScriptTotal[T, ?], Hole]](g => Free.roll(FI.inject(g))),
        acc.embed.cata[Free[QScriptTotal[T, ?], Hole]](g => Free.roll(FI.inject(g))))))

  def wrapDir[T[_[_]]: Corecursive, OUT[_]: Functor]
    (name: String, d: OUT[T[OUT]])
    (implicit QC: QScriptCore[T, ?] :<: OUT)
      : OUT[T[OUT]] =
    QC.inj(Map(d.embed, Free.roll(MakeMap(StrLit(name), HoleF))))

  def allDescendents[T[_[_]]: Corecursive, M[_]: MonadFsErr, OUT[_]: Functor]
    (listContents: DiscoverPath.ListContents[M], wrapFile: AFile => OUT[T[OUT]])
    (implicit QC: QScriptCore[T, ?] :<: OUT)
      : ADir => M[List[OUT[T[OUT]]]] =
    dir => (listContents(dir) >>=
      (ps => ISet.fromList(ps.toList).toList.traverseM(_.fold(
        d => allDescendents[T, M, OUT](listContents, wrapFile).apply(dir </> dir1(d)) ∘ (_ ∘ (wrapDir(d.value, _))),
        f => List(wrapDir(f.value, wrapFile(dir </> file1(f)))).point[M]))))
      .handleError(κ(List.empty[OUT[T[OUT]]].point[M]))

  def unionDirs[T[_[_]]: Corecursive, M[_]: MonadFsErr, OUT[_]: Functor]
    (g: DiscoverPath.ListContents[M], wrapFile: AFile => OUT[T[OUT]])
    (implicit QC: QScriptCore[T, ?] :<: OUT)
      : ADir => M[Option[NonEmptyList[OUT[T[OUT]]]]] =
    allDescendents[T, M, OUT](g, wrapFile).apply(_) ∘ {
      case Nil    => None
      case h :: t => NonEmptyList.nel(h, t.toIList).some
    }

  def unionAll
    [T[_[_]]: Recursive: Corecursive, M[_]: MonadFsErr, OUT[_]: Functor, F[_]: Functor]
    (OutToF: OUT ~> F, g: DiscoverPath.ListContents[M], wrapFile: AFile => OUT[T[OUT]])
    (implicit
      QC: QScriptCore[T, ?] :<: OUT,
      FI: Injectable.Aux[OUT, QScriptTotal[T, ?]])
      : ADir => M[F[T[F]]] =
    dir => unionDirs[T, M, OUT](g, wrapFile).apply(dir) >>= (_.fold[M[F[T[F]]]](
      MonadError[M, FileSystemError].raiseError(FileSystemError.qscriptPlanningFailed(NoFilesFound(List(dir)))))(
      nel => OutToF(union(nel) ∘ (_.transAna(OutToF))).point[M]))

  // real instances

  implicit def read[T[_[_]]: Recursive: Corecursive, F[_]: Functor]
    (implicit
      R: Const[Read[AFile], ?] :<: F,
      QC:    QScriptCore[T, ?] :<: F,
      FI: Injectable.Aux[F, QScriptTotal[T, ?]])
      : ExpandDirs.Aux[T, Const[Read[APath], ?], F] =
    new ExpandDirs[Const[Read[APath], ?]] {
      type IT[F[_]] = T[F]
      type OUT[A] = F[A]

      def wrapRead[F[_]](file: AFile) =
        R.inj(Const[Read[AFile], T[F]](Read(file)))

      def expandDirs[M[_]: MonadFsErr, F[_]: Functor]
        (OutToF: OUT ~> F, g: DiscoverPath.ListContents[M]) =
        r => refineType(r.getConst.path).fold(
          unionAll(OutToF, g, wrapRead[OUT]),
          file => OutToF(wrapRead[F](file)).point[M])
    }

  implicit def shiftedReadPath[T[_[_]]: Recursive: Corecursive, F[_]: Functor]
    (implicit
      SR: Const[ShiftedRead[AFile], ?] :<: F,
      QC:            QScriptCore[T, ?] :<: F,
      FI: Injectable.Aux[F, QScriptTotal[T, ?]])
      : ExpandDirs.Aux[T, Const[ShiftedRead[APath], ?], F] =
    new ExpandDirs[Const[ShiftedRead[APath], ?]] {
      type IT[F[_]] = T[F]
      type OUT[A] = F[A]

      def wrapRead[F[_]](file: AFile, idStatus: IdStatus) =
        SR.inj(Const[ShiftedRead[AFile], T[F]](ShiftedRead(file, idStatus)))

      def expandDirs[M[_]: MonadFsErr, F[_]: Functor]
        (OutToF: OUT ~> F, g: DiscoverPath.ListContents[M]) =
        r => refineType(r.getConst.path).fold(
          unionAll(OutToF, g, wrapRead[OUT](_, r.getConst.idStatus)),
          file => (OutToF(wrapRead[F](file, r.getConst.idStatus)).point[M]))
    }

  // branch handling

  implicit def qscriptCore[T[_[_]]: Recursive: Corecursive, F[_]](implicit QC: QScriptCore[T, ?] :<: F)
      : ExpandDirs.Aux[T, QScriptCore[T, ?], F] =
    new ExpandDirs[QScriptCore[T, ?]] {
      type IT[F[_]] = T[F]
      type OUT[A] = F[A]

      def expandDirs[M[_]: MonadFsErr, F[_]: Functor]
        (OutToF: OUT ~> F, g: DiscoverPath.ListContents[M]) =
        fa => (fa match {
          case Union(src, lb, rb) =>
            (applyToBranch(g, lb) ⊛ applyToBranch(g, rb))(Union(src, _, _))
          case Take(src, lb, rb) =>
            (applyToBranch(g, lb) ⊛ applyToBranch(g, rb))(Take(src, _, _))
          case Drop(src, lb, rb) =>
            (applyToBranch(g, lb) ⊛ applyToBranch(g, rb))(Drop(src, _, _))
          case x => x.point[M]
        }) ∘ (OutToF.compose(QC))
    }

  implicit def thetaJoin[T[_[_]]: Recursive: Corecursive, F[_]](implicit TJ: ThetaJoin[T, ?] :<: F)
      : ExpandDirs.Aux[T, ThetaJoin[T, ?], F] =
    new ExpandDirs[ThetaJoin[T, ?]] {
      type IT[F[_]] = T[F]
      type OUT[A] = F[A]

      def expandDirs[M[_]: MonadFsErr, F[_]: Functor]
        (OutToF: OUT ~> F, g: DiscoverPath.ListContents[M]) =
        fa => (fa match {
          case ThetaJoin(src, lb, rb, on, jType, combine) =>
            (applyToBranch(g, lb) ⊛ applyToBranch(g, rb))(
              ThetaJoin(src, _, _, on, jType, combine))
        }) ∘ (OutToF.compose(TJ))
    }

  implicit def equiJoin[T[_[_]]: Recursive: Corecursive, F[_]](implicit EJ: EquiJoin[T, ?] :<: F)
      : ExpandDirs.Aux[T, EquiJoin[T, ?], F] =
    new ExpandDirs[EquiJoin[T, ?]] {
      type IT[F[_]] = T[F]
      type OUT[A] = F[A]

      def expandDirs[M[_]: MonadFsErr, F[_]: Functor]
        (OutToF: OUT ~> F, g: DiscoverPath.ListContents[M]) =
        fa => (fa match {
          case EquiJoin(src, lb, rb, lk, rk, jType, combine) =>
            (applyToBranch(g, lb) ⊛ applyToBranch(g, rb))(
              EquiJoin(src, _, _, lk, rk, jType, combine))
        }) ∘ (OutToF.compose(EJ))
    }

  implicit def coproduct[T[_[_]], F[_], G[_], H[_]]
    (implicit F: ExpandDirs.Aux[T, F, H], G: ExpandDirs.Aux[T, G, H])
      : ExpandDirs.Aux[T, Coproduct[F, G, ?], H] =
    new ExpandDirs[Coproduct[F, G, ?]] {
      type IT[F[_]] = T[F]
      type OUT[A] = H[A]

      def expandDirs[M[_]: MonadFsErr, F[_]: Functor]
        (OutToF: OUT ~> F, g: DiscoverPath.ListContents[M]) =
        _.run.fold(F.expandDirs(OutToF, g), G.expandDirs(OutToF, g))
    }

  def default[T[_[_]], F[_], G[_]](implicit F: F :<: G)
      : ExpandDirs.Aux[T, F, G] =
    new ExpandDirs[F] {
      type IT[F[_]] = T[F]
      type OUT[A] = G[A]

      def expandDirs[M[_]: MonadFsErr, H[_]: Functor]
        (OutToF: OUT ~> H, g: DiscoverPath.ListContents[M]) =
        fa => OutToF(F.inj(fa)).point[M]
    }

  implicit def deadEnd[T[_[_]], F[_]](implicit DE: Const[DeadEnd, ?] :<: F)
      : ExpandDirs.Aux[T, Const[DeadEnd, ?], F] =
    default

  implicit def readFile[T[_[_]], F[_]](implicit R: Const[Read[AFile], ?] :<: F)
      : ExpandDirs.Aux[T, Const[Read[AFile], ?], F] =
    default

  implicit def shiftedReadFile[T[_[_]], F[_]]
    (implicit SR: Const[ShiftedRead[AFile], ?] :<: F)
      : ExpandDirs.Aux[T, Const[ShiftedRead[AFile], ?], F] =
    default

  implicit def projectBucket[T[_[_]], F[_]]
    (implicit PB: ProjectBucket[T, ?] :<: F)
      : ExpandDirs.Aux[T, ProjectBucket[T, ?], F] =
    default
}
