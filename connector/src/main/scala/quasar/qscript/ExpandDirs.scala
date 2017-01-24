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
import quasar.contrib.scalaz._
import quasar.fp._
import quasar.fp.ski._
import quasar.fs._
import quasar.qscript.MapFuncs._

import matryoshka._
import matryoshka.data._
import matryoshka.implicits._
import pathy.Path.{dir1, file1, refineType}
import scalaz._, Scalaz._

/** Converts any {Shifted}Read containing a directory to a union of all the
  * files in that directory.
  */
trait ExpandDirs[IN[_]] {
  type IT[F[_]]
  type OUT[A]

  def expandDirs[M[_]: Monad: MonadFsErr, F[_]: Functor]
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
  def expandDirs[T[_[_]]: BirecursiveT] = new ExpandDirsT[T]

  // real instances

  implicit def read[T[_[_]]: BirecursiveT, F[_]: Functor]
    (implicit
      R: Const[Read[AFile], ?] :<: F,
      QC:    QScriptCore[T, ?] :<: F,
      FI: Injectable.Aux[F, QScriptTotal[T, ?]])
      : ExpandDirs.Aux[T, Const[Read[APath], ?], F] =
    expandDirs[T].read[F]

  implicit def shiftedReadPath[T[_[_]]: BirecursiveT, F[_]: Functor]
    (implicit
      SR: Const[ShiftedRead[AFile], ?] :<: F,
      QC:            QScriptCore[T, ?] :<: F,
      FI: Injectable.Aux[F, QScriptTotal[T, ?]])
      : ExpandDirs.Aux[T, Const[ShiftedRead[APath], ?], F] =
    expandDirs[T].shiftedReadPath[F]

  // branch handling

  implicit def qscriptCore[T[_[_]]: BirecursiveT, F[_]](implicit QC: QScriptCore[T, ?] :<: F)
      : ExpandDirs.Aux[T, QScriptCore[T, ?], F] =
    expandDirs[T].qscriptCore[F]

  implicit def thetaJoin[T[_[_]]: BirecursiveT, F[_]](implicit TJ: ThetaJoin[T, ?] :<: F)
      : ExpandDirs.Aux[T, ThetaJoin[T, ?], F] =
    expandDirs[T].thetaJoin[F]

  implicit def equiJoin[T[_[_]]: BirecursiveT, F[_]](implicit EJ: EquiJoin[T, ?] :<: F)
      : ExpandDirs.Aux[T, EquiJoin[T, ?], F] =
    expandDirs[T].equiJoin[F]

  implicit def coproduct[T[_[_]], F[_], G[_], H[_]]
    (implicit F: ExpandDirs.Aux[T, F, H], G: ExpandDirs.Aux[T, G, H])
      : ExpandDirs.Aux[T, Coproduct[F, G, ?], H] =
    new ExpandDirs[Coproduct[F, G, ?]] {
      type IT[F[_]] = T[F]
      type OUT[A] = H[A]

      def expandDirs[M[_]: Monad: MonadFsErr, F[_]: Functor]
        (OutToF: OUT ~> F, g: DiscoverPath.ListContents[M]) =
        _.run.fold(F.expandDirs(OutToF, g), G.expandDirs(OutToF, g))
    }

  def default[T[_[_]], F[_], G[_]](implicit F: F :<: G)
      : ExpandDirs.Aux[T, F, G] =
    new ExpandDirs[F] {
      type IT[F[_]] = T[F]
      type OUT[A] = G[A]

      def expandDirs[M[_]: Monad: MonadFsErr, H[_]: Functor]
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

private[qscript] final class ExpandDirsT[T[_[_]]: BirecursiveT] extends TTypes[T] {
  private def ExpandDirsTotal = ExpandDirs[T, QScriptTotal, QScriptTotal]

  def applyToBranch[M[_]: Monad: MonadFsErr]
    (listContents: DiscoverPath.ListContents[M], branch: FreeQS)
      : M[FreeQS] =
    branch.transCataM(liftCoM(
      ExpandDirsTotal.expandDirs(
        coenvPrism[QScriptTotal, Hole].reverseGet,
        listContents)
    )) ∘ (_.convertTo[FreeQS])

  def union[OUT[_]: Functor]
    (elems: NonEmptyList[OUT[T[OUT]]])
    (implicit
      QC: QScriptCore :<: OUT,
      FI: Injectable.Aux[OUT, QScriptTotal])
      : OUT[T[OUT]] =
    elems.foldRight1(
      (elem, acc) => QC.inj(Union(QC.inj(Unreferenced[T, T[OUT]]()).embed,
        elem.embed.cata[Free[QScriptTotal, Hole]](g => Free.roll(FI.inject(g))),
        acc.embed.cata[Free[QScriptTotal, Hole]](g => Free.roll(FI.inject(g))))))

  def wrapDir[OUT[_]: Functor]
    (name: String, d: OUT[T[OUT]])
    (implicit QC: QScriptCore :<: OUT)
      : OUT[T[OUT]] =
    QC.inj(Map(d.embed, Free.roll(MakeMap(StrLit(name), HoleF))))

  def allDescendents[M[_]: Monad: MonadFsErr, OUT[_]: Functor]
    (listContents: DiscoverPath.ListContents[M], wrapFile: AFile => OUT[T[OUT]])
    (implicit QC: QScriptCore :<: OUT)
      : ADir => M[List[OUT[T[OUT]]]] =
    dir => (listContents(dir) >>=
      (ps => ISet.fromList(ps.toList).toList.traverseM(_.fold(
        d => allDescendents[M, OUT](listContents, wrapFile).apply(dir </> dir1(d)) ∘ (_ ∘ (wrapDir(d.value, _))),
        f => List(wrapDir(f.value, wrapFile(dir </> file1(f)))).point[M]))))
      .handleError(κ(List.empty[OUT[T[OUT]]].point[M]))

  def unionDirs[M[_]: Monad: MonadFsErr, OUT[_]: Functor]
    (g: DiscoverPath.ListContents[M], wrapFile: AFile => OUT[T[OUT]])
    (implicit QC: QScriptCore :<: OUT)
      : ADir => M[Option[NonEmptyList[OUT[T[OUT]]]]] =
    allDescendents[M, OUT](g, wrapFile).apply(_) ∘ {
      case Nil    => None
      case h :: t => NonEmptyList.nel(h, t.toIList).some
    }

  def unionAll[M[_]: Monad: MonadFsErr, OUT[_]: Functor, F[_]: Functor]
    (OutToF: OUT ~> F, g: DiscoverPath.ListContents[M], wrapFile: AFile => OUT[T[OUT]])
    (implicit
      QC: QScriptCore :<: OUT,
      FI: Injectable.Aux[OUT, QScriptTotal])
      : ADir => M[F[T[F]]] =
    dir => unionDirs[M, OUT](g, wrapFile).apply(dir) >>= (_.fold[M[F[T[F]]]](
      MonadError_[M, FileSystemError].raiseError(FileSystemError.qscriptPlanningFailed(NoFilesFound(List(dir)))))(
      nel => OutToF(union(nel) ∘ (_.transAna[T[F]](OutToF))).point[M]))

  // real instances

  def read[F[_]: Functor]
    (implicit
      R: Const[Read[AFile], ?] :<: F,
      QC:          QScriptCore :<: F,
      FI: Injectable.Aux[F, QScriptTotal])
      : ExpandDirs.Aux[T, Const[Read[APath], ?], F] =
    new ExpandDirs[Const[Read[APath], ?]] {
      type IT[F[_]] = T[F]
      type OUT[A] = F[A]

      def wrapRead[F[_]](file: AFile) =
        R.inj(Const[Read[AFile], T[F]](Read(file)))

      def expandDirs[M[_]: Monad: MonadFsErr, F[_]: Functor]
        (OutToF: OUT ~> F, g: DiscoverPath.ListContents[M]) =
        r => refineType(r.getConst.path).fold(
          unionAll(OutToF, g, wrapRead[OUT]),
          file => OutToF(wrapRead[F](file)).point[M])
    }

  def shiftedReadPath[F[_]: Functor]
    (implicit
      SR: Const[ShiftedRead[AFile], ?] :<: F,
      QC:                  QScriptCore :<: F,
      FI: Injectable.Aux[F, QScriptTotal])
      : ExpandDirs.Aux[T, Const[ShiftedRead[APath], ?], F] =
    new ExpandDirs[Const[ShiftedRead[APath], ?]] {
      type IT[F[_]] = T[F]
      type OUT[A] = F[A]

      def wrapRead[F[_]](file: AFile, idStatus: IdStatus) =
        SR.inj(Const[ShiftedRead[AFile], T[F]](ShiftedRead(file, idStatus)))

      def expandDirs[M[_]: Monad: MonadFsErr, F[_]: Functor]
        (OutToF: OUT ~> F, g: DiscoverPath.ListContents[M]) =
        r => refineType(r.getConst.path).fold(
          unionAll(OutToF, g, wrapRead[OUT](_, r.getConst.idStatus)),
          file => (OutToF(wrapRead[F](file, r.getConst.idStatus)).point[M]))
    }

  // branch handling

  def qscriptCore[F[_]](implicit QC: QScriptCore :<: F)
      : ExpandDirs.Aux[T, QScriptCore, F] =
    new ExpandDirs[QScriptCore] {
      type IT[F[_]] = T[F]
      type OUT[A] = F[A]

      def expandDirs[M[_]: Monad: MonadFsErr, F[_]: Functor]
        (OutToF: OUT ~> F, g: DiscoverPath.ListContents[M]) =
        fa => (fa match {
          case Union(src, lb, rb) =>
            (applyToBranch(g, lb) ⊛ applyToBranch(g, rb))(Union(src, _, _))
          case Subset(src, lb, o, rb) =>
            (applyToBranch(g, lb) ⊛ applyToBranch(g, rb))(Subset(src, _, o, _))
          case x => x.point[M]
        }) ∘ (OutToF.compose(QC))
    }

  def thetaJoin[F[_]](implicit TJ: ThetaJoin :<: F)
      : ExpandDirs.Aux[T, ThetaJoin, F] =
    new ExpandDirs[ThetaJoin] {
      type IT[F[_]] = T[F]
      type OUT[A] = F[A]

      def expandDirs[M[_]: Monad: MonadFsErr, F[_]: Functor]
        (OutToF: OUT ~> F, g: DiscoverPath.ListContents[M]) =
        fa => (fa match {
          case ThetaJoin(src, lb, rb, on, jType, combine) =>
            (applyToBranch(g, lb) ⊛ applyToBranch(g, rb))(
              ThetaJoin(src, _, _, on, jType, combine))
        }) ∘ (OutToF.compose(TJ))
    }

  def equiJoin[F[_]](implicit EJ: EquiJoin :<: F)
      : ExpandDirs.Aux[T, EquiJoin, F] =
    new ExpandDirs[EquiJoin] {
      type IT[F[_]] = T[F]
      type OUT[A] = F[A]

      def expandDirs[M[_]: Monad: MonadFsErr, F[_]: Functor]
        (OutToF: OUT ~> F, g: DiscoverPath.ListContents[M]) =
        fa => (fa match {
          case EquiJoin(src, lb, rb, lk, rk, jType, combine) =>
            (applyToBranch(g, lb) ⊛ applyToBranch(g, rb))(
              EquiJoin(src, _, _, lk, rk, jType, combine))
        }) ∘ (OutToF.compose(EJ))
    }
}
