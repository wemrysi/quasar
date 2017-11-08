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

package quasar.qscript.qsu

import slamdata.Predef._
import quasar.{RenderTree, RenderTreeT}
import quasar.common.{JoinType, SortDir}
import quasar.contrib.matryoshka.birecursiveIso
import quasar.contrib.pathy.AFile
import quasar.ejson.EJson
import quasar.fp.{τ, PrismNT}
import quasar.qscript._

import matryoshka.{BirecursiveT, Delay, EqualT, ShowT}
import monocle.{Prism, PTraversal, Traversal}
import pathy.Path
import scalaz.{:<:, Applicative, Bitraverse, Equal, Forall, Free, NonEmptyList => NEL, Scalaz, Show, Traverse}

sealed trait QScriptUniform[T[_[_]], A] extends Product with Serializable

object QScriptUniform {

  implicit def traverse[T[_[_]]]: Traverse[QScriptUniform[T, ?]] = new Traverse[QScriptUniform[T, ?]] {
    // we need both apply and traverse syntax, which conflict
    import Scalaz._

    def traverseImpl[G[_]: Applicative, A, B](qsu: QScriptUniform[T, A])(f: A => G[B]): G[QScriptUniform[T, B]] = qsu match {
      case AutoJoin(sources, combiner) =>
        sources.traverse(f).map(nel => AutoJoin(nel, combiner))

      case GroupBy(left, right) =>
        (f(left) |@| f(right))(GroupBy(_, _))

      case DimEdit(source, dtrans) =>
        f(source).map(DimEdit(_, dtrans))

      case LPJoin(left, right, condition, joinType, leftRef, rightRef) =>
        (f(left) |@| f(right) |@| f(condition))(LPJoin(_, _, _, joinType, leftRef, rightRef))

      case ThetaJoin(left, right, condition, joinType, combiner) =>
        (f(left) |@| f(right))(ThetaJoin(_, _, condition, joinType, combiner))

      case Map(source, fm) =>
        f(source).map(Map(_, fm))

      case Read(path) => (Read(path): QScriptUniform[T, B]).point[G]

      case Transpose(source, rotations) =>
        f(source).map(Transpose(_, rotations))

      case LeftShift(source, struct, idStatus, repair) =>
        f(source).map(LeftShift(_, struct, idStatus, repair))

      case LPReduce(source, reduce) =>
        f(source).map(LPReduce(_, reduce))

      case QSReduce(source, buckets, reducers, repair) =>
        f(source).map(QSReduce(_, buckets, reducers, repair))

      case Distinct(source) =>
        f(source).map(Distinct(_))

      case Sort(source, order) =>
        val T = Bitraverse[(?, ?)].leftTraverse[SortDir]

        val source2G = f(source)
        val orders2G = order.traverse(p => T.traverse(p)(f))

        (source2G |@| orders2G)(Sort(_, _))

      case UniformSort(source, buckets, order) =>
        f(source).map(UniformSort(_, buckets, order))

      case Union(left, right) =>
        (f(left) |@| f(right))(Union(_, _))

      case Subset(from, op, count) =>
        (f(from) |@| f(count))(Subset(_, op, _))

      case LPFilter(source, predicate) =>
        (f(source) |@| f(predicate))(LPFilter(_, _))

      case QSFilter(source, predicate) =>
        f(source).map(QSFilter(_, predicate))

      case Nullary(mf) => (Nullary(mf): QScriptUniform[T, B]).point[G]

      case JoinSideRef(id) => (JoinSideRef(id): QScriptUniform[T, B]).point[G]
    }
  }

  @SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
  implicit def show[T[_[_]]: ShowT]
      : Delay[Show, QScriptUniform[T, ?]] = ???

  @SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
  implicit def renderTree[T[_[_]]: RenderTreeT: ShowT]
      : Delay[RenderTree, QScriptUniform[T, ?]] = ???

  @SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
  implicit def equal[T[_[_]]: BirecursiveT: EqualT]
      : Delay[Equal, QScriptUniform[T, ?]] = ???

  final case class AutoJoin[T[_[_]], A](
      sources: NEL[A],
      combiner: MapFunc[T, Int]) extends QScriptUniform[T, A]

  final case class GroupBy[T[_[_]], A](
      left: A,
      right: A) extends QScriptUniform[T, A]

  final case class DimEdit[T[_[_]], A](
      source: A,
      trans: DTrans[T]) extends QScriptUniform[T, A]

  sealed trait DTrans[T[_[_]]] extends Product with Serializable

  object DTrans {
    final case class Squash[T[_[_]]]() extends DTrans[T]
    final case class Group[T[_[_]]](getKey: FreeMap[T]) extends DTrans[T]
  }

  // LPish
  final case class LPJoin[T[_[_]], A](
      left: A,
      right: A,
      condition: A,
      joinType: JoinType,
      leftRef: Symbol,
      rightRef: Symbol) extends QScriptUniform[T, A]

  // QScriptish
  final case class ThetaJoin[T[_[_]], A](
      left: A,
      right: A,
      condition: JoinFunc[T],
      joinType: JoinType,
      combiner: JoinFunc[T]) extends QScriptUniform[T, A]

  final case class Map[T[_[_]], A](
      source: A,
      fm: FreeMap[T]) extends QScriptUniform[T, A]

  final case class Read[T[_[_]], A](path: AFile) extends QScriptUniform[T, A]

  // LPish
  final case class Transpose[T[_[_]], A](
      source: A,
      rotations: Rotation) extends QScriptUniform[T, A]

  sealed trait Rotation extends Product with Serializable

  object Rotation {
    case object FlattenArray extends Rotation
    case object FlattenMap extends Rotation
    case object ShiftArray extends Rotation
    case object ShiftMap extends Rotation
  }

  // QScriptish
  final case class LeftShift[T[_[_]], A](
      source: A,
      struct: FreeMap[T],
      idStatus: IdStatus,
      repair: JoinFunc[T]) extends QScriptUniform[T, A]

  // LPish
  final case class LPReduce[T[_[_]], A](
      source: A,
      reduce: ReduceFunc[Unit]) extends QScriptUniform[T, A]

  // QScriptish
  final case class QSReduce[T[_[_]], A](
      source: A,
      buckets: List[FreeMap[T]],
      reducers: List[ReduceFunc[FreeMap[T]]],
      repair: FreeMapA[T, ReduceIndex]) extends QScriptUniform[T, A]

  final case class Distinct[T[_[_]], A](source: A) extends QScriptUniform[T, A]

  // LPish
  final case class Sort[T[_[_]], A](
      source: A,
      order: NEL[(A, SortDir)]) extends QScriptUniform[T, A]

  // QScriptish
  final case class UniformSort[T[_[_]], A](
      source: A,
      buckets: List[FreeMap[T]],
      order: NEL[(FreeMap[T], SortDir)]) extends QScriptUniform[T, A]

  final case class Union[T[_[_]], A](left: A, right: A) extends QScriptUniform[T, A]

  final case class Subset[T[_[_]], A](
      from: A,
      op: SelectionOp,
      count: A) extends QScriptUniform[T, A]

  // LPish
  final case class LPFilter[T[_[_]], A](
      source: A,
      predicate: A) extends QScriptUniform[T, A]

  // QScriptish
  final case class QSFilter[T[_[_]], A](
      source: A,
      predicate: FreeMap[T]) extends QScriptUniform[T, A]

  final case class Nullary[T[_[_]], A](mf: Forall[MapFunc[T, ?]]) extends QScriptUniform[T, A]

  object Constant {

    def apply[T[_[_]], A](ejson: T[EJson])(implicit IC: MapFuncCore[T, ?] :<: MapFunc[T, ?]): QScriptUniform[T, A] =
      Nullary(Forall(_(IC(MapFuncsCore.Constant(ejson)))))

    def unapply[T[_[_]], A](nary: Nullary[T, A])(implicit IC: MapFuncCore[T, ?] :<: MapFunc[T, ?]): Option[T[EJson]] = {
      nary.mf[A] match {
        case IC(MapFuncsCore.Constant(ejson)) => Some(ejson)
        case _ => None
      }
    }
  }

  final case class JoinSideRef[T[_[_]], A](id: Symbol) extends QScriptUniform[T, A]

  final class Optics[T[_[_]]] private () extends QSUTTypes[T] {
    def autojoin[A]: Prism[QScriptUniform[A], (NEL[A], MapFunc[Int])] =
      Prism.partial[QScriptUniform[A], (NEL[A], MapFunc[Int])] {
        case AutoJoin(args, func) => (args, func)
      } { case (args, func) => AutoJoin(args, func) }

    def dimEdit[A]: Prism[QScriptUniform[A], (A, DTrans[T])] =
      Prism.partial[QScriptUniform[A], (A, DTrans[T])] {
        case DimEdit(a, dt) => (a, dt)
      } { case (a, dt) => DimEdit(a, dt) }

    def distinct[A]: Prism[QScriptUniform[A], A] =
      Prism.partial[QScriptUniform[A], A] {
        case Distinct(a) => a
      } (Distinct(_))

    def groupBy[A]: Prism[QScriptUniform[A], (A, A)] =
      Prism.partial[QScriptUniform[A], (A, A)] {
        case GroupBy(l, r) => (l, r)
      } { case (l, r) => GroupBy(l, r) }

    def joinSideRef[A]: Prism[QScriptUniform[A], Symbol] =
      Prism.partial[QScriptUniform[A], Symbol] {
        case JoinSideRef(s) => s
      } (JoinSideRef(_))

    def leftShift[A]: Prism[QScriptUniform[A], (A, FreeMap, IdStatus, JoinFunc)] =
      Prism.partial[QScriptUniform[A], (A, FreeMap, IdStatus, JoinFunc)] {
        case LeftShift(s, fm, ids, jf) => (s, fm, ids, jf)
      } { case (s, fm, ids, jf) => LeftShift(s, fm, ids, jf) }

    def lpFilter[A]: Prism[QScriptUniform[A], (A, A)] =
      Prism.partial[QScriptUniform[A], (A, A)] {
        case LPFilter(s, p) => (s, p)
      } { case (s, p) => LPFilter(s, p) }

    def lpJoin[A]: Prism[QScriptUniform[A], (A, A, A, JoinType, Symbol, Symbol)] =
      Prism.partial[QScriptUniform[A], (A, A, A, JoinType, Symbol, Symbol)] {
        case LPJoin(l, r, c, t, lr, rr) => (l, r, c, t, lr, rr)
      } { case (l, r, c, t, lr, rr) => LPJoin(l, r, c, t, lr, rr) }

    def lpReduce[A]: Prism[QScriptUniform[A], (A, ReduceFunc[Unit])] =
      Prism.partial[QScriptUniform[A], (A, ReduceFunc[Unit])] {
        case LPReduce(a, rf) => (a, rf)
      } { case (a, rf) => LPReduce(a, rf) }

    def map[A]: Prism[QScriptUniform[A], (A, FreeMap)] =
      Prism.partial[QScriptUniform[A], (A, FreeMap)] {
        case Map(a, fm) => (a, fm)
      } { case (a, fm) => Map(a, fm) }

    def nullary[A]: Prism[QScriptUniform[A], Forall[MapFunc]] =
      Prism.partial[QScriptUniform[A], Forall[MapFunc]] {
        case Nullary(mf) => mf
      } (Nullary(_))

    def qsFilter[A]: Prism[QScriptUniform[A], (A, FreeMap)] =
      Prism.partial[QScriptUniform[A], (A, FreeMap)] {
        case QSFilter(a, p) => (a, p)
      } { case (a, p) => QSFilter(a, p) }

    def qsReduce[A]: Prism[QScriptUniform[A], (A, List[FreeMap], List[ReduceFunc[FreeMap]], FreeMapA[ReduceIndex])] =
      Prism.partial[QScriptUniform[A], (A, List[FreeMap], List[ReduceFunc[FreeMap]], FreeMapA[ReduceIndex])] {
        case QSReduce(a, bs, rfs, rep) => (a, bs, rfs, rep)
      } { case (a, bs, rfs, rep) => QSReduce(a, bs, rfs, rep) }

    def read[A]: Prism[QScriptUniform[A], AFile] =
      Prism.partial[QScriptUniform[A], AFile] {
        case Read(f) => f
      } (Read(_))

    def sort[A]: Prism[QScriptUniform[A], (A, NEL[(A, SortDir)])] =
      Prism.partial[QScriptUniform[A], (A, NEL[(A, SortDir)])] {
        case Sort(a, keys) => (a, keys)
      } { case (a, keys) => Sort(a, keys) }

    def subset[A]: Prism[QScriptUniform[A], (A, SelectionOp, A)] =
      Prism.partial[QScriptUniform[A], (A, SelectionOp, A)] {
        case Subset(f, op, c) => (f, op, c)
      } { case (f, op, c) => Subset(f, op, c) }

    def thetaJoin[A]: Prism[QScriptUniform[A], (A, A, JoinFunc, JoinType, JoinFunc)] =
      Prism.partial[QScriptUniform[A], (A, A, JoinFunc, JoinType, JoinFunc)] {
        case ThetaJoin(l, r, c, t, b) => (l, r, c, t, b)
      } { case (l, r, c, t, b) => ThetaJoin(l, r, c, t, b) }

    def transpose[A]: Prism[QScriptUniform[A], (A, Rotation)] =
      Prism.partial[QScriptUniform[A], (A, Rotation)] {
        case Transpose(a, r) => (a, r)
      } { case (a, r) => Transpose(a, r) }

    def uniformSort[A]: Prism[QScriptUniform[A], (A, List[FreeMap], NEL[(FreeMap, SortDir)])] =
      Prism.partial[QScriptUniform[A], (A, List[FreeMap], NEL[(FreeMap, SortDir)])] {
        case UniformSort(a, buckets, keys) => (a, buckets, keys)
      } { case (a, buckets, keys) => UniformSort(a, buckets, keys) }

    def union[A]: Prism[QScriptUniform[A], (A, A)] =
      Prism.partial[QScriptUniform[A], (A, A)] {
        case Union(l, r) => (l, r)
      } { case (l, r) => Union(l, r) }

    def freeMaps[A]: Traversal[QScriptUniform[A], FreeMap] =
      new Traversal[QScriptUniform[A], FreeMap] {
        import Scalaz._

        def modifyF[F[_]: Applicative](f: FreeMap => F[FreeMap])(qsu: QScriptUniform[A]) =
          qsu match {
            case DimEdit(a, DTrans.Group(x)) =>
              f(x) map (y => DimEdit(a, DTrans.Group(y)))

            case LeftShift(a, x, ids, rep) =>
              f(x) map (LeftShift(a, _, ids, rep))

            case Map(a, x) =>
              f(x) map (Map(a, _))

            case QSFilter(a, x) =>
              f(x) map (QSFilter(a, _))

            case QSReduce(a, bs, reds, rep) =>
              (bs.traverse(f) |@| Traverse[List].compose[ReduceFunc].traverse(reds)(f))(QSReduce(a, _, _, rep))

            case UniformSort(s, bs, keys) =>
              (bs.traverse(f) |@| keys.traverse { case (x, d) => f(x) strengthR d })(UniformSort(s, _, _))

            case other => other.point[F]
          }
      }

    def holes[A, B]: PTraversal[QScriptUniform[A], QScriptUniform[B], A, B] =
      PTraversal.fromTraverse[QScriptUniform, A, B]
  }

  object Optics {
    def apply[T[_[_]]]: Optics[T] = new Optics[T]
  }

  final class Dsl[T[_[_]]: BirecursiveT] private () extends QSUTTypes[T] {
    import Scalaz._

    type QSU = T[QScriptUniform]

    private val O = Optics[T]
    private val iso = birecursiveIso[QSU, QScriptUniform]
    private def mfc[A] = PrismNT.inject[MapFuncCore, MapFunc].asPrism[A]

    val autojoin: Prism[QSU, (NEL[QSU], MapFunc[Int])] =
      iso composePrism O.autojoin

    def autojoin2(left: QSU, right: QSU, combiner: (τ, τ) => Binary[T, τ]): QSU =
      autojoin(NEL(left, right), mfc(τ[λ[α => (α, α) => Binary[T, α]]](combiner)[Int](0, 1)))

    def autojoin3(left: QSU, center: QSU, right: QSU, combiner: (τ, τ, τ) => Ternary[T, τ]): QSU =
      autojoin(NEL(left, center, right), mfc(τ[λ[α => (α, α, α) => Ternary[T, α]]](combiner)[Int](0, 1, 2)))

    val dimEdit: Prism[QSU, (QSU, DTrans[T])] =
      iso composePrism O.dimEdit

    val distinct: Prism[QSU, QSU] =
      iso composePrism O.distinct

    val groupBy: Prism[QSU, (QSU, QSU)] =
      iso composePrism O.groupBy

    val joinSideRef: Prism[QSU, Symbol] =
      iso composePrism O.joinSideRef

    val leftShift: Prism[QSU, (QSU, FreeMap, IdStatus, JoinFunc)] =
      iso composePrism O.leftShift

    val lpFilter: Prism[QSU, (QSU, QSU)] =
      iso composePrism O.lpFilter

    val lpJoin: Prism[QSU, (QSU, QSU, QSU, JoinType, Symbol, Symbol)] =
      iso composePrism O.lpJoin

    val lpReduce: Prism[QSU, (QSU, ReduceFunc[Unit])] =
      iso composePrism O.lpReduce

    val map: Prism[QSU, (QSU, FreeMap)] =
      iso composePrism O.map

    def map1(src: QSU, f: MapFuncCore[Hole]): QSU =
      map(src, Free.roll(mfc(f as HoleF[T])))

    val nullary: Prism[QSU, Forall[MapFunc]] =
      iso composePrism O.nullary

    val qsFilter: Prism[QSU, (QSU, FreeMap)] =
      iso composePrism O.qsFilter

    val qsReduce: Prism[QSU, (QSU, List[FreeMap], List[ReduceFunc[FreeMap]], FreeMapA[ReduceIndex])] =
      iso composePrism O.qsReduce

    val read: Prism[QSU, AFile] =
      iso composePrism O.read

    val sort: Prism[QSU, (QSU, NEL[(QSU, SortDir)])] =
      iso composePrism O.sort

    val subset: Prism[QSU, (QSU, SelectionOp, QSU)] =
      iso composePrism O.subset

    val thetaJoin: Prism[QSU, (QSU, QSU, JoinFunc, JoinType, JoinFunc)] =
      iso composePrism O.thetaJoin

    val transpose: Prism[QSU, (QSU, Rotation)] =
      iso composePrism O.transpose

    def tread(file: AFile): QSU =
      transpose(read(file), Rotation.ShiftMap)

    def tread1(name: String): QSU =
      tread(Path.rootDir </> Path.file(name))

    val uniformSort: Prism[QSU, (QSU, List[FreeMap], NEL[(FreeMap, SortDir)])] =
      iso composePrism O.uniformSort

    val union: Prism[QSU, (QSU, QSU)] =
      iso composePrism O.union
  }

  object Dsl {
    def apply[T[_[_]]: BirecursiveT]: Dsl[T] = new Dsl[T]
  }
}
