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
import quasar.contrib.pathy.AFile
import quasar.ejson.EJson
import quasar.qscript._

import matryoshka.{BirecursiveT, Delay, EqualT, ShowT}
import scalaz.{:<:, Applicative, Bitraverse, Equal, Forall, NonEmptyList => NEL, Scalaz, Show, Traverse}

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

      case ThetaJoin(left, right, condition, joinType) =>
        (f(left) |@| f(right))(ThetaJoin(_, _, condition, joinType))

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
      joinType: JoinType) extends QScriptUniform[T, A]

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
}
