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
import scalaz.{:<:, Equal, NonEmptyList => NEL, Traverse}

sealed trait QScriptUniform[T[_[_]], A] extends Product with Serializable

object QScriptUniform {

  @SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
  implicit def traverse[T[_[_]]]: Traverse[QScriptUniform[T, ?]] = ???

  @SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
  implicit def renderTree[T[_[_]]: RenderTreeT: ShowT]
      : Delay[RenderTree, QScriptCore[T, ?]] = ???

  @SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
  implicit def equal[T[_[_]]: BirecursiveT: EqualT]: Delay[Equal, QScriptCore[T, ?]] = ???

  final case class AutoJoin[T[_[_]], A](
      sources: NEL[A],
      combiner: MapFunc[T, Int]) extends QScriptUniform[T, A]

  final case class GroupBy[T[_[_]], A](
      left: A,
      right: A) extends QScriptUniform[T, A]

  final case class DimEdit[T[_[_]], A](
      source: A,
      trans: DTrans[A]) extends QScriptUniform[T, A]

  sealed trait DTrans[A] extends Product with Serializable

  object DTrans {
    final case class Squash[A]() extends DTrans[A]
    final case class PushValue[A](source: A) extends DTrans[A]
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
      reducers: List[ReduceFunc[FreeMap[T]]],
      buckets: List[FreeMap[T]],
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

  final case class Nullary[T[_[_]], A, B](mf: MapFunc[T, B]) extends QScriptUniform[T, A]

  object Constant {

    def apply[T[_[_]], A](ejson: T[EJson])(implicit IC: MapFuncCore[T, ?] :<: MapFunc[T, ?]): QScriptUniform[T, A] =
      Nullary(IC(MapFuncsCore.Constant(ejson)))

    def unapply[T[_[_]], A, B](nary: Nullary[T, A, B])(implicit IC: MapFuncCore[T, ?] :<: MapFunc[T, ?]): Option[T[EJson]] = {
      nary match {
        case Nullary(IC(MapFuncsCore.Constant(ejson))) => Some(ejson)
        case _ => None
      }
    }
  }

  final case class JoinSideRef[T[_[_]], A](id: Symbol) extends QScriptUniform[T, A]
}
