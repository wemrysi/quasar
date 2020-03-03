/*
 * Copyright 2020 Precog Data
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

package quasar.api.push

import slamdata.Predef._

import quasar.{Condition, Exhaustive}
import quasar.api.ColumnType

import scala.collection.immutable.SortedMap

import cats.{Applicative, Monad}
import cats.data.{EitherT, NonEmptyList, NonEmptyMap, NonEmptySet}
import cats.implicits._

import shapeless._

import skolems.∃

trait ResultPush[F[_], TableId, DestinationId, Query] {
  import ResultPushError._

  def cancel(tableId: TableId, destinationId: DestinationId)
      : F[Condition[ExistentialError[TableId, DestinationId]]]

  def cancelAll: F[Unit]

  def coerce(destinationId: DestinationId, tpe: ColumnType.Scalar)
      : F[Either[DestinationNotFound[DestinationId], TypeCoercion[CoercedType]]]

  def destinationStatus(destinationId: DestinationId)
      : F[Either[DestinationNotFound[DestinationId], Map[TableId, ∃[PushMeta]]]]

  def start(
      tableId: TableId,
      destinationId: DestinationId,
      push: ∃[Push[?, Query]],
      limit: Option[Long])
      : F[Condition[NonEmptyList[ResultPushError[TableId, DestinationId]]]]

  def resume(tableId: TableId, destinationId: DestinationId, limit: Option[Long])
      : F[Condition[ResultPushError[TableId, DestinationId]]]

  /** Attempts to cancel the push to `destinationId` for each entry `tables`.
    *
    * The resulting `Map` will contain an entry for any push that failed to
    * cancel. An empty `Map` indicates no errors were encountered.
    */
  def cancelThese(
      destinationId: DestinationId,
      tables: NonEmptySet[TableId])(
      implicit F: Applicative[F])
      : F[Map[TableId, ExistentialError[TableId, DestinationId]]] = {

    val failed: Map[TableId, ExistentialError[TableId, DestinationId]] =
      SortedMap.empty(tables.toSortedSet.ordering)

    tables.foldLeft(failed.pure[F]) {
      case (f, tid) =>
        (f, cancel(tid, destinationId)) mapN {
          case (m, Condition.Abnormal(err)) => m.updated(tid, err)
          case (m, Condition.Normal()) => m
        }
    }
  }

  /** Attempts to start a push to `destinationId` for each entry `tables`.
    *
    * The resulting `Map` will contain an entry for any push that failed to
    * start. An empty `Map` indicates all pushes were started successfully.
    */
  def startThese(
      destinationId: DestinationId,
      pushes: NonEmptyMap[TableId, (∃[Push[?, Query]], Option[Long])])(
      implicit F: Applicative[F])
      : F[Map[TableId, NonEmptyList[ResultPushError[TableId, DestinationId]]]] = {

    val pushesM = pushes.toSortedMap

    val failed: Map[TableId, NonEmptyList[ResultPushError[TableId, DestinationId]]] =
      SortedMap.empty(pushesM.ordering)

    pushesM.foldLeft(failed.pure[F]) {
      case (f, (tid, (push, limit))) =>
        (f, start(tid, destinationId, push, limit)) mapN {
          case (m, Condition.Abnormal(errs)) => m.updated(tid, errs)
          case (m, Condition.Normal()) => m
        }
    }
  }

  def coercions(destinationId: DestinationId)(implicit F: Monad[F])
      : F[Either[DestinationNotFound[DestinationId], NonEmptyMap[ColumnType.Scalar, TypeCoercion[CoercedType]]]] = {

    // Ensures the map contains an entry for every ColumType.Scalar
    def coercions0[H <: HList](l: H)(implicit E: Exhaustive[ColumnType.Scalar, H])
        : F[Either[DestinationNotFound[DestinationId], NonEmptyMap[ColumnType.Scalar, TypeCoercion[CoercedType]]]] =
      E.toNel(l)
        .traverse(t => EitherT(coerce(destinationId, t)).tupleLeft(t))
        .map(n => NonEmptyMap.of(n.head, n.tail: _*))
        .value

    coercions0(
      ColumnType.Null ::
      ColumnType.Boolean ::
      ColumnType.LocalTime ::
      ColumnType.OffsetTime ::
      ColumnType.LocalDate ::
      ColumnType.OffsetDate ::
      ColumnType.LocalDateTime ::
      ColumnType.OffsetDateTime ::
      ColumnType.Interval ::
      ColumnType.Number ::
      ColumnType.String ::
      HNil)
  }
}
