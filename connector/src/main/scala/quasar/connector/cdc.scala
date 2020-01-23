/*
 * Copyright 2014–2019 SlamData Inc.
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

package quasar.connector

import slamdata.Predef._

import quasar.ScalarStages
import quasar.api.datasource.DatasourceType
import quasar.api.destination._
import quasar.api.push.RenderConfig
import quasar.api.resource._
import quasar.api.table._

import cats.data.NonEmptyList

import qdata.QDataDecode

import fs2.Stream

sealed trait Change[F[_], +O, +A] extends Product with Serializable {
  /** The offset representing the end of the event payload. */
  def endOffset: O
}

final case class Create[F[_], O, A](
    stages: ScalarStages,
    data: Stream[F, A],
    endOffset: O)
    extends Change[F, O, A]

final case class Replace[F[_], O, A](
    stages: ScalarStages,
    correlationId: Array[Byte],
    endOffset: O,
    data: Stream[F, A])
    extends Change[F, O, A]

final case class Delete[F[_], O](
    correlationIds: Stream[F, Array[Byte]],
    endOffset: O)
    extends Change[F, O, Nothing]


sealed trait DeltaResult[F[_], +O] extends Product with Serializable

final case class Parsed[F[_], O, A](
		decode: QDataDecode[A],
		changes: Stream[F, Change[F, O, A]])
		extends DeltaResult[F, O]


class TableColumns(correlationId: Option[TableColumn], columns: List[TableColumn])


// TODO: better name?
trait Loader[F[_], O, -A] extends Product with Serializable
final case class Full[F[_], A](f: A => Stream[F, QueryResult[F]])
final case class Delta[F[_], O, A](f: (A, Option[O]) => Stream[F, DeltaResult[F, O]])

trait Datasource[F[_], G[_], Q, P <: ResourcePathType] {

  type Offset

  def kind: DatasourceType

  def offsetCodec: scodec.Codec[Offset]

  def loaders: NonEmptyList[Loader[F, Offset, Q]]

  def pathIsResource(path: ResourcePath): F[Boolean]

  def prefixedChildPaths(prefixPath: ResourcePath)
      : F[Option[G[(ResourceName, P)]]]
}

/** Indicates a destination cannot support a column as a correlation id. */
final case class UnsupportedCorrelationColumn(col: TableColumn)// extends ResultPushError

/** First class push representation, enables resuming delta pushes and redoing full pushes. */
final case class Push[T, D](
    tableId: T,
    destinationId: D,
    columns: TableColumns,
    path: ResourcePath,
    format: ResultType)


/** Allows for Destinations to handle what they are able. */
sealed trait ResultSink[F[_]] extends Product with Serializable

final case class CreateSink[F[_]](
    format: RenderConfig,
    ingest: (ResourcePath, TableColumns, Stream[F, Byte]) => Stream[F, Unit])
    extends ResultSink[F]

final case class ReplaceSink[F[_]](
    format: RenderConfig,
    ingest: (ResourcePath, TableColumns, Array[Byte], Stream[F, Byte]) => Stream[F, Unit])
    extends ResultSink[F]

final case class DeleteSink[F[_]](
    format: RenderConfig,
    ingest: (ResourcePath, TableColumns, Stream[F, Array[Byte]]) => Stream[F, Unit])
    extends ResultSink[F]


trait Destination[F[_]] {
  def kind: DestinationType

  def sinks: NonEmptyList[ResultSink[F]]
}
