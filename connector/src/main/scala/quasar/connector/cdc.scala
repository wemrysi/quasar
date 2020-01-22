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

// CDC Query
// (InterpretedRead[Path], Offset) => CDCResult

// Changes
//
// How to communicate/negotiate which changes are supported by datasource + destinations


// Types, existentials or not?
//
sealed trait Change[+F[_], +O, +A] extends Product with Serializable {
  def endOffset: O
}

final case class Delete(correlationIds: List[???], endOffset: O) extends Change[Nothing, O, Nothing]
// or maybe
final case class Delete2(correlationIds: Stream[F, Array[Byte]], endOffset: O) extends Change[F, Nothing]

// AdditiveChange[F, A] ???
sealed trait NonReductiveChange[+F[_], +O, +A] extends Change[F, O, A]

final case class Create[F[_], O, A](
    stages: ScalarStages,
    data: Stream[F, A],
    endOffset: O)
    extends NonReductiveChange[F, O, A]

final case class Replace[F[_], O, A](
    stages: ScalarStages,
    correlationId: Array[Byte],
    endOffset: O,
    data: Stream[F, A])
    extends NonReductiveChange[F, O, A]

// Result
sealed trait CDCResult[F[_], C <: Change] extends Product with Serializable

final case class Parsed[F[_], A, C <: Change[F, A]](
		decode: QDataDecode[A],
		changes: Stream[F, C])
		extends CDCResult[F]

class TableColumns(correlationId: TableColumn, columns: List[TableColumn])

/*
 * Datasource needs to provide the offset somehow
 *
 * Keep in mind at least once delivery of events.
 */

// TODO: name

trait Loader[F[_], -O, -A] extends Product with Serializable
final case class Full[F[_], A](f: A => Stream[F, QueryResult[F]])
final case class Delta[F[_], O, A](f: (A, Option[O]) => Stream[F, CDCResult[F]])

trait SomethingDatasource[F[_]] extends Datasource[F, A] {

  type Offset

  def offsetCodec: Codec[Offset]

  def loaders: NonEmptyList[Loader[Offset, A]]

  def prefixedChildPaths(path: ResourcePath)

  def isResource(path: ResourcePath)

/*
  def evaluate(read: IR): Option[Stream[F, QueryResult[F]]]

  def delta(read: IR, offset: Option[Offset]): Option[Stream[F, CDCResult[F]]]

  // What if the the initial backfill doesn't complete?
  def backfill(read: IR, offset: Option[Offset]): Option[Stream[F, CDCResult[F]]]
*/
}

// For when a destination cannot support a column as a correlation id
final case class UnsupportedCorrelationColumn(col: TableColumn) extends PushError

// Given an id that can be used to restart, reference, remove, etc this push definition
//
// Full Push
// Delta Push
// Backfill (Full + Delta without losing events)
//
final case class Push[T, D](
    tableId: T,
    destinationId: D,
    columns: TableColumns,
    path: ResourcePath,
    format: ResultType)

// push changes: ??? Resumption Push, Delta Push
// push everything: ???, Full Push, Fresh Push, Total Push, Complete Push
//
// Do we need to be able to "reset" a delta push to start from the beginning?
//  * May not be useful, or be able to actually reset very far into the past
//  * If needed, then want to expose some sort of capability reporting from destination
//
// Backfill?
//  * Is starting from whatever the current delta stream can provide good enough?
//  * Do we need to incorporate historical records and then delta load from some point in time?
//  * Want to allow for any help the backend can provide in enabling backfill

val evaluator = ???

Stream[F, Change[A]] => Stream[F, Unit]

flatMap { change =>
  case Create(stages, data, offset) =>
    evaluate(stages, data)
      .flatMap(_.renderCsv(createSink.format))
      .flatMap(createSink.f(path, columsn, _)) ++
      Stream.eval_(commitOffset)

  case Delete(ids, offset) =>
    deleteSink.f(path, columns, ids) ++ Stream.eval_(commitOffset)
}

object ResultSink {
  // Many rows
  final case class CreateSink(
    format: RenderFormat,
    f: (ResourcePath, TableColumns, Stream[F, Byte]) => Stream[F, Unit])

  // One row
  final case class ReplaceSink(
    format: RenderFormat,
    f: (ResourcePath, TableColumns, Array[Byte], Stream[F, Byte]) => Stream[F, Unit])

  // Many rows
  final case class DeleteSink(
    format: RenderFormat,
    f: (ResourcePath, TableColumns, Stream[F, Array[Byte]]) => Stream[F, Unit])
}

// Errors/push behavior when capabilities differ between sourc/dest?

// How do we handle the REFORM table definition changing after having been pushed with CDC?
//  * Check for compatibility with existing?
//  * If incompatible, refuse and require they make a new push
//
// One of these per-consumption capability? Just start with all Changes
trait SomethingElseDestination[F[_]] extends Destination[F[_]] {
  // new column information representing correlation id/primary key
  // a way to consume a change stream

  def sinks: NonEmptyList[ResultSink[F]]
}

// something like this?
def evaluate[C <: Change[F, A]](changes: Stream[F, C]): Stream[F, Change[MimirRepr]] =
	changes evalMap {
		case Create(stages, data) =>
			underlying
				.evaluate(stages, data)
				.map(Create(ScalarStages.Id, _))

		case Update(stages, id, data) =>
			underlying
				.evaluate(stages, data)
        .map(Update(ScalarStages.Id, id, _))

		case d @ Delete(_) => d.pure[F]
	}
