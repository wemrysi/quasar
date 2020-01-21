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

// Types, existentials or not?
//
sealed trait Change[+F[_], +A] extends Product with Serializable {
  def endOffset: ???
}

final case class Delete(correlationIds: List[???], endOffset: ???) extends Change[Nothing, Nothing]
// or maybe
final case class Delete2(correlationIds: Stream[F, ???]), endOffset: ???) extends Change[F, Nothing]

// AdditiveChange[F, A] ???
sealed trait NonReductiveChange[+F[_], +A] extends Change[F, A]

final case class Create[F[_], A](
  stages: ScalarStages,
  data: Stream[F, A],
  endOffset: ???)
  extends NonReductiveChange[F, A]

final case class Replace[F[_], A](
  stages: ScalarStages,
  correlationId: ???,
  endOffset: ???,
  data: Stream[F, A])
  extends NonReductiveChange[F, A]

// Result
sealed trait CDCResult[F[_], C <: Change] extends Product with Serializable

final case class Parsed[F[_], A, C <: Change[F, A]](
		decode: QDataDecode[A],
		changes: Stream[F, C])
		extends CDCResult[F]

final case class Typed[F[_], C <: Change[F, Byte]](
		format: DataFormat,
		changes: Stream[F, C])
		extends CDCResult[F]
}

final case class Stateful[F[_], C <: Change[F, Byte], P <: Plate[Unit], S](
		format: DataFormat,
		plateF: F[P],
		state: P => F[Option[S]],
		data: Option[S] => Stream[F, C])
		extends CDCResult[F]


/*
 * Datasource needs to provide the offset somehow
 *
 * Keep in mind at least once delivery of events.
 */
trait SomethingDatasource[F[_]] extends Datasource[F, (InterpretedRead[Path], Option[Offset]), CDCResult[F]] {
  ???
}

// One of these per-consumption capability? Just start with all Changes
trait SomethingElseDestination[F[_]] extends Destination[F[_]] {
  // new column information representing correlation id/primary key
  // a way to consume a change stream
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
