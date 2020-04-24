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

package quasar.connector.destination

import slamdata.Predef._

import quasar.api.Column
import quasar.api.push.OffsetKey
import quasar.api.resource.ResourcePath
import quasar.connector._
import quasar.connector.render.RenderConfig

import cats.data.NonEmptyList

import fs2.Stream

import skolems.∀

sealed trait ResultSink[F[_], T] extends Product with Serializable

object ResultSink {
  final case class CreateSink[F[_], T](
      config: RenderConfig,
      consume: (ResourcePath, NonEmptyList[Column[T]], Stream[F, Byte]) => Stream[F, Unit])
      extends ResultSink[F, T]

  object UpsertSink {
    final case class Args[F[_], T, A](
        path: ResourcePath,
        idColumn: Column[T],
        otherColumns: List[Column[T]],
        writeMode: WriteMode,
        input: Stream[F, DataEvent[_, OffsetKey.Actual[A]]]) {

      def columns: NonEmptyList[Column[T]] =
        NonEmptyList(idColumn, otherColumns)
    }
  }

  final case class UpsertSink[F[_], T](
      renderConfig: RenderConfig.Csv,
      consume: ∀[λ[α => UpsertSink.Args[F, T, α] => Stream[F, OffsetKey.Actual[α]]]])
      extends ResultSink[F, T]

  def create[F[_], T](
      config: RenderConfig)(
      consume: (ResourcePath, NonEmptyList[Column[T]], Stream[F, Byte]) => Stream[F, Unit])
      : ResultSink[F, T] =
    CreateSink(config, consume)

  def upsert[F[_], T](
      config: RenderConfig.Csv)(
      consume: ∀[λ[α => UpsertSink.Args[F, T, α] => Stream[F, OffsetKey.Actual[α]]]])
      : ResultSink[F, T] =
    UpsertSink(config, consume)
}
