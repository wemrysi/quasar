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

package quasar.connector.render

import scala._, Predef._

import java.time._

import cats.Applicative

import qdata.time.{DateTimeInterval, OffsetDate}

trait ColumnRender[+A] {
  def renderUndefined(columnName: String): A

  def renderBoolean(columnName: String, value: Boolean): A

  def renderLong(columnName: String, value: Long): A

  def renderDouble(columnName: String, value: Double): A

  def renderBigDecimal(columnName: String, value: BigDecimal): A

  def renderString(columnName: String, value: String): A

  def renderLocalTime(columnName: String, value: LocalTime): A

  def renderOffsetTime(columnName: String, value: OffsetTime): A

  def renderLocalDate(columnName: String, value: LocalDate): A

  def renderOffsetDate(columnName: String, value: OffsetDate): A

  def renderLocalDateTime(columnName: String, value: LocalDateTime): A

  def renderOffsetDateTime(columnName: String, value: OffsetDateTime): A

  def renderInterval(columnName: String, value: DateTimeInterval): A
}

object ColumnRender {
  implicit val columnRenderApplicative: Applicative[ColumnRender] =
    new Applicative[ColumnRender] {
      def pure[A](a: A): ColumnRender[A] =
        new ColumnRender[A] {
          def renderUndefined(columnName: String) = a
          def renderBoolean(columnName: String, value: Boolean) = a
          def renderLong(columnName: String, value: Long) = a
          def renderDouble(columnName: String, value: Double) = a
          def renderBigDecimal(columnName: String, value: BigDecimal) = a
          def renderString(columnName: String, value: String) = a
          def renderLocalTime(columnName: String, value: LocalTime) = a
          def renderOffsetTime(columnName: String, value: OffsetTime) = a
          def renderLocalDate(columnName: String, value: LocalDate) = a
          def renderOffsetDate(columnName: String, value: OffsetDate) = a
          def renderLocalDateTime(columnName: String, value: LocalDateTime) = a
          def renderOffsetDateTime(columnName: String, value: OffsetDateTime) = a
          def renderInterval(columnName: String, value: DateTimeInterval) = a
        }

      def ap[A, B](ff: ColumnRender[A => B])(fa: ColumnRender[A]): ColumnRender[B] =
        new ColumnRender[B] {
          def renderUndefined(columnName: String) =
            ff.renderUndefined(columnName)(fa.renderUndefined(columnName))

          def renderBoolean(columnName: String, value: Boolean) =
            ff.renderBoolean(columnName, value)(fa.renderBoolean(columnName, value))

          def renderLong(columnName: String, value: Long) =
            ff.renderLong(columnName, value)(fa.renderLong(columnName, value))

          def renderDouble(columnName: String, value: Double) =
            ff.renderDouble(columnName, value)(fa.renderDouble(columnName, value))

          def renderBigDecimal(columnName: String, value: BigDecimal) =
            ff.renderBigDecimal(columnName, value)(fa.renderBigDecimal(columnName, value))

          def renderString(columnName: String, value: String) =
            ff.renderString(columnName, value)(fa.renderString(columnName, value))

          def renderLocalTime(columnName: String, value: LocalTime) =
            ff.renderLocalTime(columnName, value)(fa.renderLocalTime(columnName, value))

          def renderOffsetTime(columnName: String, value: OffsetTime) =
            ff.renderOffsetTime(columnName, value)(fa.renderOffsetTime(columnName, value))

          def renderLocalDate(columnName: String, value: LocalDate) =
            ff.renderLocalDate(columnName, value)(fa.renderLocalDate(columnName, value))

          def renderOffsetDate(columnName: String, value: OffsetDate) =
            ff.renderOffsetDate(columnName, value)(fa.renderOffsetDate(columnName, value))

          def renderLocalDateTime(columnName: String, value: LocalDateTime) =
            ff.renderLocalDateTime(columnName, value)(fa.renderLocalDateTime(columnName, value))

          def renderOffsetDateTime(columnName: String, value: OffsetDateTime) =
            ff.renderOffsetDateTime(columnName, value)(fa.renderOffsetDateTime(columnName, value))

          def renderInterval(columnName: String, value: DateTimeInterval) =
            ff.renderInterval(columnName, value)(fa.renderInterval(columnName, value))
        }
    }
}
