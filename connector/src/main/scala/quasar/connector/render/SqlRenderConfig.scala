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
import java.time.format.DateTimeFormatter

import qdata.time.{DateTimeInterval, OffsetDate}

/** Describes how to render column values to SQL literals. */
trait SqlRenderConfig {
  def renderUndefined: String

  def renderBoolean(columnName: String, value: Boolean): String

  def renderLong(columnName: String, value: Long): String

  def renderDouble(columnName: String, value: Double): String

  def renderBigDecimal(columnName: String, value: BigDecimal): String

  def renderString(columnName: String, value: String): String

  def renderLocalTime(columnName: String, value: LocalTime): String

  def renderOffsetTime(columnName: String, value: OffsetTime): String

  def renderLocalDate(columnName: String, value: LocalDate): String

  def renderOffsetDate(columnName: String, value: OffsetDate): String

  def renderLocalDateTime(columnName: String, value: LocalDateTime): String

  def renderOffsetDateTime(columnName: String, value: OffsetDateTime): String

  def renderInterval(columnName: String, value: DateTimeInterval): String
}

object SqlRenderConfig {
  import DateTimeFormatter._

  class Default extends SqlRenderConfig {
    def renderUndefined: String = "NULL"

    def renderBoolean(columnName: String, value: Boolean): String =
      value.toString

    def renderLong(columnName: String, value: Long): String =
      value.toString

    def renderDouble(columnName: String, value: Double): String =
      value.toString

    def renderBigDecimal(columnName: String, value: BigDecimal): String =
      value.toString

    def renderString(columnName: String, value: String): String =
      value.toString

    def renderLocalTime(columnName: String, value: LocalTime): String =
      ISO_LOCAL_TIME.format(value)

    def renderOffsetTime(columnName: String, value: OffsetTime): String =
      ISO_OFFSET_TIME.format(value)

    def renderLocalDate(columnName: String, value: LocalDate): String =
      ISO_LOCAL_DATE.format(value)

    def renderOffsetDate(columnName: String, value: OffsetDate): String =
      ISO_OFFSET_DATE.format(value)

    def renderLocalDateTime(columnName: String, value: LocalDateTime): String =
      ISO_LOCAL_DATE_TIME.format(value)

    def renderOffsetDateTime(columnName: String, value: OffsetDateTime): String =
      ISO_DATE_TIME.format(value)

    def renderInterval(columnName: String, value: DateTimeInterval): String =
      value.toString
  }
}
