/*
 * Copyright 2014–2018 SlamData Inc.
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

package quasar.yggdrasil.table

import qdata.time.{DateTimeInterval, OffsetDate}

import java.time.{LocalDate, LocalDateTime, LocalTime, OffsetDateTime, OffsetTime}

import scala.specialized

abstract class SingletonColumn[@specialized A](value: A) extends ExtensibleColumn {
  override def isDefinedAt(row: Int) = row == 0
  def apply(row: Int): A = value
}

final case class SingletonBoolColumn(value: Boolean)
  extends SingletonColumn[Boolean](value) with BoolColumn

final case class SingletonLongColumn(value: Long)
  extends SingletonColumn[Long](value) with LongColumn

final case class SingletonDoubleColumn(value: Double)
  extends SingletonColumn[Double](value) with DoubleColumn

final case class SingletonNumColumn(value: BigDecimal)
  extends SingletonColumn[BigDecimal](value) with NumColumn

final case class SingletonStrColumn(value: String)
  extends SingletonColumn[String](value) with StrColumn

final case class SingletonOffsetDateTimeColumn(value: OffsetDateTime)
  extends SingletonColumn[OffsetDateTime](value) with OffsetDateTimeColumn

final case class SingletonOffsetTimeColumn(value: OffsetTime)
  extends SingletonColumn[OffsetTime](value) with OffsetTimeColumn

final case class SingletonOffsetDateColumn(value: OffsetDate)
  extends SingletonColumn[OffsetDate](value) with OffsetDateColumn

final case class SingletonLocalDateTimeColumn(value: LocalDateTime)
  extends SingletonColumn[LocalDateTime](value) with LocalDateTimeColumn

final case class SingletonLocalTimeColumn(value: LocalTime)
  extends SingletonColumn[LocalTime](value) with LocalTimeColumn

final case class SingletonLocalDateColumn(value: LocalDate)
  extends SingletonColumn[LocalDate](value) with LocalDateColumn

final case class SingletonIntervalColumn(value: DateTimeInterval)
  extends SingletonColumn[DateTimeInterval](value) with IntervalColumn
