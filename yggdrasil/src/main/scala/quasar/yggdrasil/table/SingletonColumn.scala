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

import quasar.precog.BitSet
import qdata.time.{DateTimeInterval, OffsetDate}

import java.time.{LocalDate, LocalDateTime, LocalTime, OffsetDateTime, OffsetTime}

import scala.specialized

abstract class SingletonColumn[@specialized A](value: A) extends ExtensibleColumn {

  override def isDefinedAt(row: Int) = row == 0

  def apply(row: Int): A = value

  lazy val defined: BitSet = {
    val b = new BitSet(1)
    b.set(0)
    b
  }
}

final case class SingletonBoolColumn(value: Boolean)
    extends SingletonColumn[Boolean](value) with BoolColumn {

  lazy val values: BitSet = {
    val bs = new BitSet(1)
    if (value) bs.set(0)
    bs
  }
}

final case class SingletonLongColumn(value: Long)
    extends SingletonColumn[Long](value) with LongColumn {
  lazy val values: Array[Long] = Array(value)
}

final case class SingletonDoubleColumn(value: Double)
    extends SingletonColumn[Double](value) with DoubleColumn {
  lazy val values: Array[Double] = Array(value)
}

final case class SingletonNumColumn(value: BigDecimal)
    extends SingletonColumn[BigDecimal](value) with NumColumn {
  lazy val values: Array[BigDecimal] = Array(value)
}

final case class SingletonStrColumn(value: String)
    extends SingletonColumn[String](value) with StrColumn {
  lazy val values: Array[String] = Array(value)
}

final case class SingletonOffsetDateTimeColumn(value: OffsetDateTime)
    extends SingletonColumn[OffsetDateTime](value) with OffsetDateTimeColumn {
  lazy val values: Array[OffsetDateTime] = Array(value)
}

final case class SingletonOffsetTimeColumn(value: OffsetTime)
    extends SingletonColumn[OffsetTime](value) with OffsetTimeColumn {
  lazy val values: Array[OffsetTime] = Array(value)
}

final case class SingletonOffsetDateColumn(value: OffsetDate)
    extends SingletonColumn[OffsetDate](value) with OffsetDateColumn {
  lazy val values: Array[OffsetDate] = Array(value)
}

final case class SingletonLocalDateTimeColumn(value: LocalDateTime)
    extends SingletonColumn[LocalDateTime](value) with LocalDateTimeColumn {
  lazy val values: Array[LocalDateTime] = Array(value)
}

final case class SingletonLocalTimeColumn(value: LocalTime)
    extends SingletonColumn[LocalTime](value) with LocalTimeColumn {
  lazy val values: Array[LocalTime] = Array(value)
}

final case class SingletonLocalDateColumn(value: LocalDate)
    extends SingletonColumn[LocalDate](value) with LocalDateColumn {
  lazy val values: Array[LocalDate] = Array(value)
}

final case class SingletonIntervalColumn(value: DateTimeInterval)
    extends SingletonColumn[DateTimeInterval](value) with IntervalColumn {
  lazy val values: Array[DateTimeInterval] = Array(value)
}
