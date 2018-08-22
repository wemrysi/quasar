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

abstract class SingletonColumn[A](value: A) extends ExtensibleColumn {

  override def isDefinedAt(row: Int) = row == 0

  override def definedAt(from: Int, to: Int): BitSet = SingletonColumn.Defined

}

final class SingletonBoolColumn(val value: Boolean)
    extends SingletonColumn[Boolean](value) with BoolColumn {
  def apply(row: Int): Boolean = value
}

object SingletonBoolColumn {
  lazy val SingleTrue = new SingletonBoolColumn(true)
  lazy val SingleFalse = new SingletonBoolColumn(false)

  def bitset(b: Boolean) =
    if (b) SingletonColumn.BitSet_1
    else SingletonColumn.BitSet_0

  def apply(value: Boolean): SingletonBoolColumn =
    if (value) SingleTrue else SingleFalse
}

final case class SingletonLongColumn(value: Long)
    extends SingletonColumn[Long](value) with LongColumn {
  def apply(row: Int): Long = value
}

final case class SingletonDoubleColumn(value: Double)
    extends SingletonColumn[Double](value) with DoubleColumn {
  def apply(row: Int): Double = value
}

final case class SingletonNumColumn(value: BigDecimal)
    extends SingletonColumn[BigDecimal](value) with NumColumn {
  def apply(row: Int): BigDecimal = value
}

final case class SingletonStrColumn(value: String)
    extends SingletonColumn[String](value) with StrColumn {
  def apply(row: Int): String = value
}

final case class SingletonOffsetDateTimeColumn(value: OffsetDateTime)
    extends SingletonColumn[OffsetDateTime](value) with OffsetDateTimeColumn {
  def apply(row: Int): OffsetDateTime = value
}

final case class SingletonOffsetTimeColumn(value: OffsetTime)
    extends SingletonColumn[OffsetTime](value) with OffsetTimeColumn {
  def apply(row: Int): OffsetTime = value
}

final case class SingletonOffsetDateColumn(value: OffsetDate)
    extends SingletonColumn[OffsetDate](value) with OffsetDateColumn {
  def apply(row: Int): OffsetDate = value
}

final case class SingletonLocalDateTimeColumn(value: LocalDateTime)
    extends SingletonColumn[LocalDateTime](value) with LocalDateTimeColumn {
  def apply(row: Int): LocalDateTime = value
}

final case class SingletonLocalTimeColumn(value: LocalTime)
    extends SingletonColumn[LocalTime](value) with LocalTimeColumn {
  def apply(row: Int): LocalTime = value
}

final case class SingletonLocalDateColumn(value: LocalDate)
    extends SingletonColumn[LocalDate](value) with LocalDateColumn {
  def apply(row: Int): LocalDate = value
}

final case class SingletonIntervalColumn(value: DateTimeInterval)
    extends SingletonColumn[DateTimeInterval](value) with IntervalColumn {
  def apply(row: Int): DateTimeInterval = value
}

object SingletonColumn {
  val BitSet_0: BitSet = new BitSet

  val BitSet_1: BitSet = {
    val bs = new BitSet
    bs.set(0)
    bs
  }

  val Defined: BitSet = BitSet_1
}
