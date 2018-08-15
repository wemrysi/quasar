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

package quasar.precog.util

import qdata.time.OffsetDate

import java.time.{LocalDate, LocalDateTime, LocalTime, OffsetDateTime, OffsetTime}

object NumericComparisons {

  @inline def compare(a: Long, b: Long): Int = if (a < b) -1 else if (a == b) 0 else 1

  @inline def compare(a: Long, b: Double): Int = -compare(b, a)

  @inline def compare(a: Long, b: BigDecimal): Int = BigDecimal(a) compare b

  def compare(a: Double, bl: Long): Int = {
    val b = bl.toDouble
    if (b.toLong == bl) {
      if (a < b) -1 else if (a == b) 0 else 1
    } else {
      val error = math.abs(b * 2.220446049250313E-16)
      if (a < b - error) -1 else if (a > b + error) 1 else bl.signum
    }
  }

  @inline def compare(a: Double, b: Double): Int = if (a < b) -1 else if (a == b) 0 else 1

  @inline def compare(a: Double, b: BigDecimal): Int = BigDecimal(a) compare b

  @inline def compare(a: BigDecimal, b: Long): Int = a compare BigDecimal(b)

  @inline def compare(a: BigDecimal, b: Double): Int = a compare BigDecimal(b)

  @inline def compare(a: BigDecimal, b: BigDecimal): Int = a compare b

  @inline def compare(a: OffsetDateTime, b: OffsetDateTime): Int = {
    val res: Int = a compareTo b
    if (res < 0) -1
    else if (res > 0) 1
    else 0
  }

  @inline def compare(a: OffsetTime, b: OffsetTime): Int = {
    val res: Int = a compareTo b
    if (res < 0) -1
    else if (res > 0) 1
    else 0
  }

  @inline def compare(a: OffsetDate, b: OffsetDate): Int = {
    val res: Int = a compareTo b
    if (res < 0) -1
    else if (res > 0) 1
    else 0
  }

  @inline def compare(a: LocalDateTime, b: LocalDateTime): Int = {
    val res: Int = a compareTo b
    if (res < 0) -1
    else if (res > 0) 1
    else 0
  }

  @inline def compare(a: LocalTime, b: LocalTime): Int = {
    val res: Int = a compareTo b
    if (res < 0) -1
    else if (res > 0) 1
    else 0
  }

  @inline def compare(a: LocalDate, b: LocalDate): Int = {
    val res: Int = a compareTo b
    if (res < 0) -1
    else if (res > 0) 1
    else 0
  }


  @inline def eps(b: Double): Double = math.abs(b * 2.220446049250313E-16)

  def approxCompare(a: Double, b: Double): Int = {
    val aError = eps(a)
    val bError = eps(b)
    if (a + aError < b - bError) -1 else if (a - aError > b + bError) 1 else 0
  }

  import scalaz.Ordering.{ LT, GT, EQ }

  @inline def order(a: Long, b: Long): scalaz.Ordering =
    if (a < b) LT else if (a == b) EQ else GT

  @inline def order(a: Double, b: Double): scalaz.Ordering =
    if (a < b) LT else if (a == b) EQ else GT

  @inline def order(a: Long, b: Double): scalaz.Ordering =
    scalaz.Ordering.fromInt(compare(a, b))

  @inline def order(a: Double, b: Long): scalaz.Ordering =
    scalaz.Ordering.fromInt(compare(a, b))

  @inline def order(a: Long, b: BigDecimal): scalaz.Ordering =
    scalaz.Ordering.fromInt(compare(a, b))

  @inline def order(a: Double, b: BigDecimal): scalaz.Ordering =
    scalaz.Ordering.fromInt(compare(a, b))

  @inline def order(a: BigDecimal, b: Long): scalaz.Ordering =
    scalaz.Ordering.fromInt(compare(a, b))

  @inline def order(a: BigDecimal, b: Double): scalaz.Ordering =
    scalaz.Ordering.fromInt(compare(a, b))

  @inline def order(a: BigDecimal, b: BigDecimal): scalaz.Ordering =
    scalaz.Ordering.fromInt(compare(a, b))

  @inline def order(a: LocalDateTime, b: LocalDateTime): scalaz.Ordering =
    scalaz.Ordering.fromInt(compare(a, b))

  @inline def order(a: LocalDate, b: LocalDate): scalaz.Ordering =
    scalaz.Ordering.fromInt(compare(a, b))

  @inline def order(a: LocalTime, b: LocalTime): scalaz.Ordering =
    scalaz.Ordering.fromInt(compare(a, b))

  @inline def order(a: OffsetDateTime, b: OffsetDateTime): scalaz.Ordering =
    scalaz.Ordering.fromInt(compare(a, b))

  @inline def order(a: OffsetDate, b: OffsetDate): scalaz.Ordering =
    scalaz.Ordering.fromInt(compare(a, b))

  @inline def order(a: OffsetTime, b: OffsetTime): scalaz.Ordering =
    scalaz.Ordering.fromInt(compare(a, b))
}
