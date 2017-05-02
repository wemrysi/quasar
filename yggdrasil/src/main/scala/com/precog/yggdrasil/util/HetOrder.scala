/*
 *  ____    ____    _____    ____    ___     ____
 * |  _ \  |  _ \  | ____|  / ___|  / _/    / ___|        Precog (R)
 * | |_) | | |_) | |  _|   | |     | |  /| | |  _         Advanced Analytics Engine for NoSQL Data
 * |  __/  |  _ <  | |___  | |___  |/ _| | | |_| |        Copyright (C) 2010 - 2013 SlamData, Inc.
 * |_|     |_| \_\ |_____|  \____|   /__/   \____|        All Rights Reserved.
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either version
 * 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See
 * the GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along with this
 * program. If not, see <http://www.gnu.org/licenses/>.
 *
 */
package quasar.yggdrasil
package util

import blueeyes._
import com.precog.util.NumericComparisons

/**
  * Compare values of different types.
  */
trait HetOrder[@specialized(Boolean, Long, Double, AnyRef) A, @specialized(Boolean, Long, Double, AnyRef) B] {
  def compare(a: A, b: B): Int
}

trait HetOrderLow {
  def reverse[@specialized(Boolean, Long, Double, AnyRef) A, @specialized(Boolean, Long, Double, AnyRef) B](ho: HetOrder[A, B]) =
    new HetOrder[B, A] {
      def compare(b: B, a: A) = {
        val cmp = ho.compare(a, b)
        if (cmp < 0) 1 else if (cmp == 0) 0 else -1
      }
    }

  implicit def fromOrder[@specialized(Boolean, Long, Double, AnyRef) A](implicit o: SpireOrder[A]) = new HetOrder[A, A] {
    def compare(a: A, b: A) = o.compare(a, b)
  }
}

object HetOrder extends HetOrderLow {
  implicit def fromScalazOrder[A](implicit z: ScalazOrder[A]) = new HetOrder[A, A] {
    def compare(a: A, b: A): Int = z.order(a, b).toInt
  }

  implicit def DoubleLongOrder       = reverse(LongDoubleOrder)
  implicit def BigDecimalLongOrder   = reverse(LongBigDecimalOrder)
  implicit def BigDecimalDoubleOrder = reverse(DoubleBigDecimalOrder)

  implicit object LongDoubleOrder extends HetOrder[Long, Double] {
    def compare(a: Long, b: Double): Int = NumericComparisons.compare(a, b)
  }

  implicit object LongBigDecimalOrder extends HetOrder[Long, BigDecimal] {
    def compare(a: Long, b: BigDecimal): Int = NumericComparisons.compare(a, b)
  }

  implicit object DoubleBigDecimalOrder extends HetOrder[Double, BigDecimal] {
    def compare(a: Double, b: BigDecimal): Int = NumericComparisons.compare(a, b)
  }

  @inline final def apply[@specialized(Boolean, Long, Double, AnyRef) A, @specialized(Boolean, Long, Double, AnyRef) B](implicit ho: HetOrder[A, B]) = ho
}
