package ygg.table

import blueeyes._

/**
  * Compare values of different types.
  */
trait HetOrder[@spec(Boolean, Long, Double, AnyRef) A, @spec(Boolean, Long, Double, AnyRef) B] {
  def compare(a: A, b: B): Int
}

trait HetOrderLow {
  def reverse[@spec(Boolean, Long, Double, AnyRef) A, @spec(Boolean, Long, Double, AnyRef) B](ho: HetOrder[A, B]) =
    new HetOrder[B, A] {
      def compare(b: B, a: A) = {
        val cmp = ho.compare(a, b)
        if (cmp < 0) 1 else if (cmp == 0) 0 else -1
      }
    }

  implicit def fromOrder[@spec(Boolean, Long, Double, AnyRef) A](implicit o: Ord[A]) = new HetOrder[A, A] {
    def compare(a: A, b: A) = o.order(a, b).toInt
  }
}

object HetOrder extends HetOrderLow {
  implicit def fromScalazOrder[A](implicit z: Ord[A]) = new HetOrder[A, A] {
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

  @inline final def apply[@spec(Boolean, Long, Double, AnyRef) A, @spec(Boolean, Long, Double, AnyRef) B](implicit ho: HetOrder[A, B]) = ho
}
