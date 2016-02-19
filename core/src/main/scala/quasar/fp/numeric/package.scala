/*
 * Copyright 2014–2016 SlamData Inc.
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

package quasar.fp

import quasar.Predef._

import eu.timepit.refined.numeric.{NonNegative, Positive => RPositive, Greater}
import eu.timepit.refined.refineV
import eu.timepit.refined.api.{RefType, Refined}
import scalaz.{Equal, Show, Monoid}
import scalaz.syntax.show._

package object numeric {

  type Natural = Long Refined NonNegative
  type Positive = Long Refined RPositive

  // Side-step https://issues.scala-lang.org/browse/SI-9581 by avoiding values of limit
  // that are close to Int.MaxValue
  // TODO: Remove after upgrade to Scala 2.11.8
  final class SafeIntForVector private [numeric](val value: Int) extends scala.AnyVal

  object SafeIntForVector {
    val minValue = Int.MinValue
    val maxValue = 10000000
    def apply(a: Int): Option[SafeIntForVector] =
      apply(a.toLong)
    def apply(a: Long): Option[SafeIntForVector] =
      apply(BigInt(a))
    def apply(a: BigInt): Option[SafeIntForVector] =
      if (a <= maxValue) Some(new SafeIntForVector(a.toInt))
      else None
    def unsafe(a: Int): SafeIntForVector =
      apply(a).getOrElse(throw new java.lang.IllegalArgumentException(s"$a is not below maximum value of: $maxValue"))
  }

  trait SafeIntIsIntegral extends scala.math.Integral[SafeIntForVector] {
    def plus(x: SafeIntForVector, y: SafeIntForVector): SafeIntForVector = SafeIntForVector.unsafe(x.value + y.value)
    def minus(x: SafeIntForVector, y: SafeIntForVector): SafeIntForVector = SafeIntForVector.unsafe(x.value - y.value)
    def times(x: SafeIntForVector, y: SafeIntForVector): SafeIntForVector = SafeIntForVector.unsafe(x.value * y.value)
    def quot(x: SafeIntForVector, y: SafeIntForVector): SafeIntForVector = SafeIntForVector.unsafe(x.value / y.value)
    def rem(x: SafeIntForVector, y: SafeIntForVector): SafeIntForVector = SafeIntForVector.unsafe(x.value % y.value)
    def negate(x: SafeIntForVector): SafeIntForVector = SafeIntForVector.unsafe(-x.value)
    def fromInt(x: Int): SafeIntForVector = SafeIntForVector.unsafe(x)
    def toInt(x: SafeIntForVector): Int = x.value
    def toLong(x: SafeIntForVector): Long = x.value.toLong
    def toFloat(x: SafeIntForVector): scala.Float = x.value.toFloat
    def toDouble(x: SafeIntForVector): Double = x.value.toDouble
    def compare(x: SafeIntForVector, y: SafeIntForVector) =
      if (x.value < y.value) -1
      else if (x.value == y.value) 0
      else 1
  }
  implicit object SafeIntIsIntegral extends SafeIntIsIntegral

  def Positive(a: Long): Option[Positive] = refineV[RPositive](a).right.toOption
  def Natural(a: Long): Option[Natural] = refineV[NonNegative](a).right.toOption

  implicit def widenPositive[F[_,_],N](a: F[Int,Greater[N]])(implicit rt: RefType[F]): F[Long,Greater[N]] =
    rt.unsafeWrap(rt.unwrap(a).toLong)

  implicit def widenNatural[F[_,_]](a: F[Int, NonNegative])(implicit rt: RefType[F]): F[Long,NonNegative] =
    rt.unsafeWrap(rt.unwrap(a).toLong)

  implicit def positiveToNatural[F[_,_], A](a: F[A,RPositive])(implicit rt: RefType[F]): F[A, NonNegative] =
    rt.unsafeWrap(rt.unwrap(a))

  implicit def refinedMonoid[F[_,_],T](implicit rt: RefType[F], num: scala.Numeric[T]): Monoid[F[T,NonNegative]] =
    Monoid.instance(
      (a,b) => rt.unsafeWrap(num.plus(rt.unwrap(a), rt.unwrap(b))),
      rt.unsafeWrap(num.zero))

  implicit def refinedEqual[F[_,_],T:Equal,M](implicit rt: RefType[F]): Equal[F[T,M]] = Equal.equalBy(rt.unwrap)

  implicit def refinedShow[F[_,_],T:Show,M](implicit rt: RefType[F]): Show[F[T,M]] = Show.shows(f => rt.unwrap(f).shows)
}
