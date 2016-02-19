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

package quasar.fp.numeric

import quasar.Predef._

import scala.math.Numeric

import eu.timepit.refined.api.{ RefType, Validate }
import eu.timepit.refined.numeric._
import eu.timepit.refined.scalacheck.arbitraryRefType
import eu.timepit.refined.scalacheck.numeric.Bounded
import org.scalacheck.{ Arbitrary, Gen }
import org.scalacheck.Gen.Choose
import shapeless.{ Nat, Witness }
import shapeless.ops.nat.ToInt

/**
  * Functions defined in this trait are largely identical to the ones defined in eu.timepit.refined.scalacheck.numeric.
  * These versions are specific to discrete numerical values as opposed to any numerical value. This allows us to avoid
  * using filter which leads to discarded generation attempts and ultimately often to failed proprety validation
  * attempts.
  */
trait NumericArbitrary {

  trait Discrete[A]

  object Discrete {
    implicit val int: Discrete[Int] = new Discrete[Int] {}
    implicit val long: Discrete[Long] = new Discrete[Long] {}
    implicit val short: Discrete[Short] = new Discrete[Short] {}
    implicit val byte: Discrete[Byte] = new Discrete[Byte] {}
    implicit val safeIntForVector: Discrete[SafeIntForVector] = new Discrete[SafeIntForVector] {}
  }

  implicit def lessArbitraryWitDiscrete[F[_, _]: RefType, T: Numeric: Choose: Discrete, N <: T](
     implicit
     bounded: Bounded[T],
     wn: Witness.Aux[N]
   ): Arbitrary[F[T, Less[N]]] =
    rangeClosedOpenArbitrary(bounded.minValue, wn.value)

  implicit def lessArbitraryNatDiscrete[F[_, _]: RefType, T: Choose: Discrete, N <: Nat](
    implicit
    bounded: Bounded[T],
    nt: Numeric[T],
    tn: ToInt[N]
  ): Arbitrary[F[T, Less[N]]] =
    rangeClosedOpenArbitrary(bounded.minValue, nt.fromInt(tn()))

  implicit def greaterArbitraryWitDiscrete[F[_, _]: RefType, T: Numeric: Choose: Discrete, N <: T](
    implicit
    bounded: Bounded[T],
    wn: Witness.Aux[N]
  ): Arbitrary[F[T, Greater[N]]] =
    rangeOpenClosedArbitrary(wn.value, bounded.maxValue)

  implicit def greaterArbitraryNatDiscrete[F[_, _]: RefType, T: Choose: Discrete, N <: Nat](
     implicit
     bounded: Bounded[T],
     nt: Numeric[T],
     tn: ToInt[N]
   ): Arbitrary[F[T, Greater[N]]] =
    rangeOpenClosedArbitrary(nt.fromInt(tn()), bounded.maxValue)

  implicit def intervalOpenArbitraryDiscrete[F[_, _]: RefType, T: Numeric: Choose : Discrete, L <: T, H <: T](
    implicit
    wl: Witness.Aux[L],
    wh: Witness.Aux[H]
  ): Arbitrary[F[T, Interval.Open[L, H]]] =
    rangeOpenArbitrary(wl.value, wh.value)

  implicit def intervalOpenClosedArbitraryDiscrete[F[_, _]: RefType, T: Numeric: Choose : Discrete, L <: T, H <: T](
    implicit
    wl: Witness.Aux[L],
    wh: Witness.Aux[H]
  ): Arbitrary[F[T, Interval.OpenClosed[L, H]]] =
    rangeOpenClosedArbitrary(wl.value, wh.value)

  implicit def intervalClosedOpenArbitraryDiscrete[F[_, _]: RefType, T: Numeric: Choose : Discrete, L <: T, H <: T](
    implicit
    wl: Witness.Aux[L],
    wh: Witness.Aux[H]
  ): Arbitrary[F[T, Interval.ClosedOpen[L, H]]] =
    rangeClosedOpenArbitrary(wl.value, wh.value)

  ///

  private def rangeOpenArbitrary[F[_, _]: RefType, T: Choose: Discrete, P](min: T, max: T)(
    implicit
    nt: Numeric[T]
  ): Arbitrary[F[T, P]] = {
    import nt.mkOrderingOps
    arbitraryRefType(Gen.chooseNum(nt.plus(min,nt.fromInt(1)), nt.minus(max, nt.fromInt(1))))
  }

  private def rangeOpenClosedArbitrary[F[_, _]: RefType, T: Choose: Discrete, P](min: T, max: T)(
    implicit
    nt: Numeric[T]
  ): Arbitrary[F[T, P]] = {
    import nt.mkOrderingOps
    arbitraryRefType(Gen.chooseNum(nt.plus(min,nt.fromInt(1)), max))
  }

  private def rangeClosedOpenArbitrary[F[_, _]: RefType, T: Choose: Discrete, P](min: T, max: T)(
    implicit
    nt: Numeric[T]
  ): Arbitrary[F[T, P]] = {
    import nt.mkOrderingOps
    arbitraryRefType(Gen.chooseNum(min, nt.minus(max, nt.fromInt(1))))
  }
}

object NumericArbitrary extends NumericArbitrary
