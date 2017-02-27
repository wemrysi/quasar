/*
 * Copyright 2014–2017 SlamData Inc.
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

package quasar.physical.fallback.fs

import quasar.Predef._

trait NumericAlgebra[A] {
  def negate(x: A): A
  def plus(x: A, y: A): A
  def minus(x: A, y: A): A
  def times(x: A, y: A): A
  def div(x: A, y: A): A
  def mod(x: A, y: A): A
  def pow(x: A, y: A): A
}
object NumericAlgebra {
  import quasar.Data

  def apply[A](implicit z: NumericAlgebra[A]): NumericAlgebra[A] = z

  implicit object NumericAlgebraData extends NumericAlgebra[Data] {
    def undef = Data.NA

    def negate(x: Data): Data = x match {
      case Data.Int(v)      => Data.Int(-v)
      case Data.Dec(v)      => Data.Dec(-v)
      case Data.Interval(v) => Data.Interval(v.negated())
      case _                => undef
    }

    def plus(d1: Data, d2: Data): Data = (d1, d2) match {
      case (Data.Int(a), Data.Int(b))            => Data.Int(a + b)
      case (Data.Int(a), Data.Dec(b))            => Data.Dec(BigDecimal(a) + b)
      case (Data.Dec(a), Data.Int(b))            => Data.Dec(a + BigDecimal(b))
      case (Data.Dec(a), Data.Dec(b))            => Data.Dec(a + b)
      case (Data.Interval(a), Data.Interval(b))  => Data.Interval(a.plus(b))
      case (Data.Timestamp(a), Data.Interval(b)) => Data.Timestamp(a.plus(b))
      case (Data.Date(a), Data.Interval(b))      => Data.Date(a.plus(b))
      case (Data.Time(a), Data.Interval(b))      => Data.Time(a.plus(b))
      case _                                     => undef
    }

    def minus(d1: Data, d2: Data): Data = (d1, d2) match {
      case (Data.Dec(a), Data.Dec(b))            => Data.Dec(a - b)
      case (Data.Int(a), Data.Dec(b))            => Data.Dec(BigDecimal(a) - b)
      case (Data.Dec(a), Data.Int(b))            => Data.Dec(a - BigDecimal(b))
      case (Data.Int(a), Data.Int(b))            => Data.Int(a - b)
      case (Data.Interval(a), Data.Interval(b))  => Data.Interval(a.minus(b))
      case (Data.Date(a), Data.Interval(b))      => Data.Date(a.minus(b))
      case (Data.Time(a), Data.Interval(b))      => Data.Time(a.minus(b))
      case (Data.Timestamp(a), Data.Interval(b)) => Data.Timestamp(a.minus(b))
      case _                                     => undef
    }

    def times(d1: Data, d2: Data): Data = (d1, d2)  match {
      case (Data.Dec(a), Data.Dec(b))      => Data.Dec(a * b)
      case (Data.Int(a), Data.Dec(b))      => Data.Dec(BigDecimal(a) * b)
      case (Data.Dec(a), Data.Int(b))      => Data.Dec(a * BigDecimal(b))
      case (Data.Int(a), Data.Int(b))      => Data.Int(a * b)
      case (Data.Interval(a), Data.Dec(b)) => Data.Interval(a.multipliedBy(b.toLong))
      case (Data.Interval(a), Data.Int(b)) => Data.Interval(a.multipliedBy(b.toLong))
      case _                               => undef
    }

    def div(d1: Data, d2: Data): Data = (d1, d2) match {
      case (Data.Dec(a), Data.Dec(b))      => Data.Dec(a / b)
      case (Data.Int(a), Data.Dec(b))      => Data.Dec(BigDecimal(a) / b)
      case (Data.Dec(a), Data.Int(b))      => Data.Dec(a / BigDecimal(b))
      case (Data.Int(a), Data.Int(b))      => Data.Dec(BigDecimal(a) / BigDecimal(b))
      case (Data.Interval(a), Data.Dec(b)) => Data.Interval(a.multipliedBy(b.toLong))
      case (Data.Interval(a), Data.Int(b)) => Data.Interval(a.multipliedBy(b.toLong))
      case _                               => undef
    }

    // TODO other cases?
    def mod(d1: Data, d2: Data) = (d1, d2) match {
      case (Data.Int(a), Data.Int(b))      => Data.Int(a % b)
      case (Data.Int(a), Data.Dec(b))      => ???
      case (Data.Dec(a), Data.Int(b))      => ???
      case (Data.Dec(a), Data.Dec(b))      => ???
      case (Data.Interval(a), Data.Int(b)) => ???
      case (Data.Interval(a), Data.Dec(b)) => ???
      case _                               => undef
    }

    // TODO we loose precision here, consider using https://github.com/non/spire/
    def pow(d1: Data, d2: Data): Data = (d1, d2) match {
      case (Data.Int(a), Data.Int(b)) => Data.Dec(scala.math.pow(a.toDouble, b.toDouble))
      case (Data.Int(a), Data.Dec(b)) => Data.Dec(scala.math.pow(a.toDouble, b.toDouble))
      case (Data.Dec(a), Data.Int(b)) => Data.Dec(scala.math.pow(a.toDouble, b.toDouble))
      case (Data.Dec(a), Data.Dec(b)) => Data.Dec(scala.math.pow(a.toDouble, b.toDouble))
      case _                          => undef
    }
  }
}
