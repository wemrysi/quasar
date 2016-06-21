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

package quasar.qscript

import quasar.Predef._

import matryoshka._
import monocle.Prism

class MapFuncs[T[_[_]], A] {
  // array
  def Length(a1: A) = Unary[T, A](a1)

  // date
  def Date(a1: A) = Unary[T, A](a1)
  def Time(a1: A) = Unary[T, A](a1)
  def Timestamp(a1: A) = Unary[T, A](a1)
  def Interval(a1: A) = Unary[T, A](a1)
  def TimeOfDay(a1: A) = Unary[T, A](a1)
  def ToTimestamp(a1: A) = Unary[T, A](a1)
  def Extract(a1: A, a2: A) = Binary[T, A](a1, a2)

  // math
  def Negate(a1: A) = Unary[T, A](a1)
  def Add(a1: A, a2: A) = Binary[T, A](a1, a2)
  def Multiply(a1: A, a2: A) = Binary[T, A](a1, a2)
  def Subtract(a1: A, a2: A) = Binary[T, A](a1, a2)
  def Divide(a1: A, a2: A) = Binary[T, A](a1, a2)
  def Modulo(a1: A, a2: A) = Binary[T, A](a1, a2)
  def Power(a1: A, a2: A) = Binary[T, A](a1, a2)

  // relations
  def Not(a1: A) = Unary[T, A](a1)
  def Eq(a1: A, a2: A) = Binary[T, A](a1, a2)
  def Neq(a1: A, a2: A) = Binary[T, A](a1, a2)
  def Lt(a1: A, a2: A) = Binary[T, A](a1, a2)
  def Lte(a1: A, a2: A) = Binary[T, A](a1, a2)
  def Gt(a1: A, a2: A) = Binary[T, A](a1, a2)
  def Gte(a1: A, a2: A) = Binary[T, A](a1, a2)
  def IfUndefined(a1: A, a2: A) = Binary[T, A](a1, a2)
  def And(a1: A, a2: A) = Binary[T, A](a1, a2)
  def Or(a1: A, a2: A) = Binary[T, A](a1, a2)
  def Coalesce(a1: A, a2: A) = Binary[T, A](a1, a2)
  def Between(a1: A, a2: A, a3: A) = Ternary[T, A](a1, a2, a3)
  def Cond(a1: A, a2: A, a3: A) = Ternary[T, A](a1, a2, a3)

  // set
  def In(a1: A, a2: A) = Binary[T, A](a1, a2)
  def Within(a1: A, a2: A) = Binary[T, A](a1, a2)
  def Constantly(a1: A, a2: A) = Binary[T, A](a1, a2)

  // string
  def Lower(a1: A) = Unary[T, A](a1)
  def Upper(a1: A) = Unary[T, A](a1)
  def Boolean(a1: A) = Unary[T, A](a1)
  def Integer(a1: A) = Unary[T, A](a1)
  def Decimal(a1: A) = Unary[T, A](a1)
  def Null(a1: A) = Unary[T, A](a1)
  def ToString(a1: A) = Unary[T, A](a1)
  def Like(a1: A, a2: A, a3: A) = Ternary[T, A](a1, a2, a3)
  def Search(a1: A, a2: A, a3: A) = Ternary[T, A](a1, a2, a3)
  def Substring(a1: A, a2: A, a3: A) = Ternary[T, A](a1, a2, a3)

  // structural
  def MakeArray(a1: A) = Unary[T, A](a1)
  val MakeObject = Prism.partial[MapFunc[T, A], (A, A)] {
    case Binary(a1, a2) => (a1, a2)
  } ((Binary[T, A](_, _)).tupled)
  val ConcatArrays = Prism.partial[MapFunc[T, A], List[A]] {
    case Variadic(as) => as
  } (Variadic[T, A](_))
  val ConcatObjects = Prism.partial[MapFunc[T, A], List[A]] {
    case Variadic(as) => as
  } (Variadic[T, A](_))
  def ProjectIndex(a1: A, a2: A) = Binary[T, A](a1, a2)
  val ProjectField = Prism.partial[MapFunc[T, A], (A, A)] {
    case Binary(a1, a2) => (a1, a2)
  } ((Binary[T, A](_, _)).tupled)
  def DeleteField(a1: A, a2: A) = Binary[T, A](a1, a2)

  // helpers & QScript-specific
  def StrLit(str: String)(implicit T: Corecursive[T]) =
    Nullary[T, A](EJson.Str[T[EJson]](str).embed)
}
