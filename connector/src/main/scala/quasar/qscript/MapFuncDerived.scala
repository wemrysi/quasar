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

package quasar.qscript

import matryoshka._
import monocle.macros.Lenses

sealed abstract class MapFuncDerived[T[_[_]], A]

sealed abstract class NullaryDerived[T[_[_]], A] extends MapFuncDerived[T, A]

sealed abstract class UnaryDerived[T[_[_]], A] extends MapFuncDerived[T, A] {
  def a1: A
}
sealed abstract class BinaryDerived[T[_[_]], A] extends MapFuncDerived[T, A] {
  def a1: A
  def a2: A
}
sealed abstract class TernaryDerived[T[_[_]], A] extends MapFuncDerived[T, A] {
  def a1: A
  def a2: A
  def a3: A
}

object MapFuncsDerived {
  @Lenses final case class Abs[T[_[_]], A](a1: A) extends UnaryDerived[T, A]
}
