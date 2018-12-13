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

package quasar.qscript.rewrites

import slamdata.Predef.{None, Option, Some, String}

import matryoshka.{Hole => _, _}
import matryoshka.implicits._
import matryoshka.patterns.{InitF, LastF, NelF}

import quasar.contrib.iota._
import quasar.contrib.scalaz.free._
import quasar.qscript._
import quasar.qscript.RecFreeS._

import scalaz.NonEmptyList
import scalaz.std.option._

/**
 * Matches on a `FreeMap` consisting only of (at least one) make
 * map, returning the NEL[String] implied by the nested structure.
 */
object Wrapped {
  import MapFuncsCore.{MakeMap, StrLit}

  def unapply[T[_[_]]: BirecursiveT](fm: FreeMap[T]): Option[NonEmptyList[String]] =
    wrapped(fm)

  def wrapped[T[_[_]]: BirecursiveT](fm: FreeMap[T]): Option[NonEmptyList[String]] = {
    val alg: Algebra[NelF[String, ?], NonEmptyList[String]] = {
      case LastF(s) => NonEmptyList(s)
      case InitF(s, nel) => s <:: nel
    }

    val coalg: CoalgebraM[Option, NelF[String, ?], FreeMap[T]] = {
      case ExtractFunc(MakeMap(StrLit(key), FreeA(_))) =>
        Some(LastF(key))

      case ExtractFunc(MakeMap(StrLit(key), value)) =>
        Some(InitF(key, value))

      case _ => None
    }

    fm.hyloM(alg andThen some, coalg)
  }
}

object WrappedRec {
  def unapply[T[_[_]]: BirecursiveT](fm: RecFreeMap[T]): Option[NonEmptyList[String]] =
    Wrapped.wrapped(fm.linearize)
}
