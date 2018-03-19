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

package quasar.sst

import slamdata.Predef._
import quasar.contrib.matryoshka.envT
import quasar.ejson.{EJson, TypeTag}
import quasar.tpe.{SimpleType, TypeF}

import scala.Char

import matryoshka.{Corecursive, Recursive}
import scalaz.Order
import scalaz.syntax.either._
import spire.algebra.{AdditiveSemigroup, Field}
import spire.math.ConvertableTo


object strings {
  import StructuralType.{TagST, TypeST, STF}

  val StructuralString = TypeTag("_structural.string")

  /** An sst annotated with the given stats for a string of unknown characters. */
  def lubString[T, J, A: AdditiveSemigroup](ts: TypeStat[A])(
    implicit C: Corecursive.Aux[T, SSTF[J, A, ?]]
  ): SSTF[J, A, T] = {
    val charSst =
      C.embed(envT(
        TypeStat.char(ts.size, Char.MinValue, Char.MaxValue),
        TypeST(TypeF.simple(SimpleType.Char))))

    stringTagged(ts, C.embed(envT(ts, TypeST(TypeF.arr(charSst.right)))))
  }

  /** Widens a string into an array of characters.
    *
    * FIXME: Overly specific, define in terms of [Co]Recursive.
    */
  def widenString[J: Order, A: ConvertableTo: Field: Order](count: A, s: String)(
    implicit
    JC: Corecursive.Aux[J, EJson],
    JR: Recursive.Aux[J, EJson]
  ): SSTF[J, A, SST[J, A]] = {
    val charArr =
      SST.fromEJson(count, EJson.arr(s.map(EJson.char[J](_)) : _*))

    stringTagged(charArr.copoint, charArr)
  }

  ////

  private def stringTagged[T, L, V](v: V, t: T): STF[L, V, T] =
    envT(v, TagST[L](Tagged(StructuralString, t)))
}
