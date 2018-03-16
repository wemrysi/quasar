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

import quasar.contrib.matryoshka.envT
import quasar.ejson.TypeTag
import quasar.tpe.{SimpleType, TypeF}

import matryoshka.Corecursive
import scalaz.{\/, IList}
import scalaz.syntax.either._
import spire.algebra.AdditiveSemigroup

object strings {
  import StructuralType.{TagST, TypeST, STF}

  val StringTag = TypeTag("_ejson.string")

  /** An sst annotated with the given stats for a string of unknown characters. */
  def lubString[T, J, A: AdditiveSemigroup](ts: TypeStat[A])(
    implicit C: Corecursive.Aux[T, SSTF[J, A, ?]]
  ): SSTF[J, A, T] = {
    val charSst =
      C.embed(envT(TypeStat.count(ts.size), TypeST(TypeF.simple(SimpleType.Char))))

    stringArr(ts, charSst.right)
  }

  def stringArr[T, L, V](v: V, t: IList[T] \/ T)(
    implicit C: Corecursive.Aux[T, STF[L, V, ?]]
  ): STF[L, V, T] =
    envT(v, TagST[L](Tagged(StringTag, C.embed(envT(v, TypeST(TypeF.arr(t)))))))
}
