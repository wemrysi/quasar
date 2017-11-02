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

package quasar.qscript.qsu

import slamdata.Predef.{Eq => _, _}
import quasar.fp._
import quasar.fp.ski.ι
import quasar.qscript._
import quasar.qscript.provenance._

import matryoshka._
import scalaz.{Lens => _, _}, Scalaz._

import MapFuncsCore._

// TODO: Still need to add reification once we understand how it should work.
final class QProv[T[_[_]]: BirecursiveT: EqualT]
    extends Dimension[Symbol, Symbol, QProv.P[T]]
    with TTypes[T] {

  type D     = Symbol
  type I     = Symbol
  type PF[A] = QProv.PF[A]
  type P     = QProv.P[T]

  val prov: Prov[D, I, P] =
    Prov[D, I, P](ι)

  def autojoinCondition(ls: Dimensions[P], rs: Dimensions[P])(f: Symbol => FreeMap): JoinFunc = {
    def eqCond(k: JoinKeys.JoinKey[I]): JoinFunc =
      Free.roll(MFC(Eq(f(k.left).as(LeftSide), f(k.right).as(RightSide))))

    autojoinKeys(ls, rs).keys.toNel.fold[JoinFunc](BoolLit(true)) { jks =>
      jks.foldMapLeft1(eqCond)((l, r) => Free.roll(MFC(And(l, eqCond(r)))))
    }
  }
}

object QProv {
  type PF[A]      = ProvF[Symbol, Symbol, A]
  type P[T[_[_]]] = T[PF]

  def apply[T[_[_]]: BirecursiveT: EqualT]: QProv[T] =
    new QProv[T]
}
