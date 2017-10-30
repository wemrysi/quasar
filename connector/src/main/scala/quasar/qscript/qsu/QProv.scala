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
import quasar.ejson._
import quasar.ejson.implicits._
import quasar.fp._
import quasar.qscript._
import quasar.qscript.provenance._

import matryoshka._
import matryoshka.implicits._
import matryoshka.data._
import scalaz.{Lens => _, _}, Scalaz._

import MapFuncsCore._

final class QProv[T[_[_]]: BirecursiveT: EqualT]
    extends Dimension[T[EJson], FreeMapA[T, Symbol], QProv.P[T]]
    with TTypes[T] {

  type D     = T[EJson]
  type I     = FreeMapA[Symbol]
  type PF[A] = QProv.PF[T, A]
  type P     = QProv.P[T]

  val prov: Prov[D, I, P] =
    Prov[D, I, P](ejs => Free.roll(MFC[T, I](Constant(ejs))))

  /** The `JoinFunc` representing the autojoin of the given dimensions. */
  def autojoinCondition(ls: Dimensions[P], rs: Dimensions[P])(f: Symbol => FreeMap): JoinFunc = {
    def eqCond(k: JoinKeys.JoinKey[I]): JoinFunc =
      Free.roll(MFC(Eq(
        k.left.flatMap(f).as(LeftSide),
        k.right.flatMap(f).as(RightSide))))

    autojoinKeys(ls, rs).keys.toNel.fold[JoinFunc](BoolLit(true)) { jks =>
      jks.foldMapLeft1(eqCond)((l, r) => Free.roll(MFC(And(l, eqCond(r)))))
    }
  }

  /** Renames `from` to `to` in the given dimensions. */
  def rename(from: Symbol, to: Symbol, dims: Dimensions[P]): Dimensions[P] = {
    def rename0(sym: Symbol): Symbol =
      (sym === from) ? to | sym

    dims map (_.transCata[P](pfo.identities modify (_ map rename0)))
  }

  ////

  private val pfo = ProvF.Optics[D, I]
}

object QProv {
  type PF[T[_[_]], A] = ProvF[T[EJson], FreeMapA[T, Symbol], A]
  type P[T[_[_]]]     = T[PF[T, ?]]

  def apply[T[_[_]]: BirecursiveT: EqualT]: QProv[T] =
    new QProv[T]
}
