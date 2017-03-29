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

package quasar

import slamdata.Predef._
import quasar.contrib.matryoshka._
import quasar.{ejson => ejs}
import quasar.ejson.{CommonEJson => C, EJson, ExtEJson => E}

import algebra.PartialOrder
import algebra.lattice._
import matryoshka._
import monocle.Prism
import scalaz._, Scalaz._

package object tpe {
  type PrimaryType = SimpleType \/ CompositeType

  object PrimaryType {
    val name: Prism[String, PrimaryType] =
      Prism((s: String) =>
        SimpleType.name.getOption(s).map(_.left) orElse
        CompositeType.name.getOption(s).map(_.right))(
        _.fold(SimpleType.name(_), CompositeType.name(_)))
  }

  object SimpleEJson {
    def unapply(ejs: EJson[_]): Option[SimpleType] =
      simpleTypeOf(ejs)
  }

  /** Returns the `CompositeType` of the given EJson value, if exists. */
  val compositeTypeOf: EJson[_] => Option[CompositeType] = {
    case C(ejs.Arr(_)) => some(CompositeType.Arr)
    case E(ejs.Map(_)) => some(CompositeType.Map)
    case C(ejs.Str(_)) => some(CompositeType.Arr)
    case _             => none
  }

  /** Returns the `SimpleType` of the given EJson value, if exists. */
  val simpleTypeOf: EJson[_] => Option[SimpleType] = {
    case C(ejs.Null() ) => some(SimpleType.Null)
    case C(ejs.Bool(_)) => some(SimpleType.Bool)
    case E(ejs.Byte(_)) => some(SimpleType.Byte)
    case E(ejs.Char(_)) => some(SimpleType.Char)
    case E( ejs.Int(_)) => some(SimpleType.Int)
    case C( ejs.Dec(_)) => some(SimpleType.Dec)
    case _              => none
  }

  /** Returns the `PrimaryType` of the given EJson value, if exists. */
  val primaryTypeOf: EJson[_] => Option[PrimaryType] =
    j => compositeTypeOf(j).map(_.right) orElse simpleTypeOf(j).map(_.left)

  // Instances

  implicit def birecursiveTTypePartialOrder[T[_[_]]: BirecursiveT, J: Order](
    implicit
    JC: Corecursive.Aux[J, EJson],
    JR: Recursive.Aux[J, EJson]
  ): PartialOrder[T[TypeF[J, ?]]] =
    TypeF.subtypingPartialOrder[J, T[TypeF[J, ?]]]

  implicit def birecursiveTTypeBoundedDistributiveLattice[T[_[_]]: BirecursiveT, J: Order](
    implicit
    JC: Corecursive.Aux[J, EJson],
    JR: Recursive.Aux[J, EJson]
  ): BoundedDistributiveLattice[T[TypeF[J, ?]]] =
    TypeF.boundedDistributiveLattice[J, T[TypeF[J, ?]]]
}
