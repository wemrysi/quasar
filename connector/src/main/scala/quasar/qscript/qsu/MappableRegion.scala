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

import slamdata.Predef.{Boolean, Option, Symbol}
import quasar.fp._
import quasar.fp.ski.κ
import quasar.qscript.{
  FreeMap,
  FreeMapA,
  JoinFunc,
  JoinSide,
  LeftSide,
  MapFunc,
  RightSide,
  SrcHole
}

import matryoshka._
import matryoshka.data._
import matryoshka.implicits._
import matryoshka.patterns._
import scalaz.{\/, Kleisli, Traverse}
import scalaz.std.option._
import scalaz.syntax.bind._
import scalaz.syntax.either._
import scalaz.syntax.equal._
import scalaz.syntax.foldable._
import scalaz.syntax.plus._
import scalaz.syntax.std.boolean._

/** The maximal "mappable" (expressable via MapFunc) region of a graph. */
object MappableRegion {
  import QScriptUniform._
  import QSUGraph.QSUPattern

  def apply[T[_[_]]](halt: Symbol => Boolean, g: QSUGraph[T]): FreeMapA[T, QSUGraph[T]] =
    g.elgotApo[FreeMapA[T, QSUGraph[T]]](mappableRegionƒ[T](halt))

  def binaryOf[T[_[_]]](left: Symbol, right: Symbol, g: QSUGraph[T]): Option[JoinFunc[T]] =
    funcOf(Kleisli(replaceWith[JoinSide](left, LeftSide)) <+> Kleisli(replaceWith(right, RightSide)), g)

  def funcOf[T[_[_]], A](f: Symbol => Option[A], g: QSUGraph[T]): Option[FreeMapA[T, A]] =
    Traverse[FreeMapA[T, ?]].traverse(apply(s => f(s).isDefined, g))(qg => f(qg.root))

  def maximal[T[_[_]]](g: QSUGraph[T]): FreeMapA[T, QSUGraph[T]] =
    apply(κ(false), g)

  def unaryOf[T[_[_]]](src: Symbol, g: QSUGraph[T]): Option[FreeMap[T]] =
    funcOf(replaceWith(src, SrcHole), g)

  def mappableRegionƒ[T[_[_]]](halt: Symbol => Boolean)
      : ElgotCoalgebra[FreeMapA[T, QSUGraph[T]] \/ ?, CoEnv[QSUGraph[T], MapFunc[T, ?], ?], QSUGraph[T]] =
    g => g.project match {
      case QSUPattern(s, _) if halt(s) =>
        CoEnv(g.left).right

      case QSUPattern(_, AutoJoin(srcs, combine)) =>
        CoEnv(combine.map(srcs.toVector).right[QSUGraph[T]]).right

      case QSUPattern(_, Map(srcG, mf)) =>
        mf.as(srcG).left

      case QSUPattern(_, Nullary(mf)) =>
        CoEnv(mf[QSUGraph[T]].right[QSUGraph[T]]).right

      case _ =>
        CoEnv(g.left).right
    }

  ////

  private def replaceWith[A](target: Symbol, a: A): Symbol => Option[A] =
    s => (s === target) option a
}
