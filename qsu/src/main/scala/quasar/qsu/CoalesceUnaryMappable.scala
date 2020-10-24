/*
 * Copyright 2020 Precog Data
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

package quasar.qsu

import slamdata.Predef.{Map => SMap, _}

import quasar.contrib.iota._
import quasar.contrib.matryoshka.safe
import quasar.fp._
import quasar.qscript.{
  construction,
  Center,
  Hole,
  LeftSide,
  LeftSide3,
  RightSide,
  RightSide3
}
import quasar.qscript.RecFreeS._
import quasar.qsu.{QScriptUniform => QSU}

import cats.{Monoid, MonoidK}
import cats.instances.map._

import matryoshka.BirecursiveT
import matryoshka.data.free._
import matryoshka.patterns.CoEnv

import scalaz.syntax.bind._
import scalaz.syntax.equal._

import shims.monoidToScalaz

/** Coalesces adjacent mappable regions of a single root. */
final class CoalesceUnaryMappable[T[_[_]]: BirecursiveT] private () extends QSUTTypes[T] {
  import QSUGraph.Extractors._
  import MappableRegion.MaximalUnary

  private val mf = construction.Func[T]
  private implicit val vertsMonoid: Monoid[QSUVerts[T]] = MonoidK[SMap[Symbol, ?]].algebra

  def apply(graph: QSUGraph): QSUGraph = {
    val coalesced = graph rewrite {
      case g @ MaximalUnary(src, fm) if g.root =/= src.root =>
        g.overwriteAtRoot(QScriptUniform.Map(src.root, fm.asRec))

      case g @ AutoJoin2(left, right, combine) =>
        val nodes = mapNodes(List(left, right))

        if (nodes.isEmpty)
          g
        else {
          val (l, lf) = nodes.getOrElse(0, (left.root, mf.Hole))
          val (r, rf) = nodes.getOrElse(1, (right.root, mf.Hole))

          val cmb = combine flatMap {
            case LeftSide => lf >> mf.LeftSide
            case RightSide => rf >> mf.RightSide
          }

          g.overwriteAtRoot(QSU.AutoJoin2(l, r, cmb))
        }

      case g @ AutoJoin3(left, center, right, combine) =>
        val nodes = mapNodes(List(left, center, right))

        if (nodes.isEmpty)
          g
        else {
          val (l, lf) = nodes.getOrElse(0, (left.root, mf.Hole))
          val (c, cf) = nodes.getOrElse(1, (center.root, mf.Hole))
          val (r, rf) = nodes.getOrElse(2, (right.root, mf.Hole))

          val cmb = combine flatMap {
            case LeftSide3 => lf >> mf.LeftSide3
            case Center => cf >> mf.Center
            case RightSide3 => rf >> mf.RightSide3
          }

          g.overwriteAtRoot(QSU.AutoJoin3(l, c, r, cmb))
        }
    }

    // Make all coalesced FreeMaps strict, eliminating all lazy nodes that may refer to a
    // QSUGraph due to how `MaximumUnary` works. This is necessary to prevent loops in
    // QSUGraph#reverseIndex when we attempt to compute `Free#hashCode`.
    val strictVerts = coalesced foldMapDown { g =>
      g.vertices(g.root) match {
        case QScriptUniform.Map(src, fm) =>
          val strictFm =
            safe.transCata(fm.linearize)((ft: CoEnv[Hole, MapFunc, FreeMap]) => ft)

          SMap(g.root -> QScriptUniform.Map(src, strictFm.asRec))

        case other => SMap(g.root -> other)
      }
    }

    QSUGraph(coalesced.root, strictVerts)
  }

  def mapNodes(gs: List[QSUGraph]): SMap[Int, (Symbol, FreeMap)] =
    gs.zipWithIndex.foldLeft(SMap[Int, (Symbol, FreeMap)]()) {
      case (acc, (Map(s, fm), i)) => acc.updated(i, (s.root, fm.linearize))
      case (acc, _) => acc
    }
}

object CoalesceUnaryMappable {
  def apply[T[_[_]]: BirecursiveT](graph: QSUGraph[T]): QSUGraph[T] =
    (new CoalesceUnaryMappable[T]).apply(graph)
}
