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

import quasar.NameGenerator
import quasar.contrib.scalaz.MonadState_
import quasar.qscript.{
  Center,
  Hole,
  LeftSide,
  LeftSide3,
  RightSide,
  RightSide3,
  SrcHole
}
import quasar.qscript.qsu.{QScriptUniform => QSU}
import slamdata.Predef._

import matryoshka.BirecursiveT
import scalaz.{~>, Free, Monad, StateT}
import scalaz.std.list._
import scalaz.syntax.foldable._
import scalaz.syntax.monad._

final class MinimizeAutoJoins[T[_[_]]: BirecursiveT] private () extends QSUTTypes[T] {
  import QSUGraph.Extractors._

  def apply[F[_]: Monad: NameGenerator](qgraph: QSUGraph): F[QSUGraph] = {
    type G[A] = StateT[F, RevIdx, A]

    val back = qgraph rewriteM {
      case qgraph @ AutoJoin2(left, right, combiner) =>
        val fml = MappableRegion.maximal(left)
        val fmr = MappableRegion.maximal(right)

        val minimized = minimizeSources(extractSources(fml) ::: extractSources(fmr))

        lazy val extended = combiner map {
          case LeftSide => fml.map(_ => SrcHole: Hole)
          case RightSide => fmr.map(_ => SrcHole: Hole)
        }

        attemptCoalesce[G](qgraph, minimized, extended)

      case qgraph @ AutoJoin3(left, center, right, combiner) =>
        val fml = MappableRegion.maximal(left)
        val fmc = MappableRegion.maximal(center)
        val fmr = MappableRegion.maximal(right)

        val minimized =
          minimizeSources(extractSources(fml) ::: extractSources(fmc) ::: extractSources(fmr))

        lazy val extended = combiner map {
          case LeftSide3 => fml.map(_ => SrcHole: Hole)
          case Center => fmc.map(_ => SrcHole: Hole)
          case RightSide3 => fmr.map(_ => SrcHole: Hole)
        }

        attemptCoalesce[G](qgraph, minimized, extended)
    }

    back.eval(qgraph.generateRevIndex)
  }

  private def attemptCoalesce[G[_]: Monad: NameGenerator: MonadState_[?[_], RevIdx]](
      qgraph: QSUGraph,
      minimized: List[QSUGraph],
      extended: => MapFunc[FreeMap]): G[QSUGraph] = {

    lazy val fm = Free.roll[MapFunc, Hole](extended)

    minimized match {
      case Nil =>
        QSUGraph.withName[T, G](QSU.Unreferenced[T, Symbol]()) map { unref =>
          qgraph.overwriteAtRoot(QSU.Map[T, Symbol](unref.root, fm)) :++ unref
        }

      case single :: Nil =>
        qgraph.overwriteAtRoot(QSU.Map[T, Symbol](single.root, fm)).point[G]

      case minimized =>
        // TODO implement coalesce heuristics here
        qgraph.point[G]
    }
  }

  private def minimizeSources(sources: List[QSUGraph]): List[QSUGraph] = {
    val (_, minimized) = sources.foldRight((Set[Symbol](), List[QSUGraph]())) {
      // just drop unreferenced sources entirely
      case (Unreferenced(), pair) => pair

      case (g, (mask, acc)) =>
        if (mask(g.root))
          (mask, acc)
        else
          (mask + g.root, g :: acc)
    }

    minimized
  }

  private def extractSources[A](fma: FreeMapA[A]): List[A] =
    fma.foldMap(λ[MapFunc ~> List](_.toList))
}

object MinimizeAutoJoins {
  def apply[T[_[_]]: BirecursiveT]: MinimizeAutoJoins[T] = new MinimizeAutoJoins[T]
}
