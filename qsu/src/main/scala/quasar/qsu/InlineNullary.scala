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

package quasar.qsu

import slamdata.Predef.{None, Option, Some, String, Symbol}

import quasar.common.effect.NameGenerator
import quasar.qscript.{
  Center,
  HoleF,
  LeftSide,
  LeftSideF,
  LeftSide3,
  RecFreeS,
  RightSide,
  RightSideF,
  RightSide3
}

import scalaz.{Monad, StateT}
import scalaz.syntax.monad._

/** Inlines nullary mappable expressions into AutoJoin combiners, reducing or
  * eliminating the AutoJoin.
  */
final class InlineNullary[T[_[_]]] private () extends QSUTTypes[T] {
  import QSUGraph.Extractors._
  import RecFreeS._

  private val namePrefix: String = "iln"

  def apply[F[_]: Monad: NameGenerator: RevIdxM](graph: QSUGraph): F[QSUGraph] =
    graph rewriteM[F] {
      case g @ AutoJoin2(left, right, combine) =>
        (nullary(left), nullary(right)) match {
          case (Some(l), Some(r)) =>
            val fm = combine flatMap {
              case LeftSide => l
              case RightSide => r
            }

            QSUGraph.withName[T, F](namePrefix)(QSU.Unreferenced[T, Symbol]()) map { u =>
              g.overwriteAtRoot(QSU.Map(u.root, fm.asRec))
            }

          case (Some(l), None) =>
            val fm = combine flatMap {
              case LeftSide => l
              case RightSide => HoleF[T]
            }
            g.overwriteAtRoot(QSU.Map(right.root, fm.asRec)).point[F]

          case (None, Some(r)) =>
            val fm = combine flatMap {
              case LeftSide => HoleF[T]
              case RightSide => r
            }
            g.overwriteAtRoot(QSU.Map(left.root, fm.asRec)).point[F]

          case (None, None) => g.point[F]
        }

      case g @ AutoJoin3(left, center, right, combine) =>
        (nullary(left), nullary(center), nullary(right)) match {
          case (Some(l), Some(c), Some(r)) =>
            val fm = combine flatMap {
              case LeftSide3 => l
              case Center => c
              case RightSide3 => r
            }

            QSUGraph.withName[T, F](namePrefix)(QSU.Unreferenced[T, Symbol]()) map { u =>
              g.overwriteAtRoot(QSU.Map(u.root, fm.asRec))
            }

          case (Some(l), Some(c), None) =>
            val fm = combine flatMap {
              case LeftSide3 => l
              case Center => c
              case RightSide3 => HoleF[T]
            }
            g.overwriteAtRoot(QSU.Map(right.root, fm.asRec)).point[F]

          case (Some(l), None, Some(r)) =>
            val fm = combine flatMap {
              case LeftSide3 => l
              case Center => HoleF[T]
              case RightSide3 => r
            }
            g.overwriteAtRoot(QSU.Map(center.root, fm.asRec)).point[F]

          case (Some(l), None, None) =>
            val jf = combine flatMap {
              case LeftSide3 => l >> LeftSideF[T]
              case Center => LeftSideF[T]
              case RightSide3 => RightSideF[T]
            }
            g.overwriteAtRoot(QSU.AutoJoin2(center.root, right.root, jf)).point[F]

          case (None, Some(c), Some(r)) =>
            val fm = combine flatMap {
              case LeftSide3 => HoleF[T]
              case Center => c
              case RightSide3 => r
            }
            g.overwriteAtRoot(QSU.Map(left.root, fm.asRec)).point[F]

          case (None, Some(c), None) =>
            val jf = combine flatMap {
              case LeftSide3 => LeftSideF[T]
              case Center => c >> LeftSideF[T]
              case RightSide3 => RightSideF[T]
            }
            g.overwriteAtRoot(QSU.AutoJoin2(left.root, right.root, jf)).point[F]

          case (None, None, Some(r)) =>
            val jf = combine flatMap {
              case LeftSide3 => LeftSideF[T]
              case Center => RightSideF[T]
              case RightSide3 => r >> LeftSideF[T]
            }
            g.overwriteAtRoot(QSU.AutoJoin2(left.root, center.root, jf)).point[F]

          case (None, None, None) => g.point[F]
        }
    }

  private val QSU = QScriptUniform

  private val nullary: QSUGraph => Option[FreeMap] =
    MappableRegion.MaximalNullary.unapply[T] _
}

object InlineNullary {
  def apply[T[_[_]], F[_]: Monad: NameGenerator](graph: QSUGraph[T]): F[QSUGraph[T]] = {
    type G[A] = StateT[F, QSUGraph.RevIdx[T], A]
    (new InlineNullary[T])[G](graph).eval(graph.generateRevIndex)
  }
}
