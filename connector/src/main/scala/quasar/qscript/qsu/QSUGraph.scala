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

import slamdata.Predef.{Map => SMap, _}
import quasar.fp._

import monocle.macros.Lenses
import matryoshka._
import scalaz.{Applicative, Traverse}
import scalaz.syntax.equal._
import scalaz.syntax.traverse._
import scalaz.syntax.std.boolean._

@Lenses
final case class QSUGraph[T[_[_]]](
    root: Symbol,
    vertices: SMap[Symbol, QScriptUniform[T, Symbol]]) {

  /**
   * Uniquely merge the graphs, retaining the root from the right.
   */
  def ++:(left: QSUGraph[T]): QSUGraph[T] = QSUGraph(root, vertices ++ left.vertices)

  /**
   * Uniquely merge the graphs, retaining the root from the left.
   */
  def :++(right: QSUGraph[T]): QSUGraph[T] = right ++: this

  def refocus(node: Symbol): QSUGraph[T] =
    copy(root = node)

  /** Removes the `src` vertex, replacing any references to it with `target`. */
  def replace(src: Symbol, target: Symbol): QSUGraph[T] = {
    import QScriptUniform._

    def replaceIfSrc(sym: Symbol): Symbol =
      (sym === src) ? target | sym

    if (vertices.isDefinedAt(src) && vertices.isDefinedAt(target))
      QSUGraph(
        replaceIfSrc(root),
        (vertices - src) mapValues {
          case JoinSideRef(s) => JoinSideRef(replaceIfSrc(s))
          case other          => other map replaceIfSrc
        })
    else
      this
  }

  // projects the root of the graph (which we assume exists)
  def unfold: QScriptUniform[T, QSUGraph[T]] =
    vertices(root).map(refocus)
}

object QSUGraph extends QSUGraphInstances {

  /**
   * The pattern functor for `QSUGraph[T]`.
   */
  @Lenses
  final case class QSUPattern[T[_[_]], A](root: Symbol, qsu: QScriptUniform[T, A])

  object QSUPattern {
    implicit def traverse[T[_[_]]]: Traverse[QSUPattern[T, ?]] =
      new Traverse[QSUPattern[T, ?]] {
        def traverseImpl[G[_]: Applicative, A, B](pattern: QSUPattern[T, A])(f: A => G[B])
            : G[QSUPattern[T, B]] =
          pattern match {
            case QSUPattern(root, qsu) =>
              qsu.traverse(f).map(QSUPattern[T, B](root, _))
          }
      }
  }

  /**
   * This object contains extraction helpers in terms of QSU nodes.
   */
  object Extractors {
    import quasar.qscript.qsu.{QScriptUniform => QSU}

    object AutoJoin {
      def unapply[T[_[_]]](g: QSUGraph[T]) = g.unfold match {
        case g: QSU.AutoJoin[T, QSUGraph[T]] => QSU.AutoJoin.unapply(g)
        case _ => None
      }
    }

    object GroupBy {
      def unapply[T[_[_]]](g: QSUGraph[T]) = g.unfold match {
        case g: QSU.GroupBy[T, QSUGraph[T]] => QSU.GroupBy.unapply(g)
        case _ => None
      }
    }

    object DimEdit {
      def unapply[T[_[_]]](g: QSUGraph[T]) = g.unfold match {
        case g: QSU.DimEdit[T, QSUGraph[T]] => QSU.DimEdit.unapply(g)
        case _ => None
      }
    }

    object LPJoin {
      def unapply[T[_[_]]](g: QSUGraph[T]) = g.unfold match {
        case g: QSU.LPJoin[T, QSUGraph[T]] => QSU.LPJoin.unapply(g)
        case _ => None
      }
    }

    object ThetaJoin {
      def unapply[T[_[_]]](g: QSUGraph[T]) = g.unfold match {
        case g: QSU.ThetaJoin[T, QSUGraph[T]] => QSU.ThetaJoin.unapply(g)
        case _ => None
      }
    }

    object Map {
      def unapply[T[_[_]]](g: QSUGraph[T]) = g.unfold match {
        case g: QSU.Map[T, QSUGraph[T]] => QSU.Map.unapply(g)
        case _ => None
      }
    }

    object Transpose {
      def unapply[T[_[_]]](g: QSUGraph[T]) = g.unfold match {
        case g: QSU.Transpose[T, QSUGraph[T]] => QSU.Transpose.unapply(g)
        case _ => None
      }
    }

    object LeftShift {
      def unapply[T[_[_]]](g: QSUGraph[T]) = g.unfold match {
        case g: QSU.LeftShift[T, QSUGraph[T]] => QSU.LeftShift.unapply(g)
        case _ => None
      }
    }

    object LPReduce {
      def unapply[T[_[_]]](g: QSUGraph[T]) = g.unfold match {
        case g: QSU.LPReduce[T, QSUGraph[T]] => QSU.LPReduce.unapply(g)
        case _ => None
      }
    }

    object QSReduce {
      def unapply[T[_[_]]](g: QSUGraph[T]) = g.unfold match {
        case g: QSU.QSReduce[T, QSUGraph[T]] => QSU.QSReduce.unapply(g)
        case _ => None
      }
    }

    object Distinct {
      def unapply[T[_[_]]](g: QSUGraph[T]) = g.unfold match {
        case g: QSU.Distinct[T, QSUGraph[T]] => QSU.Distinct.unapply(g)
        case _ => None
      }
    }

    object Sort {
      def unapply[T[_[_]]](g: QSUGraph[T]) = g.unfold match {
        case g: QSU.Sort[T, QSUGraph[T]] => QSU.Sort.unapply(g)
        case _ => None
      }
    }

    object UniformSort {
      def unapply[T[_[_]]](g: QSUGraph[T]) = g.unfold match {
        case g: QSU.UniformSort[T, QSUGraph[T]] => QSU.UniformSort.unapply(g)
        case _ => None
      }
    }

    object Union {
      def unapply[T[_[_]]](g: QSUGraph[T]) = g.unfold match {
        case g: QSU.Union[T, QSUGraph[T]] => QSU.Union.unapply(g)
        case _ => None
      }
    }

    object Subset {
      def unapply[T[_[_]]](g: QSUGraph[T]) = g.unfold match {
        case g: QSU.Subset[T, QSUGraph[T]] => QSU.Subset.unapply(g)
        case _ => None
      }
    }

    object LPFilter {
      def unapply[T[_[_]]](g: QSUGraph[T]) = g.unfold match {
        case g: QSU.LPFilter[T, QSUGraph[T]] => QSU.LPFilter.unapply(g)
        case _ => None
      }
    }

    object QSFilter {
      def unapply[T[_[_]]](g: QSUGraph[T]) = g.unfold match {
        case g: QSU.QSFilter[T, QSUGraph[T]] => QSU.QSFilter.unapply(g)
        case _ => None
      }
    }

    object Nullary {
      def unapply[T[_[_]]](g: QSUGraph[T]) = g.unfold match {
        case g: QSU.Nullary[T, QSUGraph[T]] => QSU.Nullary.unapply(g)
        case _ => None
      }
    }

    object Constant {
      def unapply[T[_[_]]](g: QSUGraph[T]) = g.unfold match {
        case g: QSU.Nullary[T, QSUGraph[T]] => QSU.Constant.unapply(g)
        case _ => None
      }
    }
  }
}

sealed abstract class QSUGraphInstances extends QSUGraphInstances0 {
  import QSUGraph._

  implicit def corecursive[T[_[_]]]: Corecursive.Aux[QSUGraph[T], QSUPattern[T, ?]] =
    birecursive[T]

  implicit def recursive[T[_[_]]]: Recursive.Aux[QSUGraph[T], QSUPattern[T, ?]] =
    birecursive[T]
}

sealed abstract class QSUGraphInstances0 {
  import QSUGraph._

  implicit def birecursive[T[_[_]]]: Birecursive.Aux[QSUGraph[T], QSUPattern[T, ?]] =
    Birecursive.algebraIso(φ[T], ψ[T])

  ////

  // only correct given compacted subgraphs which agree on names
  private def φ[T[_[_]]]: Algebra[QSUPattern[T, ?], QSUGraph[T]] = {
    case QSUPattern(root, qsu) =>
      val initial: QSUGraph[T] = QSUGraph[T](root, SMap(root -> qsu.map(_.root)))

      qsu.foldRight(initial) {
        case (graph, acc) => graph ++: acc // retain the root from the right
      }
  }

  private def ψ[T[_[_]]]: Coalgebra[QSUPattern[T, ?], QSUGraph[T]] = graph => {
    QSUPattern[T, QSUGraph[T]](graph.root, graph.unfold)
  }
}
