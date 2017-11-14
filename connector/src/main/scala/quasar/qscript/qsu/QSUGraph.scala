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
import quasar.NameGenerator
import quasar.contrib.scalaz.MonadState_
import quasar.fp._

import monocle.macros.Lenses
import matryoshka._
import matryoshka.implicits._
import scalaz.{Applicative, Id, Monad, MonadState, Traverse, Scalaz, State, StateT}, Scalaz._

@Lenses
final case class QSUGraph[T[_[_]]](
    root: Symbol,
    vertices: QSUVerts[T]) {

  /**
   * Uniquely merge the graphs, retaining the root from the right.
   */
  def ++:(left: QSUGraph[T]): QSUGraph[T] = QSUGraph(root, left.vertices ++ vertices)

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

  def overwriteAtRoot(qsu: QScriptUniform[T, Symbol]): QSUGraph[T] =
    QSUGraph(root, vertices.updated(root, qsu))

  /**
   * Allows rewriting of arbitrary subgraphs.  Rewrites are
   * applied in a bottom-up (leaves-first) order, which avoids
   * ambiguities when changing nodes that are visible to subsequent
   * rewrites.  The one caveat here is that all "changes" to the graph
   * must be additive in nature.  You can never remove a node.
   * Rewrites to the definition of a node are valid, but you cannot
   * remove it from the graph, as other nodes will still point to it.
   * This becomes relevant if you want to transform some subgraph
   * g into f(g).  When this happens, you must internally generate a
   * new symbol for g and rewrite your subgraph to point to that new
   * g, then the composite graph f(g) must be given the original symbol.
   * As a safety check, because this case is just entirely unsound, the
   * symbol you return at the root is entirely ignored in favor of the
   * original root at that locus.
   */
  def rewriteM[F[_]: Monad](pf: PartialFunction[QSUGraph[T], F[QSUGraph[T]]]): F[QSUGraph[T]] = {
    type G[A] = StateT[F, Set[Symbol], A]
    val MS = MonadState[G, Set[Symbol]]

    @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
    def inner(g: QSUGraph[T]): G[QSUGraph[T]] = {
      for {
        visited <- MS.get

        back <- if (visited(g.root)) {
          this.point[G]
        } else {
          for {
            _ <- MS.modify(_ + g.root)

            recursive <- g.unfold traverse { sg =>
              for {
                previs <- MS.get
                sg2 <- inner(sg)
                postvis <- MS.get
              } yield (sg2, postvis &~ previs)
            }

            index = recursive.foldLeft[SMap[Symbol, Set[Symbol]]](SMap()) {
              case (acc, (sg, snapshot)) =>
                val sym = sg.root

                if (acc.contains(sym))
                  acc.updated(sym, acc(sym).union(snapshot))
                else
                  acc + (sym -> snapshot)
            }

            sum = index.values.reduceOption(_ union _).getOrElse(Set())

            // remove the keys which were touched from the original
            preimage = g.vertices -- sum

            collapsed = recursive.foldLeft[QSUVerts[T]](preimage) {
              case (acc, (sg, _)) =>
                acc ++ (sg.vertices -- (sum &~ index(sg.root)))
            }

            self2 = QSUGraph(g.root, collapsed)

            applied <- if (pf.isDefinedAt(self2))
              pf(self2).liftM[StateT[?[_], Set[Symbol], ?]]
            else
              self2.point[G]
          } yield applied.copy(root = self2.root)   // prevent users from building invalid graphs
        }
      } yield back
    }

    inner(this).eval(Set())
  }

  def rewrite(pf: PartialFunction[QSUGraph[T], QSUGraph[T]]): QSUGraph[T] =
    rewriteM(pf.andThen(_.point[Id]))

  // projects the root of the graph (which we assume exists)
  def unfold: QScriptUniform[T, QSUGraph[T]] =
    vertices(root).map(refocus)
}

object QSUGraph extends QSUGraphInstances {

  /** Construct a QSUGraph from a tree of `QScriptUniform` by compacting
    * common subtrees.
    */
  def fromTree[T[_[_]]: RecursiveT](qsu: T[QScriptUniform[T, ?]]): QSUGraph[T] = {
    type F[A] = StateT[State[Long, ?], NodeNames[T], A]
    qsu.cataM(fromTreeƒ[T, F]).eval(SMap()).eval(0)
  }

  type NodeNames[T[_[_]]] = SMap[QScriptUniform[T, Symbol], Symbol]
  type NodeNamesM[T[_[_]], F[_]] = MonadState_[F, NodeNames[T]]

  def fromTreeƒ[T[_[_]], F[_]: Monad: NameGenerator: NodeNamesM[T, ?[_]]]
      : AlgebraM[F, QScriptUniform[T, ?], QSUGraph[T]] =
    qsu => for {
      nodes <- MonadState_[F, NodeNames[T]].get
      node  =  qsu map (_.root)
      name  <- nodes.get(node).getOrElseF(
                 NameGenerator[F] prefixedName "__fromTree" map (Symbol(_)))
      _     <- MonadState_[F, NodeNames[T]].put(nodes + (node -> name))
    } yield qsu.foldRight(QSUGraph(name, SMap(name -> node)))(_ ++: _)

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
    import quasar.Data
    import quasar.qscript.qsu.{QScriptUniform => QSU}
    import quasar.qscript.{
      FreeMap,
      Hole,
      JoinSide,
      JoinSide3,
      LeftSide,
      MapFunc,
      MapFuncsCore,
      MapFuncCore,
      RightSide,
      SrcHole
    }

    import pathy.Path
    import scalaz.:<:

    object AutoJoin2 {
      def unapply[T[_[_]]](g: QSUGraph[T]) = g.unfold match {
        case g: QSU.AutoJoin2[T, QSUGraph[T]] => QSU.AutoJoin2.unapply(g)
        case _ => None
      }
    }

    object AutoJoin3 {
      def unapply[T[_[_]]](g: QSUGraph[T]) = g.unfold match {
        case g: QSU.AutoJoin3[T, QSUGraph[T]] => QSU.AutoJoin3.unapply(g)
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

    object Read {
      def unapply[T[_[_]]](g: QSUGraph[T]) = g.unfold match {
        case g: QSU.Read[T, QSUGraph[T]] => QSU.Read.unapply(g)
        case _ => None
      }
    }

    object Unary {
      def unapply[T[_[_]]](g: QSUGraph[T]) = g.unfold match {
        case g: QSU.Unary[T, QSUGraph[T]] => QSU.Unary.unapply(g)
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

    object LPSort {
      def unapply[T[_[_]]](g: QSUGraph[T]) = g.unfold match {
        case g: QSU.LPSort[T, QSUGraph[T]] => QSU.LPSort.unapply(g)
        case _ => None
      }
    }

    object QSSort {
      def unapply[T[_[_]]](g: QSUGraph[T]) = g.unfold match {
        case g: QSU.QSSort[T, QSUGraph[T]] => QSU.QSSort.unapply(g)
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

    object Unreferenced {
      def unapply[T[_[_]]](g: QSUGraph[T]): Boolean = g.unfold match {
        case g: QSU.Unreferenced[T, QSUGraph[T]] => QSU.Unreferenced.unapply(g)
        case _ => false
      }
    }

    object JoinSideRef {
      def unapply[T[_[_]]](g: QSUGraph[T]) = g.unfold match {
        case g: QSU.JoinSideRef[T, QSUGraph[T]] => QSU.JoinSideRef.unapply(g)
        case _ => None
      }
    }

    object AutoJoin2C {
      def unapply[T[_[_]]](qgraph: QSUGraph[T])(
          implicit IC: MapFuncCore[T, ?] :<: MapFunc[T, ?])
          : Option[(QSUGraph[T], QSUGraph[T], MapFuncCore[T, JoinSide])] = qgraph match {

        case AutoJoin2(left, right, IC(mfc)) => Some((left, right, mfc))
        case _ => None
      }
    }

    object AutoJoin3C {
      def unapply[T[_[_]]](qgraph: QSUGraph[T])(
          implicit IC: MapFuncCore[T, ?] :<: MapFunc[T, ?])
          : Option[(QSUGraph[T], QSUGraph[T], QSUGraph[T], MapFuncCore[T, JoinSide3])] = qgraph match {

        case AutoJoin3(left, center, right, IC(mfc)) => Some((left, center, right, mfc))
        case _ => None
      }
    }

    object DataConstant {
      def unapply[T[_[_]]: BirecursiveT](qgraph: QSUGraph[T])(
          implicit IC: MapFuncCore[T, ?] :<: MapFunc[T, ?]): Option[Data] = qgraph match {

        case Unary(Unreferenced(), IC(MapFuncsCore.Constant(ejson))) =>
          Some(ejson.cata(Data.fromEJson))
        case _ => None
      }
    }

    object DataConstantMapped {
      def unapply[T[_[_]]: BirecursiveT](qgraph: QSUGraph[T])(
          implicit IC: MapFuncCore[T, ?] :<: MapFunc[T, ?]): Option[Data] = qgraph match {

        case Map(Unreferenced(), FMFC1(MapFuncsCore.Constant(ejson))) =>
          Some(ejson.cata(Data.fromEJson))
        case _ => None
      }
    }

    // TODO doesn't guarantee only one function; could be more!
    object FMFC1 {
      def unapply[T[_[_]]](fm: FreeMap[T])(
          implicit IC: MapFuncCore[T, ?] :<: MapFunc[T, ?]): Option[MapFuncCore[T, Hole]] = {

        fm.resume.swap.toOption collect {
          case IC(mfc) => mfc.map(_ => SrcHole: Hole)
        }
      }
    }

    object TRead {
      @SuppressWarnings(Array("org.wartremover.warts.Equals"))
      def unapply[T[_[_]]: BirecursiveT](qgraph: QSUGraph[T])(
          implicit IC: MapFuncCore[T, ?] :<: MapFunc[T, ?]): Option[String] = qgraph match {

        case AutoJoin2C(
          Transpose(Read(path), QSU.Rotation.ShiftMap),
          DataConstant(Data.Int(i)),
          MapFuncsCore.ProjectIndex(LeftSide, RightSide)) if i == 1 =>

          for {
            (front, end) <- Path.peel(path)
            file <- end.toOption
            if Path.peel(front).isEmpty
          } yield file.value

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
