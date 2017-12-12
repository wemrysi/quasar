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
import quasar.{NameGenerator, Planner}, Planner.PlannerErrorME
import quasar.contrib.matryoshka._
import quasar.contrib.scalaz.MonadState_
import quasar.ejson.{EJson, Fixed}
import quasar.ejson.implicits._
import quasar.fp._
import quasar.fp.ski.κ
import quasar.qscript.{
  construction,
  Center,
  Hole,
  LeftSide,
  LeftSide3,
  RightSide,
  RightSide3,
  SrcHole
}
import quasar.qscript.qsu.{QScriptUniform => QSU}
import quasar.qscript.qsu.ApplyProvenance.AuthenticatedQSU

import matryoshka.{delayEqual, BirecursiveT, EqualT}
import matryoshka.data.free._
import scalaz.{Bind, Equal, Monad, OptionT, Scalaz, StateT}, Scalaz._   // sigh, monad/traverse conflict

final class MinimizeAutoJoins[T[_[_]]: BirecursiveT: EqualT] private () extends QSUTTypes[T] {
  import MinimizeAutoJoins._
  import QSUGraph.Extractors._

  private val Minimizers = List(
    minimizers.MergeReductions[T],
    minimizers.ShiftProjectBelow[T])

  private val func = construction.Func[T]
  private val srcHole: Hole = SrcHole   // wtb smart constructor

  private val J = Fixed[T[EJson]]
  private val QP = QProv[T]

  // needed to avoid bug in implicit search!  don't import QP.prov._
  private implicit val QPEq: Equal[QP.P] =
    QP.prov.provenanceEqual(scala.Predef.implicitly, Equal[FreeMapA[Access[Symbol]]])

  def apply[F[_]: Monad: NameGenerator: PlannerErrorME](agraph: AuthenticatedQSU[T]): F[AuthenticatedQSU[T]] = {
    type G[A] = StateT[StateT[F, RevIdx, ?], MinimizationState[T], A]

    val back = agraph.graph rewriteM {
      case qgraph @ AutoJoin2(left, right, combiner) =>
        val combiner2: FreeMapA[Int] = combiner map {
          case LeftSide => 0
          case RightSide => 1
        }

        OptionT(coalesceToMap[G](qgraph, List(left, right), combiner2)).getOrElseF(failure[G](qgraph))

      case qgraph @ AutoJoin3(left, center, right, combiner) =>
        val combiner2: FreeMapA[Int] = combiner map {
          case LeftSide3 => 0
          case Center => 1
          case RightSide3 => 2
        }

        OptionT(coalesceToMap[G](qgraph, List(left, center, right), combiner2)).getOrElseF(failure[G](qgraph))
    }

    val lifted = back(MinimizationState[T](agraph.dims, Set())) map {
      case (MinimizationState(dims, _), graph) => AuthenticatedQSU[T](graph, dims)
    }

    lifted.eval(agraph.graph.generateRevIndex)
  }

  // the Ints are indices into branches
  private def coalesceToMap[
      G[_]: Monad: NameGenerator: MonadState_[?[_], RevIdx]: MonadState_[?[_], MinimizationState[T]]: PlannerErrorME](
      qgraph: QSUGraph,
      branches: List[QSUGraph],
      combiner: FreeMapA[Int]): G[Option[QSUGraph]] = {

    for {
      state <- MonadState_[G, MinimizationState[T]].get

      fms = branches.zipWithIndex map {
        case (g, i) =>
          MappableRegion[T](state.failed, g).map(g => (g, i))
      }

      (remap, candidates) = minimizeSources(fms.flatMap(_.toList))

      fm = combiner.flatMap(i => fms(i).map(κ(remap(i))))

      back <- coalesceRoots[G](qgraph, fm, candidates)
    } yield back
  }

  // attempt to extend by seeing through constructs like filter (mostly just filter)
  @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
  private def expandSecondOrder(fm: FreeMapA[Int], candidates: List[QSUGraph]): Option[(FreeMapA[Int], List[QSUGraph])] = {
    val (fm2, candidates2, changed) = candidates.zipWithIndex.foldRight((fm, List[QSUGraph](), false)) {
      case ((QSFilter(source, predicate), i), (fm, candidates, _)) =>
        val fm2 = fm flatMap { i2 =>
          if (i2 === i)
            func.Cond(predicate.map(κ(i)), i.point[FreeMapA], func.Undefined[Int])
          else
            i2.point[FreeMapA]
        }

        (fm2, source :: candidates, true)

      case ((node, _), (fm, candidates, changed)) =>
        (fm, node :: candidates, changed)
    }

    if (changed)
      Some((fm2, candidates2))
    else
      None
  }

  // attempts to reduce the set of candidates to a single Map node, given a FreeMap[Int]
  // the Int indexes into the final number of distinct roots
  @SuppressWarnings(Array("org.wartremover.warts.TraversableOps"))
  private def coalesceRoots[
      G[_]: Monad: NameGenerator: MonadState_[?[_], RevIdx]: MonadState_[?[_], MinimizationState[T]]: PlannerErrorME](
      qgraph: QSUGraph,
      fm: => FreeMapA[Int],
      candidates: List[QSUGraph]): G[Option[QSUGraph]] = candidates match {

    case Nil =>
      updateGraph[T, G](QSU.Unreferenced[T, Symbol]()) map { unref =>
        val back = qgraph.overwriteAtRoot(QSU.Map[T, Symbol](unref.root, fm.map(κ(srcHole)))) :++ unref
        Some(back)
      }

    case single :: Nil =>
      // if this is false, it's an assertion error
      // lazy val sanityCheck = fm.toList.forall(0 ==)

      qgraph.overwriteAtRoot(QSU.Map[T, Symbol](single.root, fm.map(κ(srcHole)))).point[G] map { back =>
        Some(back)
      }

    case candidates =>
      expandSecondOrder(fm, candidates) match {
        case Some((fm, candidates)) =>
          // we need to re-expand and re-minimize, and we might end up doing additional second-order expansions
          coalesceToMap[G](qgraph, candidates, fm)

        case None =>
          for {
            resultM <- Minimizers.foldLeftM[G, Option[(QSUGraph, QSUGraph)]](None) {
              case (Some(pair), _) =>
                Option(pair).point[G]

              case (None, minimizer) =>
                if (minimizer.couldApplyTo(candidates)) {
                  val backM = for {
                    extractions <- candidates traverse { c =>
                      OptionT(minimizer.extract[G](c).point[G])
                    }

                    (exCandidates, exRebuilds) = extractions unzip {
                      case (c, r) => (c, r)
                    }

                    upperMapFragments = (0 until exCandidates.length) map { i =>
                      func.MakeMapS(i.toString, i.point[FreeMapA])
                    }

                    upperFM = upperMapFragments.reduceLeft(func.ConcatMaps(_, _))

                    upperFMReduced = upperFM.map(κ(srcHole))

                    singleSource <-
                      OptionT(
                        coalesceToMap[G](
                          exCandidates.head,    // try to collapse everything to the left
                          exCandidates,
                          upperFM))

                    (simplifiedSource, simplifiedFM) = singleSource match {
                      case Map(src, fm) => (src, fm)
                      case _ => (singleSource, func.Hole)
                    }

                    // this is mostly to get nicer graphs with less redundant structure
                    // it's a fast-path to see if we happened to get a trivial coalesce
                    // on our parent structure. if we did, then we can bypass the maps.
                    // if we didn't have this check, we would do something like
                    // ProjectKeyS(ConcatMaps(MakeMapS(Hole, "0"), ...), "0"),
                    // which is dumb
                    isReduced = simplifiedFM === upperFMReduced

                    candidates2 <- exRebuilds.zipWithIndex traverse {
                      case (rebuild, i) =>
                        val rebuiltFM = if (isReduced)
                          func.Hole
                        else
                          func.ProjectKeyS(simplifiedFM, i.toString)

                        rebuild(
                          simplifiedSource,
                          rebuiltFM).liftM[OptionT]
                    }

                    back <- OptionT(minimizer[G](qgraph, candidates2, fm))
                  } yield back

                  backM.run
                } else {
                  (None: Option[(QSUGraph, QSUGraph)]).point[G]
                }
            }

            back <- resultM traverse {
              case (retarget, result) =>
                updateForCoalesce[G](candidates, retarget.root).map(_ => result)
            }
          } yield back
      }
  }

  private def failure[
      G[_]: Monad: MonadState_[?[_], MinimizationState[T]]](
      graph: QSUGraph): G[QSUGraph] = {

    val change = MonadState_[G, MinimizationState[T]] modify { state =>
      state.copy(failed = state.failed + graph.root)
    }

    change.map(_ => graph)
  }

  private def updateForCoalesce[G[_]: Bind: MonadState_[?[_], MinimizationState[T]]](
      candidates: List[QSUGraph],
      newRoot: Symbol): G[Unit] = {

    MonadState_[G, MinimizationState[T]] modify { state =>
      val dims2 = state.dims map {
        case (key, value) =>
          val value2 = candidates.foldLeft(value) { (value, c) =>
            if (c.root =/= newRoot)
              QP.rename(c.root, newRoot, value)
            else
              value
          }

          key -> value2
      }

      state.copy(dims = dims2)
    }
  }

  // we return a remap function along with a minimized list
  // the remapper can be used to determine which input indices
  // collapsed into what output indices
  // note that if everything is Unreferenced, remap will be κ(-1),
  // which is weird but harmless
  private def minimizeSources(sources: List[(QSUGraph, Int)]): (Int => Int, List[QSUGraph]) = {
    val (_, remap, minimized) =
      sources.foldLeft((Set[Symbol](), SMap[Int, Int](), Vector[QSUGraph]())) {
        case ((seen, remap, acc), (Unreferenced(), i)) =>
          (seen, remap + (i -> (acc.length - 1)), acc)

        case ((seen, remap, acc), (g, i)) if seen(g.root) =>
          (seen, remap + (i -> (acc.length - 1)), acc)

        case ((seen, remap, acc), (g, i)) =>
          (seen + g.root, remap + (i -> acc.length), acc :+ g)
      }

    (i => remap.getOrElse(i, -1), minimized.toList)
  }
}

object MinimizeAutoJoins {
  import QSUGraph.RevIdx

  final case class MinimizationState[T[_[_]]](dims: QSUDims[T], failed: Set[Symbol])

  def apply[
      T[_[_]]: BirecursiveT: EqualT,
      F[_]: Monad: NameGenerator: PlannerErrorME]
      (agraph: AuthenticatedQSU[T])
      : F[AuthenticatedQSU[T]] =
    taggedInternalError("MinimizeAutoJoins", new MinimizeAutoJoins[T].apply[F](agraph))

  def updateGraph[
      T[_[_]]: BirecursiveT: EqualT,
      G[_]: Monad: NameGenerator: MonadState_[?[_], RevIdx[T]]: MonadState_[?[_], MinimizationState[T]]: PlannerErrorME](
      pat: QScriptUniform[T, Symbol]): G[QSUGraph[T]] = {

    for {
      qgraph <- QSUGraph.withName[T, G](pat)

      state <- MonadState_[G, MinimizationState[T]].get
      computed <- ApplyProvenance.computeProvenanceƒ[T, G].apply(
        QSUGraph.QSUPattern(qgraph.root, pat.map(s => (s, state.dims(s)))))

      dims2 = state.dims + (qgraph.root -> computed._2)
      _ <- MonadState_[G, MinimizationState[T]].put(state.copy(dims = dims2))
    } yield qgraph
  }
}
