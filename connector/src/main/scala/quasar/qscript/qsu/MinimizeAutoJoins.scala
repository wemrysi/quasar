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

import quasar.{NameGenerator, Planner}, Planner.PlannerErrorME
import quasar.contrib.scalaz.MonadState_
import quasar.ejson.{EJson, Fixed}
import quasar.ejson.implicits._
import quasar.fp._
import quasar.fp.ski.κ
import quasar.qscript.{
  construction,
  Center,
  Hole,
  HoleF,
  LeftSide,
  LeftSide3,
  ReduceIndex,
  RightSide,
  RightSide3,
  SrcHole
}
import quasar.qscript.qsu.{QScriptUniform => QSU}
import slamdata.Predef.{Map => SMap, _}

import matryoshka.{BirecursiveT, EqualT}
import scalaz.{Free, Monad, Scalaz, StateT}, Scalaz._   // sigh, monad/traverse conflict

final class MinimizeAutoJoins[T[_[_]]: BirecursiveT: EqualT] private () extends QSUTTypes[T] {
  import ApplyProvenance.AuthenticatedQSU
  import QSUGraph.Extractors._

  private val func = construction.Func[T]
  private val srcHole: Hole = SrcHole   // wtb smart constructor

  private val J = Fixed[T[EJson]]

  private val AP = ApplyProvenance[T]
  private val QP = QProv[T]

  import QP.prov._

  def apply[F[_]: Monad: NameGenerator: PlannerErrorME](agraph: AuthenticatedQSU[T]): F[AuthenticatedQSU[T]] = {
    type G[A] = StateT[StateT[F, RevIdx, ?], QSUDims[T], A]

    val back = agraph.graph rewriteM {
      case qgraph @ AutoJoin2(left, right, combiner) =>
        val combiner2 = combiner map {
          case LeftSide => 0
          case RightSide => 1
        }

        coalesceToMap[G](qgraph, List(left, right), Free.liftF[MapFunc, Int](combiner2))

      case qgraph @ AutoJoin3(left, center, right, combiner) =>
        val combiner2 = combiner map {
          case LeftSide3 => 0
          case Center => 1
          case RightSide3 => 2
        }

        coalesceToMap[G](qgraph, List(left, center, right), Free.liftF[MapFunc, Int](combiner2))
    }

    val lifted = back(agraph.dims) map {
      case (dims, graph) => AuthenticatedQSU[T](graph, dims)
    }

    lifted.eval(agraph.graph.generateRevIndex)
  }

  // the Ints are indices into branches
  private def coalesceToMap[
      G[_]: Monad: NameGenerator: MonadState_[?[_], RevIdx]: MonadState_[?[_], QSUDims[T]]: PlannerErrorME](
      qgraph: QSUGraph,
      branches: List[QSUGraph],
      combiner: FreeMapA[Int]): G[QSUGraph] = {

    val fms = branches.zipWithIndex map {
      case (g, i) =>
        expandSecondOrder(MappableRegion.maximal(g)).map(g => (g, i))
    }

    val (remap, candidates) = minimizeSources(fms.flatMap(_.toList))

    lazy val fm = combiner.flatMap(i => fms(i).map(κ(remap(i))))

    coalesceRoots[G](qgraph, fm, candidates)
  }

  // attempt to extend by seeing through constructs like filter (mostly just filter)
  @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
  private def expandSecondOrder(fm: FreeMapA[QSUGraph]): FreeMapA[QSUGraph] = {
    fm flatMap {
      case QSFilter(source, predicate) =>
        // tune into 102.5 FM, The Source
        val sourceFM = expandSecondOrder(MappableRegion.maximal(source))
        func.Cond(sourceFM, sourceFM, func.Undefined[QSUGraph])

      // TODO should we handle LPFilter here just for completion sake?

      case g => Free.pure(g)
    }
  }

  // attempts to reduce the set of candidates to a single Map node, given a FreeMap[Int]
  // the Int indexes into the final number of distinct roots
  @SuppressWarnings(Array("org.wartremover.warts.TraversableOps"))
  private def coalesceRoots[
      G[_]: Monad: NameGenerator: MonadState_[?[_], RevIdx]: MonadState_[?[_], QSUDims[T]]: PlannerErrorME](
      qgraph: QSUGraph,
      fm: => FreeMapA[Int],
      candidates: List[QSUGraph]): G[QSUGraph] = candidates match {

    case Nil =>
      updateGraph[G](QSU.Unreferenced[T, Symbol]()) map { unref =>
        qgraph.overwriteAtRoot(QSU.Map[T, Symbol](unref.root, fm.map(κ(srcHole)))) :++ unref
      }

    case single :: Nil =>
      // if this is false, it's an assertion error
      // lazy val sanityCheck = fm.toList.forall(0 ==)

      qgraph.overwriteAtRoot(QSU.Map[T, Symbol](single.root, fm.map(κ(srcHole)))).point[G]

    case candidates =>
      val reducerAttempt = candidates collect {
        case g @ QSReduce(source, buckets, reducers, repair) =>
          (source, buckets, reducers, repair)
      }

      // candidates.forall(_ ~= QSReduce)
      if (reducerAttempt.lengthCompare(candidates.length) === 0) {
        for {
          // apply coalescence recursively to our sources
          extended <- reducerAttempt traverse {
            case desc @ (source, buckets, reducers, repair) =>
              // TODO this is a weird use of the function, but it should work
              // we're just trying to run ourselves recursively to extend the sources
              // this isn't likely to be a common case
              coalesceToMap[G](source, List(source), Free.pure(0)) map {
                // coalesceToMap is always going to return... a Map, but
                // what we care about is the source and the fm, not the
                // node itself.  so we pull it apart (this is what's weird, btw)
                //
                // TODO short circuit this a bit if fm === Free.pure(Hole)
                // I'm pretty sure the fallthrough case will never be hit
                case Map(source2, fm) =>
                  (
                    source2,
                    buckets,    // buckets are based on access, so we don't need to modify here
                    reducers.map(_.map(_.flatMap(κ(fm)))),
                    repair)

                case _ => desc
              }
          }

          // we know we're non-empty by structure of outer match
          (source, buckets, _, _) = extended.head

          dims <- MonadState_[G, QSUDims[T]].get

          // do we have the same provenance at our roots?
          rootProvCheck = extended.forall(t => dims(t._1.root) === dims(source.root))

          // we need a stricter check than just provenance, since we have to inline maps
          back <- if (rootProvCheck && extended.forall(_._1.root === source.root)) {
            val lifted = extended.zipWithIndex map {
              case ((_, buckets, reducers, repair), i) =>
                (
                  buckets,
                  reducers,
                  // we use maps here to avoid definedness issues in reduction results
                  func.MakeMap(func.Constant(J.str(i.toString)), repair))
            }

            // this is fine, because we can't be here if candidates is empty
            // doing it this way avoids an extra (and useless) Option state in the fold
            val (_, lhreducers, lhrepair) = lifted.head

            // squish all the reducers down into lhead
            val (_, (reducers, repair)) =
              lifted.tail.foldLeft((lhreducers.length, (lhreducers, lhrepair))) {
                case ((roffset, (lreducers, lrepair)), (_, rreducers, rrepair)) =>
                  val reducers = lreducers ::: rreducers

                  val roffset2 = roffset + rreducers.length

                  val repair = func.ConcatMaps(
                    lrepair,
                    rrepair map {
                      case ReduceIndex(e) =>
                        ReduceIndex(e.rightMap(_ + roffset))
                    })

                  (roffset2, (reducers, repair))
              }

            // 107.7, All chiropractors, all the time
            val adjustedFM = fm flatMap { i =>
              // get the value back OUT of the map
              func.ProjectKey(HoleF[T], func.Constant(J.str(i.toString)))
            }

            val redPat = QSU.QSReduce[T, Symbol](source.root, buckets, reducers, repair)

            updateGraph[G](redPat) map { red =>
              qgraph.overwriteAtRoot(QSU.Map[T, Symbol](red.root, adjustedFM)) :++ red
            }
          } else {
            qgraph.point[G]
          }
        } yield back
      } else {
        qgraph.point[G]
      }
  }

  // we return a remap function along with a minimized list
  // the remapper can be used to determine which input indices
  // collapsed into what output indices
  // note that if everything is Unreferenced, remap will be κ(-1),
  // which is weird but harmless
  private def minimizeSources(sources: List[(QSUGraph, Int)]): (Int => Int, List[QSUGraph]) = {
    val (_, remap, offset, minimized) =
      sources.foldRight((Set[Symbol](), SMap[Int, Int](), 0, List[QSUGraph]())) {
        // just drop unreferenced sources entirely
        case ((Unreferenced(), i), (mask, remap, offset, acc)) =>
          (mask, remap + (i -> offset), offset, acc)

        case ((g, i), (mask, remap, offset, acc)) =>
          if (mask(g.root))
            (mask, remap + (i -> offset), offset, acc)
          else
            (mask + g.root, remap + (i -> offset), offset + 1, g :: acc)
      }

    // if we're asking for a non-existent remap index, it's because it isn't referenced
    // oh yeah, and we have to complement the remap because of the foldRight
    // the - 1 here comes from the fact that we over-increment offset above
    (i => remap.mapValues(offset - 1 - _).getOrElse(i, -1), minimized)
  }

  private def updateGraph[
      G[_]: Monad: NameGenerator: MonadState_[?[_], RevIdx]: MonadState_[?[_], QSUDims[T]]: PlannerErrorME](
      pat: QScriptUniform[Symbol]): G[QSUGraph] = {

    for {
      qgraph <- QSUGraph.withName[T, G](pat)

      dims <- MonadState_[G, QSUDims[T]].get
      computed <- AP.computeProvenanceƒ[G].apply(QSUGraph.QSUPattern(qgraph.root, pat.map(dims)))
      dims2 = dims + (qgraph.root -> computed)
      _ <- MonadState_[G, QSUDims[T]].put(dims2)
    } yield qgraph
  }
}

object MinimizeAutoJoins {
  def apply[T[_[_]]: BirecursiveT: EqualT]: MinimizeAutoJoins[T] =
    new MinimizeAutoJoins[T]
}
