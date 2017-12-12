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
  HoleF,
  LeftSide,
  LeftSide3,
  LeftSideF,
  ReduceIndex,
  RightSide,
  RightSide3,
  SrcHole
}
import quasar.qscript.qsu.{QScriptUniform => QSU}
import quasar.qscript.qsu.ApplyProvenance.AuthenticatedQSU

import matryoshka.{delayEqual, BirecursiveT, EqualT, ShowT}
import matryoshka.data.free._
import monocle.Traversal
import monocle.function.Each
import monocle.std.option.{some => someP}
import monocle.syntax.fields._1
import scalaz.{Bind, Equal, Free, Monad, Scalaz, StateT, Tags}, Scalaz._   // sigh, monad/traverse conflict

final class MinimizeAutoJoins[T[_[_]]: BirecursiveT: EqualT: ShowT] private () extends QSUTTypes[T] {
  import QSUGraph.Extractors._

  private val func = construction.Func[T]
  private val srcHole: Hole = SrcHole   // wtb smart constructor

  private val J = Fixed[T[EJson]]
  private val QP = QProv[T]

  // needed to avoid bug in implicit search!  don't import QP.prov._
  private implicit val QPEq: Equal[QP.P] =
    QP.prov.provenanceEqual(scala.Predef.implicitly, Equal[QIdAccess])

  type MinStateM[F[_]] = MonadState_[F, MinimizationState]
  def MinStateM[F[_]](implicit ev: MinStateM[F]): MinStateM[F] = ev

  type RevIdxM[F[_]] = MonadState_[F, RevIdx]
  def RevIdxM[F[_]](implicit ev: RevIdxM[F]): RevIdxM[F] = ev

  def apply[F[_]: Monad: NameGenerator: PlannerErrorME](agraph: AuthenticatedQSU[T]): F[AuthenticatedQSU[T]] = {
    type G[A] = StateT[StateT[F, RevIdx, ?], MinimizationState, A]

    val back = agraph.graph rewriteM {
      case qgraph @ AutoJoin2(left, right, combiner) =>
        val combiner2: FreeMapA[Int] = combiner map {
          case LeftSide => 0
          case RightSide => 1
        }

        coalesceToMap[G](qgraph, List(left, right), combiner2)

      case qgraph @ AutoJoin3(left, center, right, combiner) =>
        val combiner2: FreeMapA[Int] = combiner map {
          case LeftSide3 => 0
          case Center => 1
          case RightSide3 => 2
        }

        coalesceToMap[G](qgraph, List(left, center, right), combiner2)
    }

    val lifted = back(MinimizationState(agraph.auth, Set())) map {
      case (MinimizationState(auth, _), graph) => AuthenticatedQSU[T](graph, auth)
    }

    lifted.eval(agraph.graph.generateRevIndex)
  }

  // the Ints are indices into branches
  private def coalesceToMap[
      G[_]: Monad: NameGenerator: RevIdxM: MinStateM: PlannerErrorME](
      qgraph: QSUGraph,
      branches: List[QSUGraph],
      combiner: FreeMapA[Int]): G[QSUGraph] = {

    val groupKeyOf: Traversal[Option[QDims], Symbol] =
      someP[QDims]           composeTraversal
      Each.each[QDims, QP.P] composePrism
      QP.prov.value          composePrism
      IdAccess.groupKey      composeLens
      _1

    for {
      state <- MinStateM[G].get

      fms = branches.zipWithIndex map {
        case (g, i) =>
          // Halt coalescing if we previously failed to coalesce this graph or
          // if this graph was grouped, to ensure joining on group keys works
          MappableRegion[T](
            s =>
              state.failed(s) ||
              groupKeyOf.exist(_ === s)(state.auth.lookupDims(s)),
            g) strengthR i
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
      G[_]: Monad: NameGenerator: RevIdxM: MinStateM: PlannerErrorME](
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
      expandSecondOrder(fm, candidates) match {
        case Some((fm, candidates)) =>
          // we need to re-expand and re-minimize, and we might end up doing additional second-order expansions
          coalesceToMap[G](qgraph, candidates, fm)

        case None =>
          lazy val reducerAttempt = candidates collect {
            case g @ QSReduce(source, buckets, reducers, repair) =>
              (source, buckets, reducers, repair)
          }

          // candidates.forall(_ ~= QSReduce)
          lazy val reducerCheck = reducerAttempt.lengthCompare(candidates.length) === 0

          lazy val leftShift12Extract: Option[(QSU.LeftShift[T, QSUGraph], Boolean)] = candidates match {
            case (ls @ LeftShift(src1, struct, idStatus, repair, rot)) :: src2 :: Nil if src1.root === src2.root =>
              Some((QSU.LeftShift(src1, struct, idStatus, repair, rot), true))

            case src2 :: (ls @ LeftShift(src1, struct, idStatus, repair, rot)) :: Nil if src1.root === src2.root =>
              Some((QSU.LeftShift(src1, struct, idStatus, repair, rot), false))

            case _ => None
          }

          if (reducerCheck) {
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
                      def rewriteBucket(bucket: QAccess[Hole]): FreeMapA[QAccess[Hole]] = bucket match {
                        case v @ Access.Value(_) => fm.map(κ(v))
                        case other => Free.pure[MapFunc, QAccess[Hole]](other)
                      }

                      (
                        source2,
                        buckets.map(_.flatMap(rewriteBucket)),
                        reducers.map(_.map(_.flatMap(κ(fm)))),
                        repair)

                    case _ => desc
                  }
              }

              // we know we're non-empty by structure of outer match
              (source, buckets, _, _) = extended.head

              auth <- MinStateM[G].gets(_.auth)
              sourceDims <- auth.lookupDimsE[G](source.root)

              // do we have the same provenance at our roots?
              rootProvCheck <- Tags.Conjunction.unsubst(extended foldMapM { case (src, _, _, _) =>
                auth.lookupDimsE[G](src.root) map (ds => (ds === sourceDims).conjunction)
              })

              // we need a stricter check than just provenance, since we have to inline maps
              back <- if (rootProvCheck
                  && extended.forall(_._2 === buckets)
                  && extended.forall(_._1.root === source.root)) {

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

                for {
                  red <- updateGraph[G](redPat)
                  back = qgraph.overwriteAtRoot(QSU.Map[T, Symbol](red.root, adjustedFM)) :++ red

                  _ <- updateForCoalesce[G](candidates, red.root)
                } yield back
              } else {
                failure[G](qgraph)
              }
            } yield back
          } else {
            leftShift12Extract match {
              case Some((QSU.LeftShift(src, struct, idStatus, repair, rot), leftToRight)) =>
                val fm2 = if (leftToRight)
                  fm
                else
                  fm.map(1 - _)   // we know the domain is {0, 1}, so we invert the indices

                val repair2 = fm2 flatMap {
                  case 0 => repair
                  case 1 => LeftSideF[T]
                }


                for {
                  back <- qgraph.overwriteAtRoot(
                    QSU.LeftShift[T, Symbol](src.root, struct, idStatus, repair2, rot)).point[G]

                  _ <- updateForCoalesce[G](candidates, qgraph.root)
                } yield back
              case None =>
                failure[G](qgraph)
            }
          }
      }
  }

  private def failure[G[_]: Monad: MinStateM](graph: QSUGraph): G[QSUGraph] = {
    val change = MinStateM[G] modify { state =>
      state.copy(failed = state.failed + graph.root)
    }

    change.map(_ => graph)
  }

  private def updateForCoalesce[G[_]: Bind: MinStateM](
      candidates: List[QSUGraph],
      newRoot: Symbol): G[Unit] = {

    MinStateM[G] modify { state =>
      val auth2 = candidates.foldLeft(state.auth) { (auth, c) =>
        if (c.root =/= newRoot)
          auth.supplant(c.root, newRoot)
        else
          auth
      }

      state.copy(auth = auth2)
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

  private def updateGraph[G[_]: Monad: NameGenerator: RevIdxM: MinStateM: PlannerErrorME](
      pat: QScriptUniform[Symbol])
      : G[QSUGraph] = {

    for {
      qgraph <- QSUGraph.withName[T, G](pat)

      state <- MinStateM[G].get

      computed <- ApplyProvenance.computeProvenance[T, StateT[G, QAuth, ?]](qgraph).exec(state.auth)

      _ <- MinStateM[G].put(state.copy(auth = computed))
    } yield qgraph
  }

  case class MinimizationState(auth: QAuth, failed: Set[Symbol])
}

object MinimizeAutoJoins {
  def apply[
      T[_[_]]: BirecursiveT: EqualT: ShowT,
      F[_]: Monad: NameGenerator: PlannerErrorME]
      (agraph: AuthenticatedQSU[T])
      : F[AuthenticatedQSU[T]] =
    taggedInternalError("MinimizeAutoJoins", new MinimizeAutoJoins[T].apply[F](agraph))
}
