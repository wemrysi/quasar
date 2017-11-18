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
import quasar.ejson.{EJson, Fixed}
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

import matryoshka.BirecursiveT
import scalaz.{Free, Monad, Scalaz, StateT}, Scalaz._   // sigh, monad/traverse conflict

final class MinimizeAutoJoins[T[_[_]]: BirecursiveT] private () extends QSUTTypes[T] {
  import QSUGraph.Extractors._

  private val func = construction.Func[T]
  private val srcHole: Hole = SrcHole   // wtb smart constructor

  private val J = Fixed[T[EJson]]

  def apply[F[_]: Monad: NameGenerator](qgraph: QSUGraph): F[QSUGraph] = {
    type G[A] = StateT[F, RevIdx, A]

    val back = qgraph rewriteM {
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

    back.eval(qgraph.generateRevIndex)
  }

  // the Ints are indices into branches
  private def coalesceToMap[G[_]: Monad: NameGenerator: MonadState_[?[_], RevIdx]](
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
  private def coalesceRoots[G[_]: Monad: NameGenerator: MonadState_[?[_], RevIdx]](
      qgraph: QSUGraph,
      fm: => FreeMapA[Int],
      candidates: List[QSUGraph]): G[QSUGraph] = candidates match {

    case Nil =>
      QSUGraph.withName[T, G](QSU.Unreferenced[T, Symbol]()) map { unref =>
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

      if (reducerAttempt.lengthCompare(candidates.length) === 0) {
        for {
          extended <- reducerAttempt traverse {
            case desc @ (source, buckets, reducers, repair) =>
              // TODO this is a weird use of the function, but it should work
              // we're just trying to run ourselves recursively to extend the sources
              // this isn't likely to be a common case
              coalesceToMap[G](source, List(source), Free.pure(0)) map {
                case Map(source2, fm) =>
                  (
                    source2,
                    buckets.map(_.flatMap(κ(fm))),
                    reducers.map(_.map(_.flatMap(κ(fm)))),
                    repair)

                case _ => desc
              }
          }

          // we know we're non-empty by structure of outer match
          (source, _, _, _) = extended.head

          back <- if (extended.forall(_._1.root === source.root)) {
            val lifted = extended.zipWithIndex map {
              case ((_, buckets, reducers, repair), i) =>
                (
                  buckets,
                  reducers,
                  // we use maps here to avoid definedness issues in reduction results
                  func.MakeMap(repair, func.Constant(J.str(i.toString))))
            }

            val lhead = lifted.head

            val (_, (buckets, reducers, repair)) =
              lifted.tail.foldLeft((lhead._1.length, lhead)) {
                case ((offset, (lbuckets, lreducers, lrepair)), (rbuckets, rreducers, rrepair)) =>
                  val buckets = lbuckets ::: rbuckets
                  val reducers = lreducers ::: rreducers
                  val offset2 = offset + rbuckets.length

                  val repair = func.ConcatMaps(
                    lrepair,
                    rrepair map {
                      case ReduceIndex(e) =>
                        ReduceIndex(e.bimap(_ + offset, _ + offset))
                    })

                  (offset2, (buckets, reducers, repair))
              }

            // 107.7, All chiropractors, all the time
            val adjustedFM = fm flatMap { i =>
              // get the value back OUT of the map
              func.ProjectKey(HoleF, func.Constant(J.str(i.toString)))
            }

            val redPat = QSU.QSReduce[T, Symbol](source.root, buckets, reducers, repair)

            QSUGraph.withName[T, G](redPat) map { red =>
              qgraph.overwriteAtRoot(QSU.Map[T, Symbol](red.root, adjustedFM))
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
    val (_, remap, _, minimized) =
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
    (i => remap.getOrElse(i, -1), minimized)
  }
}

object MinimizeAutoJoins {
  def apply[T[_[_]]: BirecursiveT]: MinimizeAutoJoins[T] = new MinimizeAutoJoins[T]
}
