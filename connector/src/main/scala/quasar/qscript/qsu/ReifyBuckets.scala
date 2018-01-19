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

import slamdata.Predef._

import quasar.NameGenerator
import quasar.Planner.PlannerErrorME
import quasar.contrib.scalaz.MonadState_
import quasar.fp.symbolOrder
import quasar.fp.ski.ι
import quasar.qscript.{construction, Hole, HoleF, ReduceFunc, ReduceIndexF, SrcHole}
import ApplyProvenance.AuthenticatedQSU

import matryoshka._
import scalaz.{ISet, Monad, Scalaz, StateT}, Scalaz._

final class ReifyBuckets[T[_[_]]: BirecursiveT: EqualT: ShowT] private () extends QSUTTypes[T] {
  import QSUGraph.Extractors._

  val prov = QProv[T]
  val qsu  = QScriptUniform.Optics[T]
  val func = construction.Func[T]

  def apply[F[_]: Monad: NameGenerator: PlannerErrorME](aqsu: AuthenticatedQSU[T])
      : F[AuthenticatedQSU[T]] = {

    type G[A] = StateT[StateT[F, RevIdx, ?], QAuth, A]

    val bucketsReified = aqsu.graph.rewriteM[G] {
      case g @ LPReduce(source, reduce) =>
        for {
          res <- bucketsFor[G](source.root)

          (srcs, buckets0) = res

          reifiedG <- srcs.findMin match {

            case Some(sym) =>
              MappableRegion.unaryOf(sym, source) map { fm =>
                g.overwriteAtRoot(mkReduce(sym, buckets0, reduce as fm))
              } getOrElseF {
                val combine = func.ConcatMaps(
                  func.MakeMapS(GroupedKey, func.LeftSide),
                  func.MakeMapS(ReduceExprKey, func.RightSide))

                val buckets =
                  buckets0.map(_ flatMap { access =>
                    if (Access.valueHole.isEmpty(access))
                      func.Hole as access
                    else
                      func.ProjectKeyS(func.Hole, GroupedKey) as access
                  })

                for {
                  autojoined <- QSUGraph.withName[T, G]("rbu")(
                    qsu.autojoin2(sym, source.root, combine))

                  qauth0 <- MonadState_[G, QAuth].get
                  qauth1 <- ApplyProvenance.computeProvenance[T, G](autojoined)
                               .exec(qauth0).liftM[StateT[?[_], QAuth, ?]]

                  reduceExpr = func.ProjectKeyS(func.Hole, ReduceExprKey)
                  reduced = g.overwriteAtRoot(mkReduce(autojoined.root, buckets, reduce as reduceExpr))

                  qauth2 <- ApplyProvenance.computeProvenance[T, G](reduced)
                               .exec(qauth1).liftM[StateT[?[_], QAuth, ?]]
                  _ <- MonadState_[G, QAuth].put(qauth2)
                } yield reduced :++ autojoined
              }

            case None =>
              g.overwriteAtRoot(mkReduce(source.root, buckets0, reduce as HoleF)).point[G]
          }

        } yield reifiedG

      case g @ QSSort(source, Nil, keys) =>
        val src = source.root

        bucketsFor[G](src) map { case (_, buckets) =>
          g.overwriteAtRoot(qsu.qsSort(src, buckets, keys))
        }
    }

    bucketsReified.run(aqsu.auth).eval(aqsu.graph.generateRevIndex) map {
      case (auth, graph) => ApplyProvenance.AuthenticatedQSU(graph, auth)
    }
  }

  ////

  private val GroupedKey = "grouped"
  private val ReduceExprKey = "reduce_expr"

  private def bucketsFor[F[_]: Monad: PlannerErrorME: MonadState_[?[_], QAuth]]
      (vertex: Symbol)
      : F[(ISet[Symbol], List[FreeAccess[Hole]])] =
    for {
      qauth <- MonadState_[F, QAuth].get

      vdims <- qauth.lookupDimsE[F](vertex)

      ids = prov.buckets(prov.reduce(vdims)).toList

      res <- ids traverse {
        case id @ IdAccess.GroupKey(s, i) =>
          qauth.lookupGroupKeyE[F](s, i)
            .map(fm => (ISet.singleton(s), fm as Access.value[prov.D, Hole](SrcHole)))

        case other =>
          (ISet.empty[Symbol], HoleF[T] as Access.id[prov.D, Hole](other, SrcHole)).point[F]
      }
    } yield res.unfzip leftMap (_.foldMap(ι))

  private def mkReduce[A](
      src: A,
      buckets: List[FreeAccess[Hole]],
      reducer: ReduceFunc[FreeMap])
      : QScriptUniform[A] =
    qsu.qsReduce(
      src,
      buckets,
      List(reducer),
      ReduceIndexF[T](0.right))
}

object ReifyBuckets {
  def apply[T[_[_]]: BirecursiveT: EqualT: ShowT, F[_]: Monad: NameGenerator: PlannerErrorME]
      (aqsu: AuthenticatedQSU[T])
      : F[AuthenticatedQSU[T]] =
    taggedInternalError("ReifyBuckets", new ReifyBuckets[T].apply[F](aqsu))
}
