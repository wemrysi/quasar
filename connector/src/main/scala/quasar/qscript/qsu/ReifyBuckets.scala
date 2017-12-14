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

import quasar.Planner.{InternalError, PlannerErrorME}
import quasar.fp.symbolOrder
import quasar.fp.ski.ι
import quasar.qscript.{Hole, HoleF, ReduceIndexF, SrcHole}
import ApplyProvenance.AuthenticatedQSU

import matryoshka._
import scalaz.{Id, ISet, Monad, Scalaz}, Scalaz._

final class ReifyBuckets[T[_[_]]: BirecursiveT: EqualT: ShowT] private () extends QSUTTypes[T] {
  import QSUGraph.Extractors._

  val prov = QProv[T]
  val qsu  = QScriptUniform.Optics[T]

  def apply[F[_]: Monad: PlannerErrorME](aqsu: AuthenticatedQSU[T])
      : F[AuthenticatedQSU[T]] = {

    val bucketsReified = aqsu.graph.rewriteM[F] {
      case g @ LPReduce(source, reduce) =>
        for {
          res <- bucketsFor[F](aqsu.auth, source.root)

          (srcs, buckets) = res

          discovered <- srcs.toList.toNel.fold((source.root, HoleF[T]).point[F]) { syms =>
            val region =
              syms.findMapM[Id.Id, (Symbol, FreeMap)](
                s => MappableRegion.unaryOf(s, source) strengthL s)

            region getOrElseF PlannerErrorME[F].raiseError(
              InternalError(
                s"Expected to find a mappable region in ${source.root} based on one of ${syms}.",
                None))
          }

          (newSrc, fm) = discovered

        } yield {
          g.overwriteAtRoot(qsu.qsReduce(
            newSrc,
            buckets,
            List(reduce as fm),
            ReduceIndexF[T](0.right)))
        }

      case g @ QSSort(source, Nil, keys) =>
        val src = source.root

        bucketsFor[F](aqsu.auth, src) map { case (_, buckets) =>
          g.overwriteAtRoot(qsu.qsSort(src, buckets, keys))
        }
    }

    bucketsReified map (g => aqsu.copy(graph = g))
  }

  ////

  private def bucketsFor[F[_]: Monad: PlannerErrorME]
      (qauth: QAuth, vertex: Symbol)
      : F[(ISet[Symbol], List[FreeAccess[Hole]])] =
    for {
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
}

object ReifyBuckets {
  def apply[T[_[_]]: BirecursiveT: EqualT: ShowT, F[_]: Monad: PlannerErrorME]
      (aqsu: AuthenticatedQSU[T])
      : F[AuthenticatedQSU[T]] =
    taggedInternalError("ReifyBuckets", new ReifyBuckets[T].apply[F](aqsu))
}
