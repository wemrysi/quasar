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
import quasar.fp.ski.κ
import quasar.fp.symbolOrder
import quasar.qscript.{FreeMap, FreeMapA, Hole, HoleF, ReduceIndexF, SrcHole}

import matryoshka._
import scalaz.{Id, ISet, Monad, Traverse}
import scalaz.std.list._
import scalaz.std.option._
import scalaz.syntax.either._
import scalaz.syntax.foldable._
import scalaz.syntax.monad._
import scalaz.syntax.std.option._

object ReifyBuckets {
  import QSUGraph.Extractors._
  import ApplyProvenance.AuthenticatedQSU

  def apply[T[_[_]]: BirecursiveT: EqualT, F[_]: Monad: PlannerErrorME](aqsu: AuthenticatedQSU[T])
      : F[AuthenticatedQSU[T]] = {

    val prov = new QProv[T]
    val qsu  = QScriptUniform.Optics[T]
    val LF   = Traverse[List].compose[FreeMapA[T, ?]]
    val LFA  = LF.compose[Access]

    val bucketsReified = aqsu.graph rewriteM {
      case g @ LPReduce(source, reduce) =>
        val buckets = prov.buckets(prov.reduce(aqsu.dims(source.root)))
        val srcs = LF.foldMap(buckets)(a => ISet.fromFoldable(Access.value.getOption(a))).toIList

        val discovered: F[(Symbol, FreeMap[T])] =
          srcs.toNel.fold((source.root, HoleF[T]).point[F]) { syms =>
            val region =
              syms.findMapM[Id.Id, (Symbol, FreeMap[T])](
                s => MappableRegion.unaryOf(s, source) strengthL s)

            region getOrElseF {
              PlannerErrorME[F].raiseError(InternalError(
                s"ReifyBuckets: Expected to find a mappable region in ${source.root} based on one of ${syms}.",
                None))
            }
          }

        discovered map { case (newSrc, fm) =>
          g.overwriteAtRoot(qsu.qsReduce(
            newSrc,
            LFA.map(buckets)(κ(SrcHole : Hole)),
            List(reduce as fm),
            ReduceIndexF[T](0.right)))
        }

      case g @ QSSort(source, Nil, keys) =>
        val src = source.root
        val buckets = prov.buckets(prov.reduce(aqsu.dims(src)))

        g.overwriteAtRoot(qsu.qsSort(
          src,
          LFA.map(buckets)(κ(SrcHole : Hole)),
          keys)).point[F]
    }

    bucketsReified map (g => aqsu.copy(graph = g))
  }
}
