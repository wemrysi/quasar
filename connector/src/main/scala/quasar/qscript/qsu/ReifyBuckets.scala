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

import quasar.qscript.{HoleF, ReduceIndexF}

import matryoshka._
import scalaz.syntax.either._
import scalaz.syntax.functor._

object ReifyBuckets {
  import QSUGraph.Extractors._
  import ApplyProvenance.AuthenticatedQSU

  def apply[T[_[_]]: BirecursiveT: EqualT](aqsu: AuthenticatedQSU[T]): AuthenticatedQSU[T] = {
    val prov = new QProv[T]
    val qsu  = QScriptUniform.Optics[T]

    val bucketsReified = aqsu.graph rewrite {
      case g @ LPReduce(source, reduce) =>
        val src = source.root
        val buckets = prov.buckets(prov.reduce(aqsu.dims(src)))

        g.overwriteAtRoot(qsu.qsReduce(
          src, buckets, List(reduce as HoleF[T]), ReduceIndexF[T](0.right)))

      case g @ QSSort(source, Nil, keys) =>
        val src = source.root
        val buckets = prov.buckets(prov.reduce(aqsu.dims(src)))

        g.overwriteAtRoot(qsu.qsSort(src, buckets, keys))
    }

    aqsu.copy(graph = bucketsReified)
  }
}
