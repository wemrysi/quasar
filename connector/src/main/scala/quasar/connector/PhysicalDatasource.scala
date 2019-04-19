/*
 * Copyright 2014–2018 SlamData Inc.
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

package quasar.connector

import quasar.api.QueryEvaluator
import quasar.api.resource.{ResourcePath, ResourcePathType}

import monocle.{Lens, PLens}

trait PhysicalDatasource[F[_], G[_], Q, R] extends Datasource[F, G, Q, R] {
  type PathType = ResourcePathType.Physical
}

object PhysicalDatasource {
  def evaluator[F[_], G[_], Q, R]: Lens[PhysicalDatasource[F, G, Q, R], QueryEvaluator[F, Q, R]] =
    pevaluator[F, G, Q, R, Q, R]

  def pevaluator[F[_], G[_], Q1, R1, Q2, R2]
      : PLens[PhysicalDatasource[F, G, Q1, R1], PhysicalDatasource[F, G, Q2, R2], QueryEvaluator[F, Q1, R1], QueryEvaluator[F, Q2, R2]] =
    PLens((ds: PhysicalDatasource[F, G, Q1, R1]) => ds: QueryEvaluator[F, Q1, R1]) { qe: QueryEvaluator[F, Q2, R2] => ds =>
      new PhysicalDatasource[F, G, Q2, R2] {
        val kind = ds.kind
        def evaluate(q: Q2) = qe.evaluate(q)
        def pathIsResource(p: ResourcePath) = ds.pathIsResource(p)
        def prefixedChildPaths(pfx: ResourcePath) = ds.prefixedChildPaths(pfx)
      }
    }
}

