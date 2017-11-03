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

import quasar.fp._

import scalaz._, Scalaz._

// TODO move this to a different package, maybe foundation
object IntersectGraphs {

  private final case class Edge(from: Symbol, to: Symbol)

  private object Edge {
    implicit val order: Order[Edge] = Order.order {
      case (e1, e2) =>
        Order[(Symbol, Symbol)].order((e1.from, e1.to), (e2.from, e2.to))
    }
  }

  private def findEdges[F[_]: Foldable](vertices: SMap[Symbol, F[Symbol]])
      : ISet[Edge] =
    vertices.toList foldMap {
      case (from, node) => node.foldMap(to => ISet.singleton(Edge(from, to)))
    }

  private def edgesToRoot(edges: ISet[Edge]): Symbol = {
    val (froms, tos): (ISet[Symbol], ISet[Symbol]) =
      edges.foldRight[(ISet[Symbol], ISet[Symbol])]((ISet.empty, ISet.empty)) {
        case (Edge(from, to), (froms, tos)) =>
          (froms.insert(from), tos.insert(to))
      }

    val rootCandidates: ISet[Symbol] = froms difference tos

    // Foldable[ISet]
    rootCandidates.toIList match {
      case ICons(head, INil()) => head
      // FIXME real error handling
      case _ => scala.sys.error(s"Source merging failed. Candidates: ${Show[ISet[Symbol]].shows(rootCandidates)}")
    }
  }

  def intersect[F[_]: Foldable](left: SMap[Symbol, F[Symbol]], right: SMap[Symbol, F[Symbol]])
      : Symbol =
    edgesToRoot(findEdges[F](left) intersection findEdges[F](right))
}
