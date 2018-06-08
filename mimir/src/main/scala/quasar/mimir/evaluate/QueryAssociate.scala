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

package quasar.mimir.evaluate

import quasar.Data
import quasar.api.ResourcePath
import quasar.api.ResourceError.ReadError
import quasar.higher.HFunctor
import quasar.qscript.QScriptEducated

import fs2.Stream
import scalaz.{\/, ~>}

sealed trait QueryAssociate[T[_[_]], F[_], G[_]]

object QueryAssociate extends QueryAssociateInstances {
  final case class Lightweight[T[_[_]], F[_], G[_]](
      f: ResourcePath => F[ReadError \/ Stream[G, Data]])
      extends QueryAssociate[T, F, G]

  final case class Heavyweight[T[_[_]], F[_], G[_]](
      f: T[QScriptEducated[T, ?]] => F[ReadError \/ Stream[G, Data]])
      extends QueryAssociate[T, F, G]

  def lightweight[T[_[_]], F[_], G[_]](
      f: ResourcePath => F[ReadError \/ Stream[G, Data]])
      : QueryAssociate[T, F, G] =
    Lightweight(f)

  def heavyweight[T[_[_]], F[_], G[_]](
      f: T[QScriptEducated[T, ?]] => F[ReadError \/ Stream[G, Data]])
      : QueryAssociate[T, F, G] =
    Heavyweight(f)
}

sealed abstract class QueryAssociateInstances {
  implicit def lhfunctor[T[_[_]], G[_]]: HFunctor[QueryAssociate[T, ?[_], G]] =
    new HFunctor[QueryAssociate[T, ?[_], G]] {
      def hmap[A[_], B[_]](qa: QueryAssociate[T, A, G])(f: A ~> B) =
        qa match {
          case QueryAssociate.Lightweight(q) =>
            QueryAssociate.Lightweight(q andThen (f(_)))

          case QueryAssociate.Heavyweight(q) =>
            QueryAssociate.Heavyweight(q andThen (f(_)))
        }
    }
}
