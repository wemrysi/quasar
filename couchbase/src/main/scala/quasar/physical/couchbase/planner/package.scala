/*
 * Copyright 2014–2016 SlamData Inc.
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

package quasar.physical.couchbase

import quasar.Predef._
import quasar.{NameGenerator, PlannerErrT, PhaseResults, PhaseResultT}

import scalaz._

package object planner {

  type CBPhaseLog[F[_], A] = PlannerErrT[PhaseResultT[F, ?], A]

  def prtell[F[_]: Monad: MonadTell[?[_], PhaseResults]](pr: PhaseResults) =
    MonadTell[F, PhaseResults].tell(pr)

  def n1ql(n1ql: N1QL): String =
    N1QL.n1qlQueryString(n1ql)

  def genName[F[_]: Functor: NameGenerator]: F[String] =
    NameGenerator[F].prefixedName("_")

}
