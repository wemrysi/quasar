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

package quasar.physical.marklogic.qscript

import quasar.Predef._
import quasar.NameGenerator
import quasar.fp.ShowT
import quasar.physical.marklogic.xquery._
import quasar.physical.marklogic.xquery.syntax._
import quasar.qscript._

import matryoshka._
import scalaz._, Scalaz._

private[qscript] final class SourcedPathablePlanner[T[_[_]]: Recursive: ShowT] extends MarkLogicPlanner[SourcedPathable[T, ?]] {
  import expr.{for_, let_}

  def plan[F[_]: NameGenerator: Monad]: AlgebraM[PlanningT[F, ?], SourcedPathable[T, ?], XQuery] = {
    case LeftShift(src, struct, repair) =>
      liftP(for {
        s       <- freshVar[F]
        l       <- freshVar[F]
        r       <- freshVar[F]
        extract <- mapFuncXQuery(struct, s.xqy)
        lshift  <- local.leftShift(l.xqy)
        merge   <- mergeXQuery(repair, l.xqy, r.xqy)
      } yield for_ (s -> src) let_ (l -> extract, r -> lshift) return_ merge)

    case Union(src, lBranch, rBranch) =>
      for {
        s <- liftP(freshVar[F])
        l <- rebaseXQuery(lBranch, s.xqy)
        r <- rebaseXQuery(rBranch, s.xqy)
      } yield let_(s -> src) return_ (l union r)
  }
}
