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

import quasar.Predef.{Map => _, _}
import quasar.NameGenerator
import quasar.fp.ShowT
import quasar.physical.marklogic.xquery._
import quasar.physical.marklogic.xquery.syntax._
import quasar.qscript._

import matryoshka._
import scalaz._, Scalaz._

private[qscript] final class QScriptCorePlanner[T[_[_]]: Recursive: ShowT] extends MarkLogicPlanner[QScriptCore[T, ?]] {
  import expr.{func, if_, let_}

  def plan[F[_]: NameGenerator: Monad]: AlgebraM[PlanningT[F, ?], QScriptCore[T, ?], XQuery] = {
    case Map(src, f) =>
      liftP(for {
        x <- freshVar[F]
        g <- mapFuncXQuery(f, x.xqy)
      } yield fn.map(func(x) { g }, src))

    case Reduce(src, bucket, reducers, repair) =>
      XQuery(s"((: REDUCE :)$src)").point[PlanningT[F, ?]]

    case Sort(src, bucket, order) =>
      XQuery(s"((: SORT :)$src)").point[PlanningT[F, ?]]

    case Filter(src, f) =>
      liftP(for {
        x <- freshVar[F]
        p <- mapFuncXQuery(f, x.xqy)
      } yield fn.filter(func(x) { p }, src))

    // NB: XQuery sequences use 1-based indexing.
    case Take(src, from, count) =>
      for {
        s   <- liftP(freshVar[F])
        f   <- liftP(freshVar[F])
        c   <- liftP(freshVar[F])
        fm  <- rebaseXQuery(from, s.xqy)
        ct  <- rebaseXQuery(count, s.xqy)
      } yield let_(
        s -> src,
        f -> (fm + 1.xqy),
        c -> ct
      ) return_ s.xqy(f.xqy to c.xqy)

    case Drop(src, from, count) =>
      for {
        s  <- liftP(freshVar[F])
        f  <- liftP(freshVar[F])
        c  <- liftP(freshVar[F])
        fm <- rebaseXQuery(from, s.xqy)
        ct <- rebaseXQuery(count, s.xqy)
      } yield {
        let_(s -> src, f -> fm, c -> ct) return_ {
          if_ (f.xqy eq 0.xqy) then_ {
            fn.subsequence(s.xqy, c.xqy)
          } else_ {
            mkSeq_(
              fn.subsequence(s.xqy, 1.xqy, some(f.xqy)),
              fn.subsequence(s.xqy, c.xqy))
          }
        }
      }
  }
}
