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
import quasar.fp.ShowT
import quasar.physical.marklogic.xquery._
import quasar.physical.marklogic.xquery.syntax._
import quasar.qscript._

import matryoshka._
import scalaz._, Scalaz._

private[qscript] final class QScriptCorePlanner[T[_[_]]: Recursive: ShowT] extends MarkLogicPlanner[QScriptCore[T, ?]] {
  import expr.{func, let_}

  val plan: AlgebraM[Planning, QScriptCore[T, ?], XQuery] = {
    case Map(src, f) =>
      mapFuncXQuery(f, src).point[Planning]

    case Reduce(src, bucket, reducers, repair) =>
      XQuery(s"((: REDUCE :)$src)").point[Planning]

    case Sort(src, bucket, order) =>
      XQuery(s"((: SORT :)$src)").point[Planning]

    case Filter(src, f) =>
      fn.filter(func("$x") { mapFuncXQuery(f, "$x".xqy) }, src)
        .point[Planning]

    case Take(src, from, count) =>
      (rebaseXQuery(from, src) |@| rebaseXQuery(count, src))((fm, ct) =>
        src(fm to ct))

    case Drop(src, from, count) =>
      (rebaseXQuery(from, src) |@| rebaseXQuery(count, src))((fm, ct) =>
        let_("$x" -> src) return_ mkSeq_(
          fn.subsequence("$x".xqy, 1.xqy, some(fm)),
          fn.subsequence("$x".xqy, ct)))
  }
}
