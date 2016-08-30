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
import quasar.ejson.EJson
import quasar.fp._
import quasar.physical.marklogic.ejson.AsXQuery
import quasar.physical.marklogic.xquery._
import quasar.physical.marklogic.xquery.syntax._
import quasar.qscript.{MapFunc, MapFuncs}, MapFuncs._

import matryoshka._, Recursive.ops._
import scalaz.Monad
import scalaz.std.option._
import scalaz.syntax.monad._
import scalaz.syntax.show._

object MapFuncPlanner {
  import expr.{if_, let_}

  def apply[T[_[_]]: Recursive: ShowT, F[_]: NameGenerator: Monad]: AlgebraM[F, MapFunc[T, ?], XQuery] = {
    case Constant(ejson) => ejson.cata(AsXQuery[EJson].asXQuery).point[F]

    // array
    case Length(arr) => fn.count(arr).point[F]

    // time TODO

    // math
    case Negate(x) => (-x).point[F]
    case Add(x, y) => (x + y).point[F]
    case Multiply(x, y) => (x * y).point[F]
    case Subtract(x, y) => (x - y).point[F]
    case Divide(x, y) => (x div y).point[F]
    case Modulo(x, y) => (x mod y).point[F]
    case Power(b, e) => math.pow(b, e).point[F]

    // relations
    // TODO: When to use value vs. general comparisons?
    case Not(x) => fn.not(x).point[F]
    case Eq(x, y) => (x eq y).point[F]
    case Neq(x, y) => (x ne y).point[F]
    case Lt(x, y) => (x lt y).point[F]
    case Lte(x, y) => (x le y).point[F]
    case Gt(x, y) => (x gt y).point[F]
    case Gte(x, y) => (x ge y).point[F]
    case And(x, y) => (x and y).point[F]
    case Or(x, y) => (x or y).point[F]

    case Cond(p, t, f) => if_(p).then_(t).else_(f).point[F]

    // string
    case Lower(s) => fn.lowerCase(s).point[F]
    case Upper(s) => fn.upperCase(s).point[F]
    case ToString(x) => fn.string(x).point[F]
    case Substring(s, loc, len) => fn.substring(s, loc + 1.xqy, some(len)).point[F]

    // structural
    case MakeArray(x) => ejson.singletonArray(x).point[F]
    case MakeMap(k, v) => ejson.singletonMap(k, v).point[F]
    case ConcatArrays(x, y) => ejson.arrayConcat(x, y).point[F]

    // TODO: Handle EJson map as well as XML nodes
    case ProjectField(src, field) => (src `/` field).point[F]

    // TODO: Handle complex 'idx' expressions, this likely only works with literals, could use 'let' but
    //       will need some namegen to avoid shadowing.
    case ProjectIndex(arr, idx) =>
      (arr `/` s"child::${ejson.arrayEltName}[$idx + 1]".xs `/` "child::node()".xs).point[F]

    // other
    case Range(x, y) => (x to y).point[F]

    case ZipMapKeys(m) =>
      for {
        src  <- freshVar[F]
        zmk  <- ejson.zipMapKeys(src.xqy)
        zmnk <- local.zipMapNodeKeys(src.xqy)
      } yield {
        let_(src -> m) return_ {
          if_(ejson.isMap(src.xqy)) then_ zmk else_ zmnk
        }
      }

    case mapFunc => s"(: ${mapFunc.shows} :)()".xqy.point[F]
  }
}
