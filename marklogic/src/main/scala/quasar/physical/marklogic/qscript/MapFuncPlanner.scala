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
import quasar.fp.ShowT
import quasar.physical.marklogic.MonadError_
import quasar.physical.marklogic.ejson.AsXQuery
import quasar.physical.marklogic.validation._
import quasar.physical.marklogic.xml._
import quasar.physical.marklogic.xquery._
import quasar.physical.marklogic.xquery.syntax._
import quasar.qscript.{MapFunc, MapFuncs}, MapFuncs._

import eu.timepit.refined.refineV
import matryoshka._, Recursive.ops._
import scalaz.std.option._
import scalaz.syntax.monad._
import scalaz.syntax.show._
import scalaz.syntax.std.either._

object MapFuncPlanner {
  import expr.{if_, let_}, axes._

  def apply[T[_[_]]: Recursive: ShowT, F[_]: NameGenerator: PrologW: MonadPlanErr]: AlgebraM[F, MapFunc[T, ?], XQuery] = {
    case Constant(ejson) => ejson.cata(AsXQuery[EJson].asXQuery).point[F]

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
    case MakeArray(x) =>
      ejson.singletonArray[F] apply x

    case MakeMap(k, v) => k match {
      case XQuery.StringLit(s) =>
        refineV[IsNCName](s).disjunction map { ncname =>
          val qn = QName.local(NCName(ncname))
          ejson.singletonObject[F] apply (qn.xs, v)
        } getOrElse invalidQName(s)

      case _ => ejson.singletonObject[F] apply (k, v)
    }

    case ConcatArrays(x, y) =>
      ejson.arrayConcat[F] apply (x, y)

    case ProjectField(src, field) => field match {
      case XQuery.Step(_) =>
        (src `/` field).point[F]

      case XQuery.StringLit(s) =>
        refineV[IsNCName](s).disjunction map { ncname =>
          freshVar[F] map { m =>
            let_(m -> src) return_ {
              m.xqy `/` child(QName.local(NCName(ncname)))
            }
          }
        } getOrElse invalidQName(s)

      case _ =>
        (freshVar[F] |@| freshVar[F]) { (m, k) =>
          let_(m -> src, k -> field) return_ {
            m.xqy `/` k.xqy
          }
        }
    }

    // TODO: If we don't specify the element type, this should work for any element
    case ProjectIndex(arr, idx) =>
      (freshVar[F] |@| ejson.arrayEltN.qn) { (i, arrElt) =>
        let_(i -> idx) return_ {
          arr `/` child(arrElt)(i.xqy + 1.xqy) `/` child.node()
        }
      }

    // other
    case Range(x, y)   => (x to y).point[F]

    case ZipMapKeys(m) => qscript.zipMapNodeKeys[F] apply m

    case mapFunc => s"(: ${mapFunc.shows} :)()".xqy.point[F]
  }

  ////

  private def invalidQName[F[_]: MonadPlanErr, A](s: String): F[A] =
    MonadError_[F, MarkLogicPlannerError].raiseError(
      MarkLogicPlannerError.invalidQName(s))
}
