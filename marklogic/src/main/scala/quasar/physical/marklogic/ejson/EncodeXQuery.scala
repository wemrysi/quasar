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

package quasar.physical.marklogic.ejson

import quasar.Predef._
import quasar.NameGenerator
import quasar.ejson
import quasar.physical.marklogic.{ErrorMessages, MonadErrMsgs_}
import quasar.physical.marklogic.validation._
import quasar.physical.marklogic.xquery.{ejson => ejsxqy, _}
import quasar.physical.marklogic.xquery.syntax._

import eu.timepit.refined.refineV
import matryoshka._
import scalaz._, Scalaz._

trait EncodeXQuery[M[_], F[_]] {
  def encodeXQuery: AlgebraM[M, F, XQuery]
}

object EncodeXQuery {
  def apply[M[_], F[_]](implicit EXQ: EncodeXQuery[M, F]): EncodeXQuery[M, F] = EXQ

  implicit def coproductEncodeXQuery[M[_], F[_], G[_]](implicit F: EncodeXQuery[M, F], G: EncodeXQuery[M, G]): EncodeXQuery[M, Coproduct[F, G, ?]] =
    new EncodeXQuery[M, Coproduct[F, G, ?]] {
      val encodeXQuery: AlgebraM[M, Coproduct[F, G, ?], XQuery] =
        _.run.fold(F.encodeXQuery, G.encodeXQuery)
    }

  implicit def commonEncodeXQuery[M[_]: NameGenerator: PrologW]: EncodeXQuery[M, ejson.Common] =
    new EncodeXQuery[M, ejson.Common] {
      val encodeXQuery: AlgebraM[M, ejson.Common, XQuery] = {
        case ejson.Arr(xs) => ejsxqy.seqToArray_[M](mkSeq(xs))
        case ejson.Null()  => ejsxqy.null_[M]
        case ejson.Bool(b) => b.fold(fn.True, fn.False).point[M]
        case ejson.Str(s)  => s.xs.point[M]
        case ejson.Dec(d)  => xs.double(d.toString.xs).point[M]
      }
    }

  implicit def extensionEncodeXQuery[M[_]: PrologW: MonadErrMsgs_]: EncodeXQuery[M, ejson.Extension] =
    new EncodeXQuery[M, ejson.Extension] {
      type ValM[A] = Validation[ErrorMessages, M[A]]

      implicit val valMApplicative: Applicative[ValM] =
        Applicative[Validation[ErrorMessages, ?]].compose[M]

      val encodeXQuery: AlgebraM[M, ejson.Extension, XQuery] = {
        // TODO: We'd like to be able to deconstruct 'meta' to see if it was a stringly-keyd map or some
        //       atomic type that we could turn into attributes.
        //
        // TODO: We likely have to deal with the EJson encoding of Data used in the LP -> QScript conversion
        //       so this may be needed sooner than later?
        case ejson.Meta(value, meta) => value.point[M]

        case ejson.Map(entries)      =>
          val objEntries = entries.traverse[ValM, XQuery] {
            case (XQuery.StringLit(s), value) =>
              refineV[IsNCName](s).validation map { ncname =>
                ejsxqy.renameOrWrap[M] apply (xs.QName(ncname.get.xs), value)
              } leftAs s"'$s' is not a valid XML QName.".wrapNel

            case (xqy, _) =>
              s"'$xqy' is not a supported map key in XQuery.".failureNel
          }

          objEntries.valueOr(MonadErrMsgs_[M].raiseError(_))
            .flatMap(ents => ejsxqy.mkObject[M] apply mkSeq[List](ents))

        case ejson.Byte(b)           => xs.byte(b.toInt.xqy).point[M]
        case ejson.Char(c)           => c.toString.xs.point[M]
        // TODO: There appears to be a limit on integers in MarkLogic, find out what it is
        //       and validate `i`.
        case ejson.Int(i)            => xs.integer(i.toString.xqy).point[M]
      }
    }
}
