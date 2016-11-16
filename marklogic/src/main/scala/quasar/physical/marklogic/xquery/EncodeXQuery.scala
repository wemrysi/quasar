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

package quasar.physical.marklogic.xquery

import quasar.Predef._
import quasar.Data
import quasar.{ejson => ejs}
import quasar.physical.marklogic.{ErrorMessages, MonadErrMsgs_}
import quasar.physical.marklogic.prisms._
import quasar.physical.marklogic.validation._
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

  implicit def commonEncodeXQuery[M[_]: PrologW]: EncodeXQuery[M, ejs.Common] =
    new EncodeXQuery[M, ejs.Common] {
      val encodeXQuery: AlgebraM[M, ejs.Common, XQuery] = {
        case ejs.Arr(xs) => ejson.seqToArray_[M](mkSeq(xs))
        case ejs.Null()  => ejson.null_[M]
        case ejs.Bool(b) => b.fold(fn.True, fn.False).point[M]
        case ejs.Str(s)  => s.xs.point[M]
        case ejs.Dec(d)  => xs.double(d.toString.xqy).point[M]
      }
    }

  implicit def extensionEncodeXQuery[M[_]: PrologW: MonadErrMsgs_]: EncodeXQuery[M, ejs.Extension] =
    new EncodeXQuery[M, ejs.Extension] {
      val encodeXQuery: AlgebraM[M, ejs.Extension, XQuery] = {
        // TODO: We'd like to be able to deconstruct 'meta' to see if it was a stringly-keyd map or some
        //       atomic type that we could turn into attributes.
        //
        // TODO: We likely have to deal with the ejs encoding of Data used in the LP -> QScript conversion
        //       so this may be needed sooner than later?
        case ejs.Meta(value, meta) => value.point[M]
        case ejs.Map(entries)      => objAsXQuery(entries)(XQuery.stringLit.getOption, _.point[M])
        case ejs.Byte(b)           => xs.byte(b.toInt.xqy).point[M]
        case ejs.Char(c)           => c.toString.xs.point[M]
        // TODO: There appears to be a limit on integers in MarkLogic, find out what it is
        //       and validate `i`.
        case ejs.Int(i)            => xs.integer(i.toString.xqy).point[M]
      }
    }

  implicit def dataEncodeXQuery[M[_]: PrologW: MonadErrMsgs_]: EncodeXQuery[M, Const[Data, ?]] =
    new EncodeXQuery[M, Const[Data, ?]] {
      val encodeXQuery: AlgebraM[M, Const[Data, ?], XQuery] = _.getConst match {
        case Data.Binary(bytes) => xs.base64Binary(base64Bytes(bytes).xs).point[M]
        case Data.Bool(b)       => b.fold(fn.True, fn.False).point[M]
        case Data.Date(d)       => xs.date(isoLocalDate(d).xs).point[M]
        case Data.Dec(d)        => xs.double(d.toString.xqy).point[M]
        case Data.Id(id)        => id.xs.point[M]
        case Data.Int(i)        => xs.integer(i.toString.xqy).point[M]
        case Data.Interval(d)   => xs.duration(s"PT${durationInSeconds(d)}S".xs).point[M]
        case Data.NA            => expr.emptySeq.point[M]
        case Data.Null          => ejson.null_[M]
        case Data.Str(s)        => s.xs.point[M]
        case Data.Time(t)       => xs.time(isoLocalTime(t).xs).point[M]
        case Data.Timestamp(ts) => xs.dateTime(isoInstant(ts).xs).point[M]

        case Data.Arr(elements) =>
          elements.traverse (d =>
            encodeXQuery(Const(d)) flatMap ejson.mkArrayElt[M]
          ) flatMap (xs => ejson.mkArray_[M](mkSeq(xs)))

        case Data.Obj(entries)  =>
          objAsXQuery(entries.toList)(some, d => encodeXQuery(Const(d)))

        case Data.Set(elements) =>
          elements.traverse(d => encodeXQuery(Const(d))) map (mkSeq(_))
      }
    }

  ////

  private def objAsXQuery[F[_]: Traverse, M[_]: PrologW: MonadErrMsgs_, A: Show, B](
    entries: F[(A, B)]
  )(
    f: A => Option[String],
    g: B => M[XQuery]
  ): M[XQuery] = {
    type ValM[A] = Validation[ErrorMessages, M[A]]

    implicit val valMApplicative: Applicative[ValM] =
      Applicative[Validation[ErrorMessages, ?]].compose[M]

    def keyXqy(a: A): Validation[ErrorMessages, XQuery] =
      (f(a) \/> s"'${a.shows}' is not a supported map key in XQuery.")
        .flatMap(s => refineV[IsNCName](s).disjunction leftAs s"'$s' is not a valid XML QName.")
        .map(ncname => xs.QName(ncname.get.xs))
        .validationNel

    entries.traverse[ValM, XQuery] { case (key, value) =>
      keyXqy(key) map (k => g(value) flatMap (ejson.renameOrWrap[M] apply (k, _)))
    } valueOr (MonadErrMsgs_[M].raiseError(_)) flatMap (ejson.mkObject[M] apply mkSeq(_))
  }
}
