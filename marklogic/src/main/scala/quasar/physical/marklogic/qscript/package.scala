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

package quasar.physical.marklogic

import quasar.Predef._
import quasar.ejson.{Common, Str}
import quasar.fp.{coproductShow, QuasarFreeOps}
import quasar.fp.ski.κ
import quasar.contrib.matryoshka.{freeCataM, interpretM}
import quasar.contrib.scalaz.MonadError_
import quasar.physical.marklogic.validation._
import quasar.physical.marklogic.xml._
import quasar.physical.marklogic.xquery._
import quasar.physical.marklogic.xquery.syntax._
import quasar.qscript._

import eu.timepit.refined.refineV
import matryoshka.{Corecursive, Embed, Fix, Recursive}, Recursive.ops._
import matryoshka.patterns.CoEnv
import scalaz._, Scalaz._

package object qscript {
  type MarkLogicPlanErrT[F[_], A] = EitherT[F, MarkLogicPlannerError, A]

  type MonadPlanErr[F[_]] = MonadError_[F, MarkLogicPlannerError]

  object MonadPlanErr {
    def apply[F[_]](implicit F: MonadPlanErr[F]): MonadPlanErr[F] = F
  }

  /** Matches "iterative" FLWOR expressions, those involving at least one `for` clause. */
  object IterativeFlwor {
    def unapply(xqy: XQuery): Option[(NonEmptyList[BindingClause], Option[XQuery], IList[(XQuery, SortDirection)], Boolean, XQuery)] = xqy match {
      case XQuery.Flwor(clauses, filter, order, isStable, result) if clauses.any(BindingClause.forClause.isMatching) =>
        Some((clauses, filter, order, isStable, result))

      case _ => None
    }
  }

  /** Converts the given string to a QName if valid, failing with an error otherwise. */
  def asQName[F[_]: MonadPlanErr: Applicative](s: String): F[QName] = {
    def asNCName(str: String): Option[NCName] =
      refineV[IsNCName](str).right.toOption map (NCName(_))

    (s.split(':') match {
      case Array(pfx, loc) => (asNCName(pfx) |@| asNCName(loc))((p, l) => QName.prefixed(NSPrefix(p), l))
      case Array(loc)      => asNCName(encodeForQName(loc)) map (QName.local)
      case _               => None
    }).fold(invalidQName[F, QName](s))(_.point[F])
  }

  def mapFuncXQuery[T[_[_]]: Recursive: Corecursive, F[_]: Monad: MonadPlanErr, FMT](
    fm: FreeMap[T],
    src: XQuery
  )(implicit
    MFP: Planner[F, FMT, MapFunc[T, ?]],
    SP:  StructuralPlanner[F, FMT]
  ): F[XQuery] =
    fm.toCoEnv[T].project match {
      case MapFunc.StaticArray(elements) =>
        for {
          xqyElts <- elements.traverse(mapFuncXQueryP[T, F, FMT](_, src))
          arrElts <- xqyElts.traverse(SP.mkArrayElt)
          arr     <- SP.mkArray(mkSeq(arrElts))
        } yield arr

      case MapFunc.StaticMap(entries) =>
        for {
          xqyKV <- entries.traverse(_.bitraverse({
                     case Embed(Common(Str(s))) => s.xs.point[F]
                     case key                   => invalidQName[F, XQuery](key.convertTo[Fix].shows)
                   },
                   mapFuncXQueryP[T, F, FMT](_, src)))
          elts  <- xqyKV.traverse((SP.mkObjectEntry _).tupled)
          map   <- SP.mkObject(mkSeq(elts))
        } yield map

      case other => mapFuncXQueryP[T, F, FMT](other.embed, src)
    }

  def mapFuncXQueryP[T[_[_]]: Recursive: Corecursive, F[_]: Monad, FMT](
    fm: T[CoEnv[Hole, MapFunc[T, ?], ?]],
    src: XQuery
  )(implicit
    MFP: Planner[F, FMT, MapFunc[T, ?]]
  ): F[XQuery] =
    planMapFuncP[T, F, FMT, Hole](fm)(κ(src))

  def mergeXQuery[T[_[_]]: Recursive: Corecursive, F[_]: Monad, FMT](
    jf: JoinFunc[T],
    l: XQuery,
    r: XQuery
  )(implicit
    MFP: Planner[F, FMT, MapFunc[T, ?]]
  ): F[XQuery] =
    planMapFunc[T, F, FMT, JoinSide](jf) {
      case LeftSide  => l
      case RightSide => r
    }

  def planMapFunc[T[_[_]]: Recursive: Corecursive, F[_]: Monad, FMT, A](
    freeMap: FreeMapA[T, A])(
    recover: A => XQuery
  )(implicit
    MFP: Planner[F, FMT, MapFunc[T, ?]]
  ): F[XQuery] =
    planMapFuncP[T, F, FMT, A](freeMap.toCoEnv)(recover)

  def planMapFuncP[T[_[_]]: Recursive, F[_]: Monad, FMT, A](
    freeMap: T[CoEnv[A, MapFunc[T, ?], ?]])(
    recover: A => XQuery
  )(implicit
    MFP: Planner[F, FMT, MapFunc[T, ?]]
  ): F[XQuery] =
    freeMap.cataM(interpretM(recover(_).point[F], MFP.plan))

  def rebaseXQuery[T[_[_]], F[_]: Monad, FMT](
    fqs: FreeQS[T],
    src: XQuery
  )(implicit
    QTP: Planner[F, FMT, QScriptTotal[T, ?]]
  ): F[XQuery] =
    freeCataM(fqs)(interpretM(κ(src.point[F]), QTP.plan))

  ////

  // A string consisting only of digits.
  private val IntegralNumber = "^(\\d+)$".r

  // Applies transformations to string to make them valid QNames
  private val encodeForQName: String => String = {
    case IntegralNumber(n) => "_" + n
    case other             => other
  }

  private implicit def comfTraverse[T[_[_]], A]: Traverse[CoEnv[A, MapFunc[T, ?], ?]] =
    Bitraverse[CoEnv[?, MapFunc[T, ?], ?]].rightTraverse[A]

  private def invalidQName[F[_]: MonadPlanErr, A](s: String): F[A] =
    MonadError_[F, MarkLogicPlannerError].raiseError(
      MarkLogicPlannerError.invalidQName(s))
}
