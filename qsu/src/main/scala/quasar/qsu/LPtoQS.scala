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

package quasar.qsu

import slamdata.Predef._

import quasar.{RenderTreeT, RenderTree}, RenderTree.ops._
import quasar.common.{PhaseResult, PhaseResultTell}
import quasar.common.effect.NameGenerator
import quasar.frontend.logicalplan.LogicalPlan
import quasar.qscript.MonadPlannerErr

import matryoshka.{BirecursiveT, EqualT, ShowT}

import org.slf4s.Logging

import cats.Eval

import scalaz.{Cord, Equal, Functor, Kleisli => K, Monad, Show}
import scalaz.syntax.functor._
import scalaz.syntax.show._

sealed abstract class LPtoQS[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT]
    extends QSUTTypes[T]
    with Logging {

  import LPtoQS.MapSyntax

  val qprov: QProv[T]
  type P = qprov.P

  implicit def PEqual: Equal[P]
  implicit def PShow: Show[P]

  def apply[F[_]: Monad: MonadPlannerErr: PhaseResultTell: NameGenerator](lp: T[LogicalPlan])
      : F[T[QScriptEducated]] = {

    val agraph =
      ApplyProvenance.AuthenticatedQSU.graph[T, P]

    val lpToQs =
      K(ReadLP[T, F])                        >==>
      debug("ReadLP")                        >==>
      RewriteGroupByArrays[T, F]             >==>
      debug("RewriteGroupByArrays")          >-
      EliminateUnary[T]                      >==>
      debug("EliminateUnary")                >-
      InlineNullary[T]                       >==>
      debug("InlineNullary")                 >-
      CoalesceUnaryMappable[T]               >==>
      debug("CoalesceUnaryMappable")         >-
      RecognizeDistinct[T]                   >==>
      debug("RecognizeDistinct")             >==>
      ExtractFreeMap[T, F]                   >==>
      debug("ExtractFreeMap")                >==>
      PruneSymmetricDimEdits[T, F]           >==>
      debug("PruneSymmetricDimEdits")        >==>
      (ApplyProvenance[T, F](qprov, _))      >==>
      debug("ApplyProvenance")               >==>
      ReifyBuckets[T, F](qprov)              >==>
      debug("ReifyBuckets")                  >==>
      MinimizeAutoJoins[T, F](qprov)         >==>
      debug("MinimizeAutoJoins")             >==>
      ReifyAutoJoins[T, F](qprov)            >==>
      debug("ReifyAutoJoins")                >==>
      ExpandShifts[T, F](qprov)              >==>
      debug("ExpandShifts")                  >-
      agraph.modify(ResolveOwnIdentities[T]) >==>
      debug("ResolveOwnIdentities")          >==>
      ReifyIdentities[T, F]                  >==>
      debug("ReifyIdentities")               >==>
      Graduate[T, F]

    log.debug("LogicalPlan\n" + lp.render.shows)

    lpToQs(lp)
  }

  private def debug[F[_]: Functor: PhaseResultTell, A: Show](name: String): A => F[A] = { a =>
    log.debug((Cord(name + "\n") ++ a.show).shows)
    PhaseResultTell[F].tell(Vector(Eval.later(PhaseResult.detail(s"QSU ($name)", a.shows)))).as(a)
  }
}

object LPtoQS {
  def apply[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT](
      qp: QProv[T])(
      implicit eqP: Equal[qp.P], showP: Show[qp.P])
      : LPtoQS[T] =
    new LPtoQS[T] {
      val qprov: qp.type = qp
      val PEqual = eqP
      val PShow = showP
    }

  final implicit class MapSyntax[F[_], A](val self: F[A]) extends AnyVal {
    def >-[B](f: A => B)(implicit F: Functor[F]): F[B] =
      F.map(self)(f)
  }
}
