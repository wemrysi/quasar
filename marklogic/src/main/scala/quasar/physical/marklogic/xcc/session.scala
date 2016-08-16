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

package quasar.physical.marklogic.xcc

import quasar.Predef._
import quasar.fp.free.lift
import quasar.physical.marklogic.xquery.XQuery

import java.lang.IllegalStateException

import com.marklogic.xcc.{Session => XccSession, _}
import com.marklogic.xcc.exceptions.RequestException
import scalaz._, Scalaz._
import scalaz.concurrent.Task

object session {
  final class Ops[S[_]](implicit S0: SessionR :<: S) {
    import XccError._, Executed._

    type F[A] = Free[S, A]

    def close(implicit S1: Task :<: S): F[Executed] =
      captured(_.close).flatMap(lift(_).into[S]).as(executed)

    def defaultEvaluateQuery(query: XQuery)(implicit S1: Task :<: S, S2: XccFailure :<: S): F[ResultSequence] =
      evaluateQuery(query, new RequestOptions())

    def evaluateQuery(query: XQuery, options: RequestOptions)(implicit S1: Task :<: S, S2: XccFailure :<: S): F[ResultSequence] =
      liftT(s => s.submitRequest(s.newAdhocQuery(query, options)))

    def session: F[XccSession] =
      SessionR.Ops[S].ask

    ////

    private def liftT[A](f: XccSession => A)(implicit S1: Task :<: S, S2: XccFailure :<: S): F[A] =
      session flatMap { s =>
        val handled = Task.delay(f(s).right[XccError]) handleWith {
          case ise: IllegalStateException => sessionIsClosed().left[A].point[Task]
          case rex: RequestException      => requestError(rex).left[A].point[Task]
        }

        XccFailure.Ops[S].unattempt(lift(handled).into[S])
      }

    private def captured[A](f: XccSession => A): F[Task[A]] =
      session.map(s => Task.delay(f(s)))
  }

  object Ops {
    def apply[S[_]](implicit S0: SessionR :<: S) =
      new Ops[S]
  }
}

