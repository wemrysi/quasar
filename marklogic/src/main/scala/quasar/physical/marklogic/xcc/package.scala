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

import quasar.SKI._
import quasar.effect.{Failure, Read}

import com.marklogic.xcc.{ContentSource, Session}
import scalaz.{:<:, ~>}
import scalaz.concurrent.Task

package object xcc {
  type SessionR[A] = Read[Session, A]

  object SessionR {
    def Ops[S[_]](implicit S: SessionR :<: S) =
      Read.Ops[Session, S]
  }

  type XccFailure[A] = Failure[XccError, A]

  object XccFailure {
    def Ops[S[_]](implicit S: XccFailure :<: S) =
      Failure.Ops[XccError, S]
  }

  def runSessionIO(contentSource: ContentSource): SessionIO ~> Task =
    new (SessionIO ~> Task) {
      def apply[A](sio: SessionIO[A]) =
        Task.delay(contentSource.newSession) flatMap { session =>
          sio.run(session).onFinish(κ(Task.delay(session.close)))
        }
    }
}
