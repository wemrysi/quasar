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

import quasar.Predef.{Option, Some, None}
import quasar.SKI._

import com.marklogic.xcc.{ContentSource, RequestOptions, Session}
import scalaz.~>
import scalaz.concurrent.Task

package object xcc {

  def runSessionIO(
    contentSource: ContentSource,
    defaultRequestOptions: RequestOptions
  ): SessionIO ~> Task =
    runSessionIO0(contentSource, Some(defaultRequestOptions))

  def runSessionIO_(contentSource: ContentSource): SessionIO ~> Task =
    runSessionIO0(contentSource, None)

  ////

  private def runSessionIO0(
    contentSource: ContentSource,
    defaultRequestOptions: Option[RequestOptions]
  ): SessionIO ~> Task =
    new (SessionIO ~> Task) {
      def apply[A](sio: SessionIO[A]) =
        newSession flatMap { session =>
          sio.run(session).onFinish(κ(Task.delay(session.close)))
        }

      def newSession: Task[Session] = Task.delay {
        val session = contentSource.newSession
        defaultRequestOptions foreach session.setDefaultRequestOptions
        session
      }
    }
}
