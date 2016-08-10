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
import quasar.SKI.κ

import com.marklogic.xcc.ResultItem
import com.marklogic.xcc.exceptions._
import monocle.Prism
import scalaz.{Equal, Show}

sealed abstract class XccError

object XccError {
  final case class RequestError(cause: RequestException) extends XccError
  case object SessionIsClosed extends XccError
  final case class StreamingError(ritem: ResultItem, cause: StreamingResultException) extends XccError

  val requestError: Prism[XccError, RequestException] =
    Prism.partial[XccError, RequestException] {
      case RequestError(c) => c
    } (RequestError)

  val sessionIsClosed: Prism[XccError, Unit] =
    Prism.partial[XccError, Unit] {
      case SessionIsClosed => ()
    } (κ(SessionIsClosed))

  val streamingError: Prism[XccError, (ResultItem, StreamingResultException)] =
    Prism.partial[XccError, (ResultItem, StreamingResultException)] {
      case StreamingError(r, c) => (r, c)
    } ((StreamingError(_, _)).tupled)

  implicit val equal: Equal[XccError] =
    Equal.equalA

  implicit val show: Show[XccError] =
    Show.shows {
      case RequestError(ex)      => s"RequestError: ${ex.getMessage}"
      case SessionIsClosed       => "Session is closed."
      case StreamingError(r, ex) => s"Failed to stream request result (${r.getItemType}): ${ex.getMessage}"
    }
}
