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

package quasar.api

import slamdata.Predef.{Option, String, Throwable, Unit}
import quasar.fp.ski.κ

import monocle.Prism
import scalaz.{Cord, Show}
import scalaz.std.option._
import scalaz.std.string._
import scalaz.syntax.show._

sealed trait ExternalStatus

object ExternalStatus extends ExternalStatusInstances {
  final case class Error(message: String, cause: Option[Throwable]) extends ExternalStatus
  case object Ok extends ExternalStatus

  val error: Prism[ExternalStatus, (String, Option[Throwable])] =
    Prism.partial[ExternalStatus, (String, Option[Throwable])] {
      case Error(m, c) => (m, c)
    } ((Error.apply _).tupled)

  val ok: Prism[ExternalStatus, Unit] =
    Prism.partial[ExternalStatus, Unit] {
      case Ok => ()
    } (κ(Ok))
}

sealed abstract class ExternalStatusInstances {
  implicit val show: Show[ExternalStatus] =
    Show.show {
      case ExternalStatus.Ok =>
        Cord("Ok")

      case ExternalStatus.Error(m, c) =>
        Cord("Error(") ++ m.show ++ ", " ++ c.map(_.getMessage).show ++ Cord(")")
    }
}
