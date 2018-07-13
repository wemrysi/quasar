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

package quasar.api.datasource

import slamdata.Predef._
import quasar.api.ResourceName

import scalaz.{Cord, Equal, ISet, NonEmptyList, Show}
import scalaz.std.option._
import scalaz.std.string._
import scalaz.std.tuple._
import scalaz.syntax.show._

sealed trait DatasourceError[+C] extends Product with Serializable

object DatasourceError extends DatasourceErrorInstances {
  sealed trait CreateError[+C] extends DatasourceError[C]

  final case class DatasourceUnsupported(kind: DatasourceType, supported: ISet[DatasourceType])
      extends CreateError[Nothing]

  sealed trait InitializationError[C] extends CreateError[C]

  final case class MalformedConfiguration[C](kind: DatasourceType, config: C, reason: String)
      extends InitializationError[C]

  final case class InvalidConfiguration[C](kind: DatasourceType, config: C, reasons: NonEmptyList[String])
      extends InitializationError[C]

  final case class UnprocessableEntity[C](kind: DatasourceType, config: C, reason: String)
    extends InitializationError[C]

  final case class ConnectionFailed[C](kind: DatasourceType, config: C, cause: Exception)
    extends InitializationError[C]

  sealed trait ExistentialError extends CreateError[Nothing]

  final case class DatasourceExists(name: ResourceName)
      extends ExistentialError

  sealed trait CommonError extends ExistentialError

  final case class DatasourceNotFound(name: ResourceName)
      extends CommonError
}

sealed abstract class DatasourceErrorInstances {
  import DatasourceError._

  implicit def equal[C: Equal]: Equal[DatasourceError[C]] =
    Equal.equalBy {
      case DatasourceExists(n) =>
        (some(n), none, none, none, none, none, none)

      case DatasourceNotFound(n) =>
        (none, some(n), none, none, none, none, none)

      case DatasourceUnsupported(k, s) =>
        (none, none, some((k, s)), none, none, none, none)

      case MalformedConfiguration(k, c, r) =>
        (none, none, none, some((k, c, r)), none, none, none)

      case UnprocessableEntity(k, c, r) =>
        (none, none, none, none, some((k, c, r)), none, none)

      case ConnectionFailed(k, c, _) =>
        (none, none, none, none, none, some((k, c)), none)

      case InvalidConfiguration(k, c, rs) =>
        (none, none, none, none, none, none, some((k, c, rs)))
    }

  implicit def show[C: Show]: Show[DatasourceError[C]] =
    Show.show {
      case DatasourceExists(n) =>
        Cord("DatasourceExists(") ++ n.show ++ Cord(")")

      case DatasourceNotFound(n) =>
        Cord("DatasourceNotFound(") ++ n.show ++ Cord(")")

      case DatasourceUnsupported(k, s) =>
        Cord("DatasourceUnsupported(") ++ k.show ++ Cord(", ") ++ s.show ++ Cord(")")

      case MalformedConfiguration(k, c, r) =>
        Cord("MalformedConfiguration(") ++ k.show ++ Cord(", ") ++ c.show ++ Cord(", ") ++ r.show ++ Cord(")")

      case UnprocessableEntity(k, c, r) =>
        Cord("UnprocessableEntity(") ++ k.show ++ Cord(", ") ++ c.show ++ Cord(", ") ++ r.show ++ Cord(")")

      case ConnectionFailed(k, c, e) =>
        Cord("ConnectionFailed(") ++ k.show ++ Cord(", ") ++ c.show ++ Cord(s")\n\n$e")

      case InvalidConfiguration(k, c, rs) =>
        Cord("InvalidConfiguration(") ++ k.show ++ Cord(", ") ++ c.show ++ Cord(", ") ++ rs.show ++ Cord(")")
    }
}
