/*
 * Copyright 2014–2017 SlamData Inc.
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

package quasar.yggdrasil

import argonaut.{Argonaut, CodecJson, DecodeResult}

import scalaz.{~>, :<:, Coproduct, Free}
import scalaz.concurrent.Task

import java.util.UUID

package object vfs {
  type POSIX[A] = Free[POSIXOp, A]
  type POSIXWithTask[A] = Free[Coproduct[POSIXOp, Task, ?], A]

  object POSIXWithTask {
    def generalize[S[_]]: GeneralizeSyntax[S] = new GeneralizeSyntax[S] {}


    trait GeneralizeSyntax[S[_]] {
      def apply[A](pwt: POSIXWithTask[A])(implicit IP: POSIXOp :<: S, IT: Task :<: S): Free[S, A] =
        pwt.mapSuspension(λ[Coproduct[POSIXOp, Task, ?] ~> S](_.run.fold(IP.inj, IT.inj)))
    }
  }

  final case class Version(value: UUID) extends AnyVal

  object Version extends (UUID => Version) {
    import Argonaut._

    implicit val codec: CodecJson[Version] =
      CodecJson[Version](v => jString(v.value.toString), { c =>
        c.as[String] flatMap { str =>
          try {
            DecodeResult.ok(Version(UUID.fromString(str)))
          } catch {
            case _: IllegalArgumentException =>
              DecodeResult.fail(s"string '${str}' is not a valid UUID", c.history)
          }
        }
      })
  }
}
