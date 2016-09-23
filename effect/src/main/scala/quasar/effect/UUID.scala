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

package quasar.effect

import java.util.{UUID => JUUID}

import scalaz._
import scalaz.concurrent.Task

sealed trait UUID[A]

object UUID {
  case object RandomUUID extends UUID[JUUID]

  final class Ops[S[_]](implicit S: UUID :<: S) extends LiftedOps[UUID, S] {
    def randomUUID: F[JUUID] = lift(RandomUUID)
  }

  object Ops {
    implicit def apply[S[_]](implicit S: UUID :<: S): Ops[S] = new Ops[S]
  }

  val toTask: UUID ~> Task = λ[UUID ~> Task] {
    case RandomUUID => Task.delay(JUUID.randomUUID)
  }

}
