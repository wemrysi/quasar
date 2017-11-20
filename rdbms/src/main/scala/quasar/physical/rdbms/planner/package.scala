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

package quasar.physical.rdbms

import slamdata.Predef._
import quasar.Planner.{InternalError, PlannerErrorME}

package object planner {

  def unexpected[F[_]: PlannerErrorME, A](name: String, src: AnyRef): F[A] =
    PlannerErrorME[F].raiseError(InternalError.fromMsg(s"unexpected $name in planner $src"))

  def notImplemented[F[_]: PlannerErrorME, A](name: String, src: AnyRef): F[A] =
    PlannerErrorME[F].raiseError(InternalError.fromMsg(s"unimplemented $name in planner $src"))

}
