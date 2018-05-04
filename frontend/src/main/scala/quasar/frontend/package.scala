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

package quasar

import slamdata.Predef.{Option, String, Vector}
import quasar.common.{PhaseResult, PhaseResultW}

import scalaz._
import scalaz.std.vector._
import scalaz.syntax.monad._
import scalaz.syntax.writer._

package object frontend {
  type CompileM[A] = SemanticErrsT[PhaseResultW, A]
  type SemanticErrors = NonEmptyList[SemanticError]
  type SemanticErrsT[F[_], A] = EitherT[F, SemanticErrors, A]

  type CIName = CIString

  object CIName {
    def apply(value: String): CIName =
      CIString(value)

    def unapply(name: CIName): Option[String] =
      CIString.unapply(name)
  }

  def compilePhase[A: RenderTree](label: String, r: SemanticErrors \/ A): CompileM[A] =
    EitherT(r.point[PhaseResultW]) flatMap { a =>
      (a.set(Vector(PhaseResult.tree(label, a)))).liftM[SemanticErrsT]
    }
}
