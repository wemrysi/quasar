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

package quasar.run

import slamdata.Predef.{Product, Serializable}
import quasar.compile.SemanticErrors
import quasar.fs.Planner.PlannerError
import quasar.sql.ParsingError

import monocle.Prism

sealed abstract trait QueryError extends Product with Serializable

object QueryError {
  final case class Compiling(errors: SemanticErrors) extends QueryError
  final case class Parsing(error: ParsingError) extends QueryError
  final case class Planning(error: PlannerError) extends QueryError

  val compiling: Prism[QueryError, SemanticErrors] =
    Prism.partial[QueryError, SemanticErrors] {
      case Compiling(errs) => errs
    } (Compiling(_))

  val parsing: Prism[QueryError, ParsingError] =
    Prism.partial[QueryError, ParsingError] {
      case Parsing(err) => err
    } (Parsing(_))

  val planning: Prism[QueryError, PlannerError] =
    Prism.partial[QueryError, PlannerError] {
      case Planning(err) => err
    } (Planning(_))
}
