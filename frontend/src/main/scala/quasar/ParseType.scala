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

import slamdata.Predef.{Product, Serializable}

import scalaz.{Equal, Show}

sealed abstract class ParseType extends Product with Serializable

sealed abstract class ScalarParseType extends ParseType
sealed abstract class CompositeParseType extends ParseType

object ParseType {
  final case object Boolean extends ScalarParseType
  final case object Null extends ScalarParseType
  final case object Number extends ScalarParseType
  final case object String extends ScalarParseType

  final case object Array extends CompositeParseType
  final case object Object extends CompositeParseType

  implicit val parseTypeShow: Show[ParseType] = Show.showFromToString
  implicit val parseTypeEqual: Equal[ParseType] = Equal.equalA
}
