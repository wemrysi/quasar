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

import slamdata.Predef._

import quasar.common.CPath

sealed abstract class ParseInstruction extends Product with Serializable

object ParseInstruction {

  /* Generates one unique identity per row. Creates a top-level object with
   * `idName` providing the unique identity and `valueName` providing the
   * original value.
   */
  final case class Ids(idName: String, valueName: String) extends ParseInstruction
  
  /* Wraps the provided `path` into an object with key `name`, thus adding
   * another layer of structure. All other paths are retained.
   */
  final case class Wrap(path: CPath, name: String) extends ParseInstruction
  
  /* Removes all values that are not both at the path `path` and of the type `tpe`.
   *
   * A `Mask` is not a `ParseInstruction` and must be constructed with `Masks`.
   */
  final case class Mask(path: CPath, tpe: ParseType)

  /* `Masks` represents the disjunction of the provided `masks`. An empty set indicates
   * that all values should be dropped.
   */
  final case class Masks(masks: Set[Mask]) extends ParseInstruction
  
  /* Pivots the indices and keys out of arrays and objects, respectively,
   * according to the `structure`, maintaining their association with the original
   * corresponding value.
   *
   * `idStatus` determines how the values is returned:
   *   - `IncludeId` wraps the key/value pair in a two element array.
   *   - `IdOnly` returns the value unwrapped.
   *   - `ExcludeId` returns the value unwrapped.
   *
   * We plan to add a boolean `retain` parameter. Currently, `retain` implicitly
   * defaults to `false`. `retain` will indicate two things:
   *   1) In the case of a successful pivot, if surrounding structure should be retained.
   *   2) In the case of an unsuccessful pivot (`path` does not reference a value of
   *      the provided `structure`), if the row should be returned.
   */
  final case class Pivot(path: CPath, idStatus: IdStatus, structure: CompositeParseType)
      extends ParseInstruction

  /* The instructions that must be interpreted sequentially while parsing. These
   * instructions are not optional and will result in incorrect query results if ignored.
   */
  final case class ParseInstructions(instructions: List[ParseInstruction])
}
