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

package quasar.api.table

import slamdata.Predef.{Product, Serializable}

sealed trait TableError extends Product with Serializable

object TableError {
  sealed trait CreationError extends TableError
  final case class NameConflict(name: TableName) extends CreationError

  sealed trait PrePreparationError[I] extends TableError
  final case class PreparationInProgress[I](tableId: I) extends PrePreparationError[I]

  sealed trait ModificationError[I] extends TableError
  final case class ConflictingPreparationState[I](tableId: I) extends ModificationError[I]

  sealed trait ExistenceError[I] extends ModificationError[I] with PrePreparationError[I]
  final case class TableNotFound[I](tableId: I) extends ExistenceError[I]
}
