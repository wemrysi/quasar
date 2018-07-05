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

import quasar.Condition

import scalaz.{\/, NonEmptyList}

/** @tparam I identity
  * @tparam D materialized table data
  * @tparam S table schema
  */
trait Tables[F[_], G[_], I, D, S] {
  def allTables: F[G[(I, Table)]]

  // errors: unparsable query, resource(s) not found, name conflict
  def createTable(table: Table): F[TableError \/ I]

  // errors: table not found
  def table(tableId: I): F[TableError \/ Table]

  // errors: table not found, conflicting preparation state, unparsable query, resource(s) not found, name conflict
  def setTableAttributes(tableId: I, attributes: NonEmptyList[TableAttribute]): F[Condition[TableError]]

  // errors: table not found, preparation in progress
  def prepareTable(tableId: I): F[Condition[TableError]]

  // errors: table not found
  def preparationStatus(tableId: I): F[TableError \/ PreparationStatus]

  // errors: table not found
  def preparedData(tableId: I): F[TableError \/ PreparationResult[D]]

  // errors: table not found
  def preparedSchema(tableId: I): F[TableError \/ PreparationResult[S]]
}
