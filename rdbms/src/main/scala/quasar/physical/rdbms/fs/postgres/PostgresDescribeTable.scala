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

package quasar.physical.rdbms.fs.postgres

import quasar.physical.rdbms.common._
import quasar.physical.rdbms.fs.RdbmsDescribeTable
import slamdata.Predef._

import doobie.imports._

trait PostgresDescribeTable extends RdbmsDescribeTable {

  private def descQuery[F[_], T](
      whereClause: Fragment,
      mapResult: List[(String, String)] => T): ConnectionIO[T] = {
    (fr"SELECT COLUMN_NAME, DATA_TYPE FROM information_schema.COLUMNS"
      ++ whereClause)
      .query[(String, String)]
      .list
      .map(mapResult)
  }

  private def whereSchemaAndTable(tablePath: TablePath): Fragment = {
    val schemaName = tablePath.schema.map(_.name).getOrElse("public")
    fr"WHERE TABLE_SCHEMA=" ++
  Fragment.const("'" + schemaName + "'") ++
    fr"AND TABLE_NAME =" ++
    Fragment.const("'" + tablePath.table.name + "'")
  }

  private def whereSchema(schemaName: SchemaName): Fragment =
    fr"WHERE TABLE_SCHEMA=" ++
      Fragment.const("'" + schemaName.name + "'")


  def findChildSchemas(parentSchema: SchemaName): ConnectionIO[Vector[SchemaName]] = {
    val prefixParam = parentSchema.name + TablePath.Separator
    sql"SELECT SCHEMA_NAME FROM information_schema.schemata WHERE SCHEMA_NAME LIKE $prefixParam"
      .query[String]
      .vector
      .map(_.map(SchemaName.apply))
  }

  override def schemaExists(schemaName: SchemaName): ConnectionIO[Boolean] =
    descQuery(whereSchema(schemaName), _.nonEmpty)

  override def tableExists(
      tablePath: TablePath): ConnectionIO[Boolean] =
    descQuery(whereSchemaAndTable(tablePath), _.nonEmpty)

}
