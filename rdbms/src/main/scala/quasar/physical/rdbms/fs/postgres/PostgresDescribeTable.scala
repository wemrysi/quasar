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

import quasar.physical.rdbms.common.{Schema, _}
import quasar.physical.rdbms.fs.RdbmsDescribeTable
import slamdata.Predef._
import doobie.imports._
import quasar.physical.rdbms.common.TablePath.Separator

import scalaz.syntax.show._
import scalaz.syntax.applicative._

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
    val schemaName = tablePath.schema match {
      case DefaultSchema => "public"
      case CustomSchema(name) => name
    }
    fr"WHERE TABLE_SCHEMA=" ++
  Fragment.const("'" + schemaName + "'") ++
    fr"AND TABLE_NAME =" ++
    Fragment.const("'" + tablePath.table.shows + "'")
  }

  private def whereSchema(schemaName: String): Fragment =
    fr"WHERE TABLE_SCHEMA=" ++
      Fragment.const("'" + schemaName + "'")


  override def findChildTables(schema: Schema): ConnectionIO[Vector[TableName]] = {
    val whereClause = schema match {
      case DefaultSchema => fr""
      case CustomSchema(name) => fr"WHERE TABLE_SCHEMA = $name"
    }

    (fr"select TABLE_NAME from information_schema.tables" ++ whereClause)
      .query[String]
      .vector
      .map(_.map(TableName.apply))
  }

  override def findChildSchemas(parent: Schema): ConnectionIO[Vector[CustomSchema]] = {
    val whereClause = parent match {
      case DefaultSchema => fr""
      case CustomSchema(name) =>
        fr"WHERE SCHEMA_NAME LIKE" ++ Fragment.const("'" + name + Separator + "%'")
    }
    (fr"SELECT SCHEMA_NAME FROM information_schema.schemata" ++ whereClause)
      .query[String]
      .vector
      .map(_.map(CustomSchema.apply))
  }

  override def schemaExists(schema: Schema): ConnectionIO[Boolean] =
    schema match {
      case DefaultSchema => true.point[ConnectionIO]
      case CustomSchema(name) =>
        descQuery(whereSchema(name), _.nonEmpty)
    }

  override def tableExists(
      tablePath: TablePath): ConnectionIO[Boolean] =
    descQuery(whereSchemaAndTable(tablePath), _.nonEmpty)

}
