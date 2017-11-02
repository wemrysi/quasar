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

package quasar.physical.rdbms.model

import slamdata.Predef._
import quasar.Data
import quasar.fs.FileSystemError
import quasar.fs.FileSystemError._

import scalaz._
import Scalaz._

sealed trait ColumnType
case object JsonCol extends ColumnType
case object StringCol extends ColumnType
case object IntCol extends ColumnType
case object NullCol extends ColumnType

final case class ColumnDesc(name: String, tpe: ColumnType)

sealed trait SchemaDesc

object SchemaDesc {

  implicit val columnDescOrder: Order[ColumnDesc] = new Order[ColumnDesc] {
    override def order(x: ColumnDesc, y: ColumnDesc): Ordering = {
      scalaz.std.string.stringInstance.order(x.name, y.name)
    }
  }

  implicit val schemaDescMonoid: Monoid[SchemaDesc] = new Monoid[SchemaDesc] {
    override def zero: SchemaDesc = ColumnarSchemaDesc(ISet.empty)
    def empty: SchemaDesc = zero

    override def append(s1: SchemaDesc, s2: => SchemaDesc): SchemaDesc = {
      s1 match {
        case JsonSchemaDesc => JsonSchemaDesc
        case ColumnarSchemaDesc(cols) if cols.isEmpty => s2
        case ColumnarSchemaDesc(cols) =>
          s2 match {
            case JsonSchemaDesc => JsonSchemaDesc
            case ColumnarSchemaDesc(newCols) if newCols.isEmpty => s1
            case ColumnarSchemaDesc(newCols) =>
             empty // TODO replace with proper main algorithm
          }
      }
    }
  }
}

case object JsonSchemaDesc extends SchemaDesc

final case class ColumnarSchemaDesc(columns: ISet[ColumnDesc]) extends SchemaDesc

/**
  * Determines what DB schema should be created depending on input Data vector.
  */
class SchemaDeviser {

  import SchemaDesc._

  def simpleColumnType(data: Data): ColumnType = {
    data match {
      case Data.Null => NullCol
      case Data.Str(_) => StringCol
      case Data.Int(_) => IntCol
      case _ => NullCol // TODO support all types
    }
  }

  def columnType(data: Data): ColumnType = {
    data match {
      case Data.Obj(_) => JsonCol
      case Data.Arr(_) => JsonCol
      case _ => simpleColumnType(data)
    }
  }

  def rowSchema(row: Data): SchemaDesc = {
    row match {
      case Data.Obj(fields) =>
        ColumnarSchemaDesc(ISet.fromList(fields.map {
          case (label, value) => ColumnDesc(label, columnType(value))
        }.toList))
      case _ => JsonSchemaDesc
    }
  }

  def schema(ds: Vector[Data]): FileSystemError \/ SchemaDesc = {
    ds match {
      case _ :+ _ => ds.foldMap(rowSchema).right
      case _ => unsupportedOperation("Cannot map empty data row to a RDBMS schema.").left
    }
  }
}