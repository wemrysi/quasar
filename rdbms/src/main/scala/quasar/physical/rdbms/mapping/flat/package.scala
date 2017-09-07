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

package quasar.physical.rdbms.mapping

import java.sql.{PreparedStatement, ResultSet}

import doobie.enum.jdbctype
import doobie.enum.jdbctype.JdbcType
import doobie.util.composite.Composite
import doobie.util.kernel.Kernel
import quasar.Data
import slamdata.Predef._

package object flat {

  implicit val FlatDataComposite: Composite[Data] =  new Composite[Data] {
    val kernel = new Kernel[Data] {
      type I      = Data
      val ia      = (i: I) => i
      val ai      = (a: I) => a
      val get     = (rs: ResultSet, _: Int) => {
        val rsMeta = rs.getMetaData
        val cc = rsMeta.getColumnCount

        val cols: ListMap[String, Data] = ListMap((1 to cc).map { index =>
          (rsMeta.getColumnName(index), {
            // TODO use colType to create a JdbcType -> Data mapper, for now it's only Data.Str or Data.Null
            val colType = JdbcType.fromInt(rsMeta.getColumnType(index))
            val colValue = rs.getString(index)
            if (colValue == null)
              Data.Null
            else colType match {
              case jdbctype.Integer =>
                Data.Int(colValue.toInt)
              case jdbctype.VarChar =>
                Data.Str(colValue)
              case _ =>
                Data.Null // unsupported
            }
          })
        }:_*)
        Data.Obj(cols)
      }
      val set = (p: PreparedStatement, _: Int, d: I) => {
        d match {
          case Data.Obj(fields) =>
            val rsMeta = p.getParameterMetaData
            fields.zipWithIndex.foreach {
              case ((_, value), shiftedIndex) =>
                val index = shiftedIndex + 1
                val colType = JdbcType.fromInt(rsMeta.getParameterType(index))
                value match {
                  // TODO map colType
                  case Data.Int(num) => p.setInt(index, num.intValue())
                  case Data.Str(str) => p.setString(index, str)
                  case Data.Null => p.setNull(index, colType.toInt)
                  case _ =>
                    println("TODO handle unsupported operation. ")
                }

            }
          case other =>
            println("TODO handle unsupported operation. ")
        }
        ()
      }
      val setNull = (_: PreparedStatement, _: Int) => ()
      val update  = (_: ResultSet, _: Int, _: I) => ()
      val width   = 1
    }
    val meta   = Nil
    val toList = (d: Data) => List(d)
  }
}
