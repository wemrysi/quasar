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
import java.util.UUID
import doobie.util.composite.Composite
import doobie.util.kernel.Kernel
import quasar.fs.FileSystemError
import quasar.{Data, DataCodec}
import slamdata.Predef._

import scalaz.syntax.show._

package object json {

  implicit val JsonDataComposite: Composite[Data] =  new Composite[Data] {
    implicit val codec = DataCodec.Precise

    val kernel = new Kernel[Data] {
      type I      = Data
      val ia      = (i: I) => i
      val ai      = (a: I) => a
      val get     = (rs: ResultSet, _: Int) => {
        val rsMeta = rs.getMetaData
        val cc = rsMeta.getColumnCount
        val jsonColumnIndex = rs.findColumn("data")
        if (jsonColumnIndex > 0) {
          val jsonStr = rs.getString(jsonColumnIndex)
          val dataOrErr = DataCodec.parse(jsonStr).leftMap(err =>
            FileSystemError.readFailed(jsonStr, err.shows))
          // TODO throw err? log?
          dataOrErr.getOrElse(Data.Null)
        }
        else {
          // TODO log error? throw?
          Data.Null
        }
      }
      val set = (p: PreparedStatement, _: Int, d: I) => {
        DataCodec.render(d).foreach { jsonStr =>
          p.setObject(1, UUID.randomUUID().toString)
          p.setString(2, jsonStr)
        }
      }
      val setNull = (_: PreparedStatement, _: Int) => ()
      val update  = (_: ResultSet, _: Int, _: I) => ()
      val width   = 1
    }
    val meta   = Nil
    val toList = (d: Data) => List(d)
  }
}
