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

import slamdata.Predef._
import java.sql.{PreparedStatement, ResultSet}
import doobie.imports.Composite
import doobie.util.kernel.Kernel
import quasar.{Data, DataCodec}
import doobie.util.meta.Meta
import org.postgresql.util.PGobject

import scalaz.syntax.show._

package object mapping {

  implicit val codec = DataCodec.Precise

  implicit val JsonDataMeta: Meta[Data] =
    Meta.other[PGobject]("json").xmap[Data](
      pGobject =>
        DataCodec.parse(pGobject.getValue).valueOr(err => scala.sys.error(err.shows)), // failure raises an exception
      data => {
        val o = new PGobject
        o.setType("json")
        o.setValue(DataCodec.render(data).getOrElse("{}"))
        o
      }
    )

  final case class ColumnCount(cols: Int)

  val CountColsDataComposite: Composite[ColumnCount] = new Composite[ColumnCount] {

    val kernel = new Kernel[ColumnCount] {
      type I = ColumnCount
      val ia = (i: I) => i
      val ai = (a: I) => a
      val get = (rs: ResultSet, _: Int)  => {
        ColumnCount(rs.getMetaData.getColumnCount)
      }

      val set = (_: PreparedStatement, _: Int, _: I) => ()
      val setNull = (_: PreparedStatement, _: Int) => ()
      val update = (_: ResultSet, _: Int, _: I) => ()
      val width = 0

    }
    val meta   = Nil
    val toList = (c: ColumnCount) => List(c)

  }

}
