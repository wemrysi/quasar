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

package quasar.physical.rdbms.fs.postgres

import slamdata.Predef._
import quasar.{Data, DataCodec}
import doobie.util.meta.Meta
import org.postgresql.util.PGobject

import scalaz.syntax.show._

package object mapping {

  implicit val codec = DataCodec.Precise

  val SingleFieldKey = "__p_single_"

  def stripSingleValue(d: Data): Data = {
    d match {
      case Data.Obj(lm) =>
        lm.toList match {
          case (`SingleFieldKey`, v) :: Nil => v
          case _ => d
        }
      case _ => d
    }
  }

  implicit val JsonDataMeta: Meta[Data] =
    Meta.other[PGobject]("json").xmap[Data](
      pGobject =>
        DataCodec.parse(pGobject.getValue)
          .map(stripSingleValue)
          .valueOr(err => scala.sys.error(err.shows)), // failure raises an exception
      data => {
        val o = new PGobject
        o.setType("json")
        o.setValue(DataCodec.render(data).getOrElse("{}"))
        o
      }
    )
}
