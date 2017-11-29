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

package quasar.physical.rdbms.fs.postgres.planner

import quasar.Qspec
import quasar.physical.rdbms.planner.{Planner, SqlExprSupport}
import quasar.qscript.{ExcludeId, IdOnly, IncludeId, ShiftedRead}
import quasar.contrib.pathy.AFile

import matryoshka.data.Fix
import pathy.Path._
import scalaz.Const
import scalaz.Scalaz.Id

class PostgresRenderQueryTest extends Qspec with SqlExprSupport {

  def sr: Planner[Fix, Id, Const[ShiftedRead[AFile], ?]] =
    Planner.constShiftedReadFilePlanner[Fix, Id]

  "PostgresJsonRenderQuery" should {
    "render shifted read with ExcludeId" in {

      val afile = rootDir </> dir("db") </> file("foo")
      val repr = sr.plan(Const(ShiftedRead(afile, ExcludeId)))

      PostgresRenderQuery.asString(repr) must
        beRightDisjunction("(select row_to_json(_0) from db.foo _0)")
    }

    "render shifted read with IncludeId" in {

      val afile = rootDir </> dir("db") </> file("foo")
      val repr = sr.plan(Const(ShiftedRead(afile, IncludeId)))

      PostgresRenderQuery.asString(repr) must
        beRightDisjunction(
          "(select (row_number() over(), row_to_json(_0)) from db.foo _0)")
    }

    "render shifted read ids only" in {

      val afile = rootDir </> dir("db") </> file("foo")
      val repr = sr.plan(Const(ShiftedRead(afile, IdOnly)))

      PostgresRenderQuery.asString(repr) must
        beRightDisjunction("(select row_number() over() from db.foo _0)")
    }

  }
}
