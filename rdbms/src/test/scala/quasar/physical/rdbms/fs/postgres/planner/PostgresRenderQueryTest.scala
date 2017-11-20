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

import slamdata.Predef._

import quasar.{Qspec, qscript}
import quasar.physical.rdbms.planner.{Planner, SqlExprSupport}
import quasar.qscript._
import quasar.contrib.pathy.AFile
import matryoshka.data.Fix
import quasar.qscript.MapFuncsCore.StrLit
import pathy.Path._
import quasar.fp.ski.κ
import quasar.physical.rdbms.planner.sql.SqlExpr

import matryoshka._
import matryoshka.data._
import matryoshka.implicits._
import matryoshka.patterns._
import scalaz._
import Scalaz._
import scalaz.concurrent.Task

class PostgresRenderQueryTest extends Qspec with SqlExprSupport with QScriptHelpers {

  def sr: Planner[Fix, Id, Const[ShiftedRead[AFile], ?]] =
    Planner.constShiftedReadFilePlanner[Fix, Id]

  def core: Planner[Fix, Task, qscript.MapFunc[Fix, ?]] =
    Planner.mapFuncPlanner[Fix, Task]

  "PostgresJsonRenderQuery" should {
    "render shifted read with ExcludeId" in {

      val afile = rootDir </> dir("db") </> file("foo")
      val repr = sr.plan(Const(ShiftedRead(afile, ExcludeId)))

      PostgresRenderQuery.asString(repr) must
        beRightDisjunction("(select row_to_json(_0) _0 from db.foo _0)")
    }

    "render shifted read with IncludeId" in {

      val afile = rootDir </> dir("db") </> file("foo")
      val repr = sr.plan(Const(ShiftedRead(afile, IncludeId)))

      PostgresRenderQuery.asString(repr) must
        beRightDisjunction(
          "(select (row_number() over(), row_to_json(_0)) _0 from db.foo _0)")
    }

    "render shifted read ids only" in {

      val afile = rootDir </> dir("db") </> file("foo")
      val repr = sr.plan(Const(ShiftedRead(afile, IdOnly)))

      PostgresRenderQuery.asString(repr) must
        beRightDisjunction("(select row_number() over() _0 from db.foo _0)")
    }

    def pKey(name: String) = ProjectKeyR(HoleF, StrLit(name))

    def aliasToHole(aliasStr: String) = {
      val id: SqlExpr[Fix[SqlExpr]] = SqlExpr.Id(aliasStr)
      κ(id.embed.η[Task])
    }

    def qsToRepr(m: qscript.FreeMap[Fix]) =
      m.cataM(interpretM(aliasToHole("d"), core.plan)).unsafePerformSync

    "render addition" in {
      val qs = AddR(pKey("a"), pKey("b"))

      PostgresRenderQuery.asString(qsToRepr(qs)) must
        beRightDisjunction("((d->>'a')::numeric + (d->>'b')::numeric)")
    }

  }
}
