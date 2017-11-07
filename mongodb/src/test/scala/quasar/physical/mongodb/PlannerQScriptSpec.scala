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

package quasar.physical.mongodb

import slamdata.Predef._
import quasar._
import quasar.common.{Map => _, _}
import quasar.contrib.pathy._
import quasar.contrib.specs2.PendingWithActualTracking
import quasar.ejson.EJson._
import quasar.physical.mongodb.expression._
import quasar.physical.mongodb.planner._
import quasar.physical.mongodb.workflow._
import quasar.sql.JoinDir

import eu.timepit.refined.auto._
import matryoshka.data.Fix
import pathy.Path._

class PlannerQScriptSpec extends
    PlannerHelpers with
    PendingWithActualTracking {

  import fixExprOp._
  import PlannerHelpers._
  import CollectionUtil._
  import Reshape.reshape

  val (func, free, fix) =
    quasar.qscript.construction.mkDefaults[Fix, fs.MongoQScript[Fix, ?]]

  //TODO make this independent of MongoQScript and move to a place where all
  //     connector tests can refer to it
  val simpleJoin: Fix[fs.MongoQScript[Fix, ?]] =
    fix.EquiJoin(
      fix.Unreferenced,
      free.Filter(
        free.ShiftedRead[AFile](rootDir </> dir("db") </> file("zips"), qscript.ExcludeId),
        func.Guard(func.Hole, Type.AnyObject, func.Constant(bool[Fix](true)), func.Constant(bool[Fix](false)))),
      free.Filter(
        free.ShiftedRead[AFile](rootDir </> dir("db") </> file("smallZips"), qscript.ExcludeId),
        func.Guard(func.Hole, Type.AnyObject, func.Constant(bool[Fix](true)), func.Constant(bool[Fix](false)))),
      List((func.ProjectKeyS(func.Hole, "_id"), func.ProjectKeyS(func.Hole, "_id"))),
      JoinType.Inner,
      func.ProjectKeyS(func.RightSide, "city"))

  "plan from qscript" should {
    "plan simple join ($lookup)" in {
      qplan(simpleJoin) must beWorkflow0(chain[Workflow](
        $read(collection("db", "zips")),
        $match(Selector.Doc(
          BsonField.Name("_id") -> Selector.Exists(true))),
        $project(reshape(JoinDir.Left.name -> $$ROOT)),
        $lookup(
          CollectionName("smallZips"),
          JoinHandler.LeftName \ BsonField.Name("_id"),
          BsonField.Name("_id"),
          JoinHandler.RightName),
        $unwind(DocField(JoinHandler.RightName)),
        $project(
          reshape(sigil.Quasar -> $field(JoinDir.Right.name, "city")),
          ExcludeId)))
    }
  }
}
