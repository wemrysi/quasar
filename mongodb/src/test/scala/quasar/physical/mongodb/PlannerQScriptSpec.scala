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

import scala.collection.immutable.{Map => ScalaMap}

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

  val constr =
    quasar.qscript.construction.mkDefaults[Fix, fs.MongoQScript[Fix, ?]]

  import constr._1._, constr._2.{Hole => _, _}, constr.{_3 => fix}

  //TODO make this independent of MongoQScript and move to a place where all
  //     connector tests can refer to it
  val simpleJoin: Fix[fs.MongoQScript[Fix, ?]] =
    fix.EquiJoin(
      fix.Unreferenced,
      Filter(
        ShiftedRead[AFile](rootDir </> dir("db") </> file("zips"), qscript.ExcludeId),
        Guard(Hole, Type.Obj(ScalaMap(),Some(Type.Top)), Constant(bool[Fix](true)), Constant(bool[Fix](false)))),
      Filter(
        ShiftedRead[AFile](rootDir </> dir("db") </> file("smallZips"), qscript.ExcludeId),
        Guard(Hole, Type.Obj(ScalaMap(),Some(Type.Top)), Constant(bool[Fix](true)), Constant(bool[Fix](false)))),
      List((ProjectKeyS(Hole, "_id"), ProjectKeyS(Hole, "_id"))),
      JoinType.Inner,
      ProjectKey(RightSide, Constant(str[Fix]("city"))))

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
    }.pendingWithActual("#1560", qtestFile("plan simple join ($lookup)"))
  }
}
