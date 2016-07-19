/*
 * Copyright 2014–2016 SlamData Inc.
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

package quasar.qscript

import quasar.Predef._
import quasar.{LogicalPlan, Data, CompilerHelpers}
import quasar.{LogicalPlan => LP}
import quasar.fp._
import quasar.fs._
import quasar.qscript.MapFuncs._
import quasar.std.StdLib._

import scala.Predef.implicitly

import matryoshka._
import org.specs2.scalaz._
import pathy.Path._
import scalaz._, Scalaz._

class QScriptSpec extends CompilerHelpers with ScalazMatchers {
  val transform = new Transform[Fix, QScriptProject[Fix, ?]]

  // TODO: Narrow this to QScriptPure
  type QS[A] = QScriptProject[Fix, A]
  val DE = implicitly[Const[DeadEnd, ?] :<: QS]
  val QC = implicitly[QScriptCore[Fix, ?] :<: QS]
  val TJ = implicitly[ThetaJoin[Fix, ?] :<: QS]

  def RootR = CorecursiveOps[Fix, QS](DE.inj(Const[DeadEnd, Fix[QS]](Root))).embed

  def ProjectFieldR[A](src: FreeMap[Fix], field: FreeMap[Fix]): FreeMap[Fix] =
    Free.roll(ProjectField(src, field))

  def lpRead(path: String): Fix[LP] =
    LogicalPlan.Read(sandboxAbs(posixCodec.parseAbsFile(path).get))

  // TODO instead of calling `.toOption` on the `\/`
  // write an `Equal[PlannerError]` and test for specific errors too
  "replan" should {
    "convert a constant boolean" in {
       // "select true"
       QueryFile.convertToQScript(LogicalPlan.Constant(Data.Bool(true))).toOption must
       equal(
         QC.inj(Map(RootR, BoolLit(true))).embed.some)
    }

    // TODO EJson does not support Data.Set
    "fail to convert a constant set" in {
      // "select {\"a\": 1, \"b\": 2, \"c\": 3, \"d\": 4, \"e\": 5}{*} limit 3 offset 1"
      QueryFile.convertToQScript(
        LogicalPlan.Constant(Data.Set(List(
          Data.Obj(ListMap("0" -> Data.Int(2))),
          Data.Obj(ListMap("0" -> Data.Int(3))))))).toOption must
      equal(None)
    }

    "convert a simple read" in {
      QueryFile.convertToQScript(lpRead("/foo")).toOption must
      equal(
        QC.inj(Map(RootR, ProjectFieldR(UnitF, StrLit("foo")))).embed.some)
    }

    "convert a squashed read" in {
      // "select * from foo"
      QueryFile.convertToQScript(
        identity.Squash(lpRead("/foo"))).toOption must
      equal(
        QC.inj(Map(RootR, ProjectFieldR(UnitF, StrLit("foo")))).embed.some)
    }

    "convert a simple read with path projects" in {
      QueryFile.convertToQScript(
        lpRead("/some/foo/bar")).toOption must
      equal(
        QC.inj(
          Map(RootR,
            ProjectFieldR(
              ProjectFieldR(
                ProjectFieldR(
                  UnitF,
                  StrLit("some")),
                StrLit("foo")),
              StrLit("bar")))).embed.some)
    }

    "convert a basic invoke" in {
      QueryFile.convertToQScript(
        math.Add(lpRead("/foo"), lpRead("/bar")).embed).toOption must
      equal(
        QC.inj(
          Map(RootR,
            Free.roll(Add(
              ProjectFieldR(UnitF, StrLit("foo")),
              ProjectFieldR(UnitF, StrLit("bar")))))).embed.some)
    }

    "convert project object and make object" in {
      QueryFile.convertToQScript(
        identity.Squash(
          makeObj(
            "name" -> structural.ObjectProject(
              lpRead("/city"),
              LogicalPlan.Constant(Data.Str("name")))))).toOption must
      equal(
        QC.inj(
          Map(RootR,
            Free.roll(MakeMap(StrLit("name"),
              Free.roll(ProjectField(
                Free.roll(ProjectField(UnitF, StrLit("city"))),
                StrLit("name"))))))).embed.some)
    }

    "convert a shift array" in {
      QueryFile.convertToQScript(
        identity.Squash[FLP](
          structural.ShiftArray[FLP](
            structural.ArrayConcat[FLP](
              structural.ArrayConcat[FLP](
                structural.MakeArrayN[Fix](LogicalPlan.Constant(Data.Int(1))),
                structural.MakeArrayN[Fix](LogicalPlan.Constant(Data.Int(2)))),
              structural.MakeArrayN[Fix](LogicalPlan.Constant(Data.Int(3))))))).toOption must
      equal(RootR.some) // TODO incorrect expectation
    }

    "convert basic join" in {  // TODO normalization
      //"select foo.name, bar.address from foo join bar on foo.id = bar.foo_id",

      val lp = LP.Let('__tmp0, lpRead("/foo"),
        LP.Let('__tmp1, lpRead("/bar"),
          LP.Let('__tmp2,
            set.InnerJoin[FLP](LP.Free('__tmp0), LP.Free('__tmp1),
              relations.Eq[FLP](
                structural.ObjectProject(LP.Free('__tmp0), LP.Constant(Data.Str("id"))),
                structural.ObjectProject(LP.Free('__tmp1), LP.Constant(Data.Str("foo_id"))))),
            makeObj(
              "name" ->
                structural.ObjectProject[FLP](
                  structural.ObjectProject(LP.Free('__tmp2), LP.Constant(Data.Str("left"))),
                  LP.Constant(Data.Str("name"))),
              "address" ->
                structural.ObjectProject[FLP](
                  structural.ObjectProject(LP.Free('__tmp2), LP.Constant(Data.Str("right"))),
                  LP.Constant(Data.Str("address")))))))
      QueryFile.convertToQScript(lp).toOption must equal(
        QC.inj(Map(RootR, ProjectFieldR(UnitF, StrLit("foo")))).embed.some)
    }
  }
}
