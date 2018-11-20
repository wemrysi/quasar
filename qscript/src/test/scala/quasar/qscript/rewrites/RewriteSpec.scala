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

package quasar.qscript.rewrites

import quasar._
import quasar.api.resource.ResourcePath
import quasar.common.JoinType
import quasar.fp._
import quasar.contrib.iota._
import quasar.contrib.iota.SubInject
import quasar.qscript._

import matryoshka.data.Fix
import matryoshka.implicits._
import pathy.Path._
import scalaz._, Scalaz._

import iotaz.{CopK, TNilK}
import iotaz.TListK.:::

object RewriteSpec extends quasar.Qspec with QScriptHelpers {
  import IdStatus.ExcludeId

  val rewrite = new Rewrite[Fix]

  def normalizeExpr(expr: Fix[QS]): Fix[QS] =
    expr.transCata[Fix[QS]](rewrite.normalizeT[QS])

  type QSI[A] = CopK[QScriptCore ::: ThetaJoin ::: TNilK, A]

  implicit val qsc: Injectable[QScriptCore, QSI] = Injectable.inject[QScriptCore, QSI]
  implicit val tj: Injectable[ThetaJoin, QSI] = Injectable.inject[ThetaJoin, QSI]

  val qsidsl = construction.mkDefaults[Fix, QSI]

  implicit def qsiToQscriptTotal: Injectable[QSI, QST] = SubInject[QSI, QST]

  // TODO instead of calling `.toOption` on the `\/`
  // write an `Equal[PlannerError]` and test for specific errors too
  "rewriter" should {

    "coalesce a Map into a subsequent LeftShift" in {
      import qsidsl._
      val exp: QSI[Fix[QSI]] =
        fix.LeftShift(
          fix.Map(
            fix.Unreferenced,
            recFunc.Constant(json.bool(true))),
          recFunc.Hole,
          ExcludeId,
          ShiftType.Array,
          OnUndefined.Omit,
          func.RightSide).unFix

      Coalesce[Fix, QSI, QSI].coalesceQC(idPrism).apply(exp) must
      equal(
        fix.LeftShift(
          fix.Unreferenced,
          recFunc.Constant(json.bool(true)),
          ExcludeId,
          ShiftType.Array,
          OnUndefined.Omit,
          func.RightSide).unFix.some)
    }

    "extract filter from join condition" >> {
      "when guard is undefined in true branch" >> {
        import qsdsl._
        val original =
          fix.ThetaJoin(
            fix.Unreferenced,
            free.Read[ResourcePath](ResourcePath.leaf(rootDir </> file("foo")), ExcludeId),
            free.Read[ResourcePath](ResourcePath.leaf(rootDir </> file("bar")), ExcludeId),
            func.Guard(
              func.LeftSide,
              Type.AnyObject,
              func.Undefined,
              func.Eq(
                func.ProjectKey(func.RightSide, func.Constant(json.str("r_id"))),
                func.ProjectKey(func.LeftSide, func.Constant(json.str("l_id"))))),
            JoinType.Inner,
            func.ConcatMaps(
              func.Guard(
                func.LeftSide,
                Type.AnyObject,
                func.Undefined,
                func.LeftSide),
              func.RightSide))

        val expected =
          fix.ThetaJoin(
            fix.Unreferenced,
            free.Filter(
              free.Read[ResourcePath](ResourcePath.leaf(rootDir </> file("foo")), ExcludeId),
              recFunc.Guard(
                recFunc.Hole,
                Type.AnyObject,
                recFunc.Constant(json.bool(false)),
                recFunc.Constant(json.bool(true)))),
            free.Read[ResourcePath](ResourcePath.leaf(rootDir </> file("bar")), ExcludeId),
            func.Eq(
              func.ProjectKeyS(func.RightSide, "r_id"),
              func.ProjectKeyS(func.LeftSide, "l_id")),
            JoinType.Inner,
            func.ConcatMaps(func.LeftSide, func.RightSide))

        normalizeExpr(original) must equal(expected)
      }

      "when guard is undefined in false branch" >> {
        import qsdsl._
        val original =
          fix.ThetaJoin(
            fix.Unreferenced,
            free.Read[ResourcePath](ResourcePath.leaf(rootDir </> file("foo")), ExcludeId),
            free.Read[ResourcePath](ResourcePath.leaf(rootDir </> file("bar")), ExcludeId),
            func.Guard(
              func.LeftSide,
              Type.AnyObject,
              func.Eq(
                func.ProjectKeyS(func.RightSide, "r_id"),
                func.ProjectKeyS(func.LeftSide, "l_id")),
              func.Undefined),
            JoinType.Inner,
            func.ConcatMaps(
              func.Guard(
                func.LeftSide,
                Type.AnyObject,
                func.LeftSide,
                func.Undefined),
              func.RightSide))

        val expected =
          fix.ThetaJoin(
            fix.Unreferenced,
            free.Filter(
              free.Read[ResourcePath](ResourcePath.leaf(rootDir </> file("foo")), ExcludeId),
              recFunc.Guard(
                recFunc.Hole,
                Type.AnyObject,
                recFunc.Constant(json.bool(true)),
                recFunc.Constant(json.bool(false)))),
            free.Read[ResourcePath](ResourcePath.leaf(rootDir </> file("bar")), ExcludeId),
            func.Eq(
              func.ProjectKeyS(func.RightSide, "r_id"),
              func.ProjectKeyS(func.LeftSide, "l_id")),
            JoinType.Inner,
            func.ConcatMaps(func.LeftSide, func.RightSide))

        normalizeExpr(original) must equal(expected)
      }

      "when cond is undefined in true branch" >> {
        import qsdsl._
        val original =
          fix.ThetaJoin(
            fix.Unreferenced,
            free.Read[ResourcePath](ResourcePath.leaf(rootDir </> file("foo")), ExcludeId),
            free.Read[ResourcePath](ResourcePath.leaf(rootDir </> file("bar")), ExcludeId),
            func.Cond(
              func.Lt(func.ProjectKeyS(func.LeftSide, "x"), func.Constant(json.int(7))),
              func.Undefined,
              func.Eq(
                func.ProjectKeyS(func.RightSide, "r_id"),
                func.ProjectKeyS(func.LeftSide, "l_id"))),
            JoinType.Inner,
            func.ConcatMaps(
              func.Cond(
                func.Lt(func.ProjectKeyS(func.LeftSide, "x"), func.Constant(json.int(7))),
                func.Undefined,
                func.LeftSide),
              func.RightSide))

        val expected =
          fix.ThetaJoin(
            fix.Unreferenced,
            free.Filter(
              free.Read[ResourcePath](ResourcePath.leaf(rootDir </> file("foo")), ExcludeId),
              recFunc.Not(recFunc.Lt(recFunc.ProjectKeyS(recFunc.Hole, "x"), recFunc.Constant(json.int(7))))),
            free.Read[ResourcePath](ResourcePath.leaf(rootDir </> file("bar")), ExcludeId),
            func.Eq(
              func.ProjectKeyS(func.RightSide, "r_id"),
              func.ProjectKeyS(func.LeftSide, "l_id")),
            JoinType.Inner,
            func.ConcatMaps(func.LeftSide, func.RightSide))

        normalizeExpr(original) must equal(expected)
      }

      "when cond is undefined in false branch" >> {
        import qsdsl._
        val original =
          fix.ThetaJoin(
            fix.Unreferenced,
            free.Read[ResourcePath](ResourcePath.leaf(rootDir </> file("foo")), ExcludeId),
            free.Read[ResourcePath](ResourcePath.leaf(rootDir </> file("bar")), ExcludeId),
            func.Cond(
              func.Lt(func.ProjectKeyS(func.LeftSide, "x"), func.Constant(json.int(7))),
              func.Eq(
                func.ProjectKeyS(func.RightSide, "r_id"),
                func.ProjectKeyS(func.LeftSide, "l_id")),
              func.Undefined),
            JoinType.Inner,
            func.ConcatMaps(
              func.Cond(
                func.Lt(func.ProjectKeyS(func.LeftSide, "x"), func.Constant(json.int(7))),
                func.LeftSide,
                func.Undefined),
              func.RightSide))

        val expected =
          fix.ThetaJoin(
            fix.Unreferenced,
            free.Filter(
              free.Read[ResourcePath](ResourcePath.leaf(rootDir </> file("foo")), ExcludeId),
              recFunc.Lt(recFunc.ProjectKeyS(recFunc.Hole, "x"), recFunc.Constant(json.int(7)))),
            free.Read[ResourcePath](ResourcePath.leaf(rootDir </> file("bar")), ExcludeId),
            func.Eq(
              func.ProjectKeyS(func.RightSide, "r_id"),
              func.ProjectKeyS(func.LeftSide, "l_id")),
            JoinType.Inner,
            func.ConcatMaps(func.LeftSide, func.RightSide))

        normalizeExpr(original) must equal(expected)
      }
    }
  }
}
