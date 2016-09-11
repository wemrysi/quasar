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
import quasar.{LogicalPlan => LP, _}
import quasar.qscript.MapFuncs._
import quasar.fp._
import quasar.fs._

import scala.Predef.implicitly

import matryoshka._, FunctorT.ops._
import pathy.Path._
import scalaz._, Scalaz._

class QScriptOptimizeSpec extends quasar.Qspec with CompilerHelpers with QScriptHelpers {
  val opt = new quasar.qscript.Optimize[Fix]

  type QSI[A] =
    (QScriptCore[Fix, ?] :\: ProjectBucket[Fix, ?] :\: ThetaJoin[Fix, ?] :/: Const[DeadEnd, ?])#M[A]
  val DEI =     implicitly[Const[DeadEnd, ?] :<: QSI]
  val PBI = implicitly[ProjectBucket[Fix, ?] :<: QSI]
  val QCI =   implicitly[QScriptCore[Fix, ?] :<: QSI]
  val TJI =     implicitly[ThetaJoin[Fix, ?] :<: QSI]
  val RootI: QSI[Fix[QSI]] = DEI.inj(Const[DeadEnd, Fix[QSI]](Root))
  val UnreferencedI: QSI[Fix[QSI]] = QCI.inj(Unreferenced[Fix, Fix[QSI]]())

  // TODO instead of calling `.toOption` on the `\/`
  // write an `Equal[PlannerError]` and test for specific errors too
  "optimizer" should {
    "elide a no-op map in a constant boolean" in {
       val query = LP.Constant(Data.Bool(true))
       val run = liftFG(opt.elideNopMap[QSI])

       QueryFile.optimizeEval[Fix, QSI](query)(run).toOption must
         equal(chain(
           UnreferencedI,
           QCI.inj(Map((), BoolLit(true)))).some)
    }

    "optimize a basic read" in {
      val run =
        (SimplifyProjection[QSI, QSI].simplifyProjection(_: QSI[Fix[QSI]])) ⋙
          liftFG(opt.coalesceMapShift[QSI, QSI](idPrism.get)) ⋙
          Normalizable[QSI].normalize ⋙
          liftFF(opt.simplifyQC[QSI, QSI](idPrism)) ⋙
          liftFG(opt.compactLeftShift[QSI, QSI])

      val query = lpRead("/foo")

      QueryFile.optimizeEval(query)(run).toOption must
      equal(chain(
        RootI,
        QCI.inj(LeftShift((),
          ProjectFieldR(HoleF, StrLit("foo")),
          Free.point(RightSide)))).some)
    }

    "simplify a ThetaJoin" in {
      val exp =
        TJ.inj(ThetaJoin(
          QC.inj(Unreferenced[Fix, Fix[QS]]()).embed,
          Free.roll(QST[QS].inject(R.inj(Const(Read(rootDir </> file("foo")))))),
          Free.roll(QST[QS].inject(R.inj(Const(Read(rootDir </> file("bar")))))),
          Free.roll(And(Free.roll(And(
            // reversed equality
            Free.roll(Eq(
              Free.roll(ProjectField(Free.point(RightSide), StrLit("r_id"))),
              Free.roll(ProjectField(Free.point(LeftSide), StrLit("l_id"))))),
            // more complicated expression, duplicated refs
            Free.roll(Eq(
              Free.roll(Add(
                Free.roll(ProjectField(Free.point(LeftSide), StrLit("l_min"))),
                Free.roll(ProjectField(Free.point(LeftSide), StrLit("l_max"))))),
              Free.roll(Subtract(
                Free.roll(ProjectField(Free.point(RightSide), StrLit("l_max"))),
                Free.roll(ProjectField(Free.point(RightSide), StrLit("l_min"))))))))),
            // inequality
            Free.roll(Lt(
              Free.roll(ProjectField(Free.point(LeftSide), StrLit("l_lat"))),
              Free.roll(ProjectField(Free.point(RightSide), StrLit("r_lat"))))))),
          Inner,
          Free.roll(ConcatMaps(Free.point(LeftSide), Free.point(RightSide))))).embed

      exp.transCata(SimplifyJoin[Fix, QS, QST].simplifyJoin(idPrism.reverseGet)) must equal(
        QS.inject(QC.inj(Map(
          QS.inject(QC.inj(Filter(
            EJ.inj(EquiJoin(
              QS.inject(QC.inj(Unreferenced[Fix, Fix[QST]]())).embed,
              Free.roll(QST[QS].inject(R.inj(Const(Read(rootDir </> file("foo")))))),
              Free.roll(QST[QS].inject(R.inj(Const(Read(rootDir </> file("bar")))))),
              Free.roll(ConcatArrays(
                Free.roll(MakeArray(
                  Free.roll(ProjectField(Free.point(SrcHole), StrLit("l_id"))))),
                Free.roll(MakeArray(
                  Free.roll(Add(
                    Free.roll(ProjectField(Free.point(SrcHole), StrLit("l_min"))),
                    Free.roll(ProjectField(Free.point(SrcHole), StrLit("l_max"))))))))),
              Free.roll(ConcatArrays(
                Free.roll(MakeArray(
                  Free.roll(ProjectField(Free.point(SrcHole), StrLit("r_id"))))),
                Free.roll(MakeArray(
                  Free.roll(Subtract(
                    Free.roll(ProjectField(Free.point(SrcHole), StrLit("l_max"))),
                    Free.roll(ProjectField(Free.point(SrcHole), StrLit("l_min"))))))))),
              Inner,
              Free.roll(ConcatArrays(
                Free.roll(MakeArray(Free.point(LeftSide))),
                Free.roll(MakeArray(Free.point(RightSide))))))).embed,
            Free.roll(Lt(
              Free.roll(ProjectField(
                Free.roll(ProjectIndex(Free.point(SrcHole), IntLit(0))),
                StrLit("l_lat"))),
              Free.roll(ProjectField(
                Free.roll(ProjectIndex(Free.point(SrcHole), IntLit(1))),
                StrLit("r_lat")))))))).embed,
          Free.roll(ConcatMaps(
            Free.roll(ProjectIndex(Free.point(SrcHole), IntLit(0))),
            Free.roll(ProjectIndex(Free.point(SrcHole), IntLit(1)))))))).embed)
    }
  }
}
