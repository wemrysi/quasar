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
       val run = liftFG(opt.elideNopQC[QSI, QSI](idPrism.reverseGet))

       QueryFile.optimizeEval[Fix, QSI](query)(run).toOption must
         equal(chain(
           UnreferencedI,
           QCI.inj(Map((), BoolLit(true)))).some)
    }

    "optimize a basic read" in {
      val run =
        (SimplifyProjection[QSI, QSI].simplifyProjection(_: QSI[Fix[QSI]])) ⋙
          liftFF(repeatedly(Coalesce[Fix, QSI, QSI].coalesce(idPrism))) ⋙
          Normalizable[QSI].normalize ⋙
          liftFF(repeatedly(Coalesce[Fix, QSI, QSI].coalesce(idPrism))) ⋙
          liftFF(repeatedly(opt.compactQC(_: QScriptCore[Fix, Fix[QSI]])))

      val query = lpRead("/foo")

      QueryFile.optimizeEval(query)(run).toOption must
      equal(chain(
        RootI,
        QCI.inj(LeftShift((),
          ProjectFieldR(HoleF, StrLit("foo")),
          Free.point(RightSide)))).some)
    }

    "coalesce a Map into a subsequent LeftShift" in {
      val exp =
        LeftShift(
          Map(
            Unreferenced[Fix, Fix[QScriptCore[Fix, ?]]]().embed,
            BoolLit[Fix, Hole](true)).embed,
          HoleF,
          Free.point[MapFunc[Fix, ?], JoinSide](RightSide))

      Coalesce[Fix, QScriptCore[Fix, ?], QScriptCore[Fix, ?]].coalesce(idPrism).apply(exp) must
      equal(
        LeftShift(
          Unreferenced[Fix, Fix[QScriptCore[Fix, ?]]]().embed,
          BoolLit[Fix, Hole](true),
          Free.point[MapFunc[Fix, ?], JoinSide](RightSide)).some)
    }

    "elide a join with a constant on one side" in {
      val exp =
        TJ.inj(ThetaJoin(
          RootR.embed,
          Free.roll(QST[QS].inject(QC.inj(LeftShift(
            Free.roll(QST[QS].inject(QC.inj(Map(
              Free.roll(QST[QS].inject(DE.inj(Const[DeadEnd, Free[QScriptTotal[Fix, ?], Hole]](Root)))),
              ProjectFieldR(HoleF, StrLit("city")))))),
            Free.roll(ZipMapKeys(HoleF)),
            Free.roll(ConcatArrays(
              Free.roll(MakeArray(Free.point(LeftSide))),
              Free.roll(MakeArray(Free.point(RightSide))))))))),
          Free.roll(QST[QS].inject(QC.inj(Map(
            Free.roll(QST[QS].inject(QC.inj(Unreferenced[Fix, Free[QScriptTotal[Fix, ?], Hole]]()))),
            StrLit("name"))))),
          BoolLit[Fix, JoinSide](true),
          Inner,
          ProjectFieldR(
            Free.roll(ProjectIndex(
              Free.roll(ProjectIndex(Free.point(LeftSide), IntLit(1))),
              IntLit(1))),
            Free.point(RightSide)))).embed

      // TODO: only require a single pass
      exp.transCata(opt.applyAll[QS]).transCata(opt.applyAll[QS]) must equal(
        chain(
          RootR,
          QC.inj(LeftShift((),
            ProjectFieldR(HoleF, StrLit("city")),
            ProjectFieldR(Free.point(RightSide), StrLit("name"))))))
    }

    "elide a join in the branch of a join" in {
      val exp =
        TJT.inj(ThetaJoin(
          DET.inj(Const[DeadEnd, Fix[QScriptTotal[Fix, ?]]](Root)).embed,
          Free.roll(QCT.inj(Map(
            Free.roll(QCT.inj(Unreferenced())),
            StrLit("name")))),
          Free.roll(TJT.inj(ThetaJoin(
            Free.roll(DET.inj(Const(Root))),
            Free.roll(QCT.inj(LeftShift(
              Free.point(SrcHole),
              Free.roll(ZipMapKeys(HoleF)),
              Free.roll(ConcatArrays(
                Free.roll(MakeArray(Free.point(LeftSide))),
                Free.roll(MakeArray(Free.point(RightSide)))))))),
            Free.roll(QCT.inj(Map(
              Free.roll(QCT.inj(Unreferenced())),
              StrLit("name")))),
            BoolLit(true),
            Inner,
            Free.roll(ConcatArrays(
              Free.roll(MakeArray(Free.point(LeftSide))),
              Free.roll(MakeArray(Free.point(RightSide)))))))),
          BoolLit(true),
          Inner,
          Free.roll(ConcatArrays(
            Free.roll(MakeArray(Free.point(LeftSide))),
            Free.roll(MakeArray(Free.point(RightSide))))))).embed

      // TODO: only require a single pass
      exp.transCata(opt.applyAll[QST]).transCata(opt.applyAll[QST]) must
      equal(
        QCT.inj(LeftShift(
          DET.inj(Const[DeadEnd, Fix[QST]](Root)).embed,
          Free.roll(ZipMapKeys(HoleF)),
          Free.roll(ConcatArrays(
            Free.roll(MakeArray(StrLit("name"))),
            Free.roll(MakeArray(
              Free.roll(ConcatArrays(
                Free.roll(MakeArray(
                  Free.roll(ConcatArrays(
                    Free.roll(MakeArray(Free.point(LeftSide))),
                    Free.roll(MakeArray(Free.point(RightSide))))))),
                Free.roll(MakeArray(StrLit("name"))))))))))).embed)
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
