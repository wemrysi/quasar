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
import quasar.ejson.EJson
import quasar.fp._
import quasar.fs._
import quasar.sql.CompilerHelpers
import quasar.qscript.MapFuncs._

import scala.Predef.implicitly

import matryoshka._, FunctorT.ops._
import pathy.Path._
import scalaz._, Scalaz._

// TODO factor out the many calls to `transCata` into a single function
class QScriptOptimizeSpec extends quasar.Qspec with CompilerHelpers with QScriptHelpers {
  val opt = new Optimize[Fix]

  type QSI[A] =
    (QScriptCore[Fix, ?] :\: ProjectBucket[Fix, ?] :\: ThetaJoin[Fix, ?] :/: Const[DeadEnd, ?])#M[A]
  val DEI =     implicitly[Const[DeadEnd, ?] :<: QSI]
  val PBI = implicitly[ProjectBucket[Fix, ?] :<: QSI]
  val QCI =   implicitly[QScriptCore[Fix, ?] :<: QSI]
  val TJI =     implicitly[ThetaJoin[Fix, ?] :<: QSI]
  val RootI: QSI[Fix[QSI]] = DEI.inj(Const[DeadEnd, Fix[QSI]](Root))
  val UnreferencedI: QSI[Fix[QSI]] = QCI.inj(Unreferenced[Fix, Fix[QSI]]())

  implicit def qsiToQscriptTotal[T[_[_]]]: Injectable.Aux[QSI, QScriptTotal[Fix, ?]] =
    Injectable.coproduct(Injectable.inject[QScriptCore[Fix, ?], QScriptTotal[Fix, ?]],
      Injectable.coproduct(Injectable.inject[ProjectBucket[Fix, ?], QScriptTotal[Fix, ?]],
        Injectable.coproduct(Injectable.inject[ThetaJoin[Fix, ?], QScriptTotal[Fix, ?]],
          Injectable.inject[Const[DeadEnd, ?], QScriptTotal[Fix, ?]])))

  // TODO instead of calling `.toOption` on the `\/`
  // write an `Equal[PlannerError]` and test for specific errors too
  "optimizer" should {
    "elide a no-op map in a constant boolean" in {
       val query = LP.Constant(Data.Bool(true))
       val run = liftFG(opt.elideNopQC[QSI, QSI](idPrism.reverseGet))

       QueryFile.convertAndNormalize[Fix, QSI](query)(run).toOption must
         equal(chain(
           UnreferencedI,
           QCI.inj(Map((), BoolLit(true)))).some)
    }

    "optimize a basic read" in {
      val run =
        (SimplifyProjection[QSI, QSI].simplifyProjection(_: QSI[Fix[QSI]])) ⋙
          liftFF(repeatedly(Coalesce[Fix, QSI, QSI].coalesce(idPrism))) ⋙
          orOriginal(Normalizable[QSI].normalize(_: QSI[Fix[QSI]])) ⋙
          liftFF(repeatedly(Coalesce[Fix, QSI, QSI].coalesce(idPrism))) ⋙
          liftFF(repeatedly(opt.compactQC(_: QScriptCore[Fix, Fix[QSI]])))

      val query = lpRead("/foo")

      QueryFile.convertAndNormalize(query)(run).toOption must
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

    "fold a constant array value" in {
      val value: Fix[EJson] =
        EJson.fromExt[Fix].apply(ejson.Int[Fix[EJson]](7))

      val exp: QS[Fix[QS]] =
        QC.inj(Map(
          RootR,
          Free.roll(MakeArray(Free.roll(Constant(value))))))

      val expected: QS[Fix[QS]] =
        QC.inj(Map(
          RootR,
          Free.roll(Constant(ejson.CommonEJson.inj(ejson.Arr(List(value)))))))

      exp.embed.transCata[QS](orOriginal(Normalizable[QS].normalize(_: QS[Fix[QS]]))) must equal(expected.embed)
    }

    "elide a join with a constant on one side" in {
      val exp =
        TJ.inj(ThetaJoin(
          RootR.embed,
          Free.roll(QCT.inj(LeftShift(
            Free.roll(QCT.inj(Map(
              Free.roll(DET.inj(Const[DeadEnd, Free[QScriptTotal[Fix, ?], Hole]](Root))),
              ProjectFieldR(HoleF, StrLit("city"))))),
            Free.roll(ZipMapKeys(HoleF)),
            Free.roll(ConcatArrays(
              Free.roll(MakeArray(Free.point(LeftSide))),
              Free.roll(MakeArray(Free.point(RightSide)))))))),
          Free.roll(QCT.inj(Map(
            Free.roll(QCT.inj(Unreferenced[Fix, Free[QScriptTotal[Fix, ?], Hole]]())),
            StrLit("name")))),
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

    "fold a constant doubly-nested array value" in {
      val value: Fix[EJson] =
        ejson.EJson.fromExt[Fix].apply(ejson.Int[Fix[EJson]](7))

      val exp: QS[Fix[QS]] =
        QC.inj(Map(
          RootR,
          Free.roll(MakeArray(Free.roll(MakeArray(Free.roll(Constant(value))))))))

      val expected: QS[Fix[QS]] =
        QC.inj(Map(
          RootR,
          Free.roll(Constant(ejson.CommonEJson.inj(ejson.Arr(List(EJson.fromCommon[Fix].apply(ejson.Arr(List(value))))))))))

      exp.embed.transCata(orOriginal(Normalizable[QS].normalize(_: QS[Fix[QS]]))) must equal(expected.embed)
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
            Free.roll(Constant(
              ejson.CommonEJson.inj(ejson.Arr(List(EJson.fromCommon[Fix].apply(ejson.Str[Fix[ejson.EJson]]("name"))))))),
            Free.roll(MakeArray(
              Free.roll(ConcatArrays(
                Free.roll(MakeArray(
                  Free.roll(ConcatArrays(
                    Free.roll(MakeArray(Free.point(LeftSide))),
                    Free.roll(MakeArray(Free.point(RightSide))))))),
                Free.roll(Constant(
                  ejson.CommonEJson.inj(ejson.Arr(List(EJson.fromCommon[Fix].apply(ejson.Str[Fix[ejson.EJson]]("name"))))))))))))))).embed)
    }

    "fold nested boolean values" in {
      val falseBool: Fix[EJson] =
        EJson.fromCommon[Fix].apply(ejson.Bool[Fix[EJson]](false))

      val trueBool: Fix[EJson] =
        EJson.fromCommon[Fix].apply(ejson.Bool[Fix[EJson]](true))

      val exp: QS[Fix[QS]] =
        QC.inj(Map(
          RootR,
          Free.roll(MakeArray(
            // !false && (false || !true)
            Free.roll(And(
              Free.roll(Not(Free.roll(Constant(falseBool)))),
              Free.roll(Or(
                Free.roll(Constant(falseBool)),
                Free.roll(Not(Free.roll(Constant(trueBool))))))))))))

      val expected: QS[Fix[QS]] =
        QC.inj(Map(
          RootR,
          Free.roll(Constant(
            ejson.CommonEJson.inj(ejson.Arr(List(EJson.fromCommon[Fix].apply(ejson.Bool[Fix[ejson.EJson]](false)))))))))

      exp.embed.transCata(orOriginal(Normalizable[QS].normalize(_: QS[Fix[QS]]))) must equal(expected.embed)
    }

    "simplify a ThetaJoin" in {
      val exp: Fix[QS] =
        TJ.inj(ThetaJoin(
          QC.inj(Unreferenced[Fix, Fix[QS]]()).embed,
          Free.roll(RT.inj(Const(Read(rootDir </> file("foo"))))),
          Free.roll(RT.inj(Const(Read(rootDir </> file("bar"))))),
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
        QCT.inj(Map(
          QCT.inj(Filter(
            EJT.inj(EquiJoin(
              QCT.inj(Unreferenced[Fix, Fix[QST]]()).embed,
              Free.roll(RT.inj(Const(Read(rootDir </> file("foo"))))),
              Free.roll(RT.inj(Const(Read(rootDir </> file("bar"))))),
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
                StrLit("r_lat"))))))).embed,
          Free.roll(ConcatMaps(
            Free.roll(ProjectIndex(Free.point(SrcHole), IntLit(0))),
            Free.roll(ProjectIndex(Free.point(SrcHole), IntLit(1))))))).embed)
    }
    "transform a ShiftedRead with IncludeId to ExcludeId when possible" in {
      val sampleFile = rootDir </> file("bar")

      val originalQScript = Fix(QS.inject(QC.inj(Map(
        Fix(SRT.inj(Const[ShiftedRead, Fix[QST]](ShiftedRead(sampleFile, IncludeId)))),
        Free.roll(ProjectIndex(HoleF, IntLit(1)))))))

      val expectedQScript = Fix(SRT.inj(Const[ShiftedRead, Fix[QST]](ShiftedRead(sampleFile, ExcludeId))))

      originalQScript.transCata(liftFG(opt.transformIncludeToExclude[QST])) must_= expectedQScript
    }
  }
}
