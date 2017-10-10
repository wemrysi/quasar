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

package quasar.qscript.analysis

import slamdata.Predef.{Map => _, _}

import scala.Predef.implicitly

import quasar.common.JoinType
import quasar.qscript._
import quasar.fp._
import quasar.qscript.MapFuncsCore._

import matryoshka.Algebra
import matryoshka.data._
import scalaz._, Scalaz._

final class DeepShapeSpec extends quasar.Qspec with QScriptHelpers with TTypes[Fix] {
  import DeepShape._

  "DeepShape" >> {

    val shape: FreeShape[Fix] = ProjectFieldR(freeShape[Fix](RootShape()), StrLit("quxx"))

    "QScriptCore" >> {

      val deepShapeQS: Algebra[QScriptCore, FreeShape[Fix]] =
        implicitly[DeepShape[Fix, QScriptCore]].deepShapeƒ

      "Map" >> {
        val func: FreeMap = ProjectIndexR(HoleF[Fix], IntLit(3))
        val qs = Map(shape, func)

        deepShapeQS(qs) must equal(func >> shape)
      }

      "LeftShift" >> {
        val struct: FreeMap = ProjectIndexR(HoleF[Fix], IntLit(3))

        val repair: JoinFunc =
          ConcatArraysR(
            MakeArrayR(AddR(LeftSideF[Fix], IntLit(9))),
            MakeArrayR(SubtractR(RightSideF[Fix], IntLit(10))))

        val qs = LeftShift(shape, struct, IdOnly, repair)

        val expected: FreeShape[Fix] =
          ConcatArraysR(
            MakeArrayR(AddR(shape, IntLit(9))),
            MakeArrayR(SubtractR(freeShape(Shifting(IdOnly, struct >> shape)), IntLit(10))))

        deepShapeQS(qs) must equal(expected)
      }

      "Reduce" >> {
        val bucket: List[FreeMap] = List(ProjectIndexR(HoleF[Fix], IntLit(3)))

        val reducers: List[ReduceFunc[FreeMap]] =
          List(ReduceFuncs.Sum(ProjectFieldR(HoleF[Fix], StrLit("foobar"))))

        val repair: FreeMapA[ReduceIndex] =
          ConcatArraysR(
            MakeArrayR(AddR(ReduceIndexF(0.left), IntLit(9))),
            MakeArrayR(SubtractR(ReduceIndexF(0.right), IntLit(10))))

        val qs = Reduce(shape, bucket, reducers, repair)

        val expected: FreeShape[Fix] =
          ConcatArraysR(
            MakeArrayR(AddR(bucket(0) >> shape, IntLit(9))),
            MakeArrayR(SubtractR(freeShape(Reducing(reducers(0).map(_ >> shape))), IntLit(10))))

        deepShapeQS(qs) must equal(expected)
      }

      "Unreferenced" >> {
        deepShapeQS(Unreferenced()) must equal(freeShape[Fix](RootShape()))
      }
    }

    "Joins" >> {

      val lBranch: FreeQS =
        Free.roll(QCT.inj(LeftShift(
          Free.roll(QCT.inj(Map(
            Free.point(SrcHole),
            ProjectFieldR(HoleF, StrLit("foo"))))),
          HoleF,
          IncludeId,
          RightSideF)))

      val rBranch: FreeQS =
        Free.roll(QCT.inj(LeftShift(
          Free.roll(QCT.inj(Map(
            Free.point(SrcHole),
            ProjectFieldR(HoleF, StrLit("bar"))))),
          HoleF,
          IncludeId,
          LeftSideF)))

      val combine: JoinFunc =
        Free.roll(MFC(Add(
          ProjectIndexR(LeftSideF, IntLit(1)),
          ProjectIndexR(RightSideF, IntLit(2)))))

      "ThetaJoin" >> {

        val deepShapeTJ: Algebra[ThetaJoin, FreeShape[Fix]] =
          implicitly[DeepShape[Fix, ThetaJoin]].deepShapeƒ

        val qs = ThetaJoin(
          shape,
          lBranch,
          rBranch,
          BoolLit[Fix, JoinSide](true),
          JoinType.Inner,
          combine)

        val expected: FreeShape[Fix] =
          Free.roll(MFC(Add(
            ProjectIndexR(
              freeShape(Shifting(IncludeId, ProjectFieldR(shape, StrLit("foo")))),
              IntLit(1)),
            ProjectIndexR(
              ProjectFieldR(shape, StrLit("bar")),
              IntLit(2)))))

        deepShapeTJ(qs) must equal(expected)
      }

      "EquiJoin" >> {

        val deepShapeEJ: Algebra[EquiJoin, FreeShape[Fix]] =
          implicitly[DeepShape[Fix, EquiJoin]].deepShapeƒ

        val qs = EquiJoin(
          shape,
          lBranch,
          rBranch,
          List((BoolLit[Fix, Hole](true), BoolLit[Fix, Hole](true))),
          JoinType.Inner,
          combine)

        val expected: FreeShape[Fix] =
          Free.roll(MFC(Add(
            ProjectIndexR(
              freeShape(Shifting(IncludeId, ProjectFieldR(shape, StrLit("foo")))),
              IntLit(1)),
            ProjectIndexR(
              ProjectFieldR(shape, StrLit("bar")),
              IntLit(2)))))

        deepShapeEJ(qs) must equal(expected)
      }
    }
  }
}
