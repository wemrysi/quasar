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

package quasar.qscript

import slamdata.Predef._
import quasar.common.{JoinType, SortDir}
import quasar.contrib.matryoshka._
import quasar.contrib.matryoshka.arbitrary._
import quasar.fp._
import quasar.fp.ski.κ
import quasar.ejson.{EJson, EJsonArbitrary}
import quasar.ejson.implicits._
import quasar.qscript.analysis.Outline

import matryoshka.data.Fix
import matryoshka.data.free._
import matryoshka.implicits._
import org.specs2.scalacheck._
import pathy.Path._
import scalaz._, Scalaz._

final class PreferProjectionSpec extends quasar.Qspec with QScriptHelpers {
  import EJsonArbitrary._
  import PreferProjection.{projectComplement, preferProjection}

  implicit val params = Parameters(maxSize = 10)

  "projectComplement" >> {
    "replace key deletion of statically known structure with projection of complement" >> prop {
      (k1: Fix[EJson], k2: Fix[EJson], k3: Fix[EJson]) => (k1 =/= k2 && k2 =/= k3 && k1 =/= k3) ==> {

      val m =
        ConcatMapsR(
          ConcatMapsR(
            MakeMapR(ConstantR(k1), HoleF),
            MakeMapR(ConstantR(k2), HoleF)),
          MakeMapR(ConstantR(k3), HoleF))

      val in =
        DeleteKeyR(m, ConstantR(k2))

      val out =
        ConcatMapsR(
          MakeMapR(ConstantR(k1), HoleF),
          MakeMapR(ConstantR(k3), HoleF))

      projectComplement(in)(κ(Outline.unknownF)) must_= out
    }}

    "preserve key deletion when structure isn't statically known" >> prop {
      (k1: Fix[EJson], k2: Fix[EJson], k3: Fix[EJson]) => (k1 =/= k2 && k2 =/= k3 && k1 =/= k3) ==> {

      val m =
        ConcatMapsR(
          ConcatMapsR(
            MakeMapR(ConstantR(k1), HoleF),
            MakeMapR(ConstantR(k2), HoleF)),
          HoleF)

      val in =
        DeleteKeyR(m, ConstantR(k2))

      projectComplement(in)(κ(Outline.unknownF)) must_= in
    }}

    "preserve key deletion when any key not statically known" >> prop {
      (k1: Fix[EJson], k2: Fix[EJson], v1: Fix[EJson]) => (k1 =/= k2) ==> {

      val m =
        ConcatMapsR(
          ConcatMapsR(
            MakeMapR(ConstantR(k1), HoleF),
            MakeMapR(ConstantR(k2), HoleF)),
          MakeMapR(ProjectIndexR(HoleF, ConstantR(ejsonInt(1))), ConstantR(v1)))

      val in =
        DeleteKeyR(m, ConstantR(k2))

      projectComplement(in)(κ(Outline.unknownF)) must_= in
    }}
  }

  "preferProjection" >> {
    val base: Fix[QS] =
      QC(LeftShift(
        ReadR(rootDir </> dir("foo") </> file("bar")).embed,
        HoleF,
        ExcludeId,
        MapFuncCore.StaticMap(List(
          ejsonStr("a") -> ProjectIndexR(RightSideF, ConstantR(ejsonInt(0))),
          ejsonStr("b") -> ProjectIndexR(RightSideF, ConstantR(ejsonInt(2))),
          ejsonStr("c") -> ProjectIndexR(RightSideF, ConstantR(ejsonInt(5))))))).embed

    def keyF[A](name: String): FreeMapA[A] =
      ConstantR[A](ejsonStr(name))

    def prjFrom[A](src: FreeMapA[A], key: String, keys: String*): FreeMapA[A] =
      MapFuncCore.StaticMap((key :: keys.toList) map { name =>
        ejsonStr(name) -> ProjectKeyR(src, keyF(name))
      })

    "Map" >> {
      val q =
        QC(Map(
          base,
          DeleteKeyR(HoleF, keyF("b")))).embed

      val e =
        QC(Map(
          base,
          prjFrom(HoleF, "a", "c"))).embed

      preferProjection[QS](q) must_= e
    }

    "LeftShift" >> {
      val q =
        QC(LeftShift(
          base,
          DeleteKeyR(HoleF, keyF("c")),
          IncludeId,
          MapFuncCore.StaticArray(List(
            DeleteKeyR(DeleteKeyR(LeftSideF, keyF("a")), keyF("b")),
            RightSideF)))).embed

      val e =
        QC(LeftShift(
          base,
          prjFrom(HoleF, "a", "b"),
          IncludeId,
          MapFuncCore.StaticArray(List(
            prjFrom(LeftSideF, "c"),
            RightSideF)))).embed

      preferProjection[QS](q) must_= e
    }

    "Reduce" >> {
      import ReduceFuncs._

      val q =
        QC(Reduce(
          base,
          List(DeleteKeyR(HoleF, keyF("a"))),
          List(Count(HoleF), First(DeleteKeyR(HoleF, keyF("b")))),
          MapFuncCore.StaticArray(List(
            ReduceIndexF(0.right),
            DeleteKeyR(ReduceIndexF(1.right), keyF("a")),
            DeleteKeyR(ReduceIndexF(0.left), keyF("c")))))).embed

      val e =
        QC(Reduce(
          base,
          List(prjFrom(HoleF, "b", "c")),
          List(Count(HoleF), First(prjFrom(HoleF, "a", "c"))),
          MapFuncCore.StaticArray(List(
            ReduceIndexF(0.right),
            prjFrom(ReduceIndexF(1.right), "c"),
            prjFrom(ReduceIndexF(0.left), "b"))))).embed

      preferProjection[QS](q) must_= e
    }

    "Sort" >> {
      val q =
        QC(Sort(
          base,
          List(DeleteKeyR(DeleteKeyR(HoleF, keyF("a")), keyF("c"))),
          NonEmptyList((DeleteKeyR(HoleF, keyF("b")), SortDir.Ascending)))).embed

      val e =
        QC(Sort(
          base,
          List(prjFrom(HoleF, "b")),
          NonEmptyList((prjFrom(HoleF, "a", "c"), SortDir.Ascending)))).embed

      preferProjection[QS](q) must_= e
    }

    "Filter" >> {
      val q =
        QC(Filter(
          base,
          EqR(DeleteKeyR(HoleF, keyF("a")), HoleF))).embed

      val e =
        QC(Filter(
          base,
          EqR(prjFrom(HoleF, "b", "c"), HoleF))).embed

      preferProjection[QS](q) must_= e
    }

    "Subset" >> {
      val q =
        QC(Subset(
          base,
          Free.roll(QCT(Map(
            HoleQS,
            DeleteKeyR(HoleF, keyF("c"))))),
          Take,
          Free.roll(QCT(Reduce(
            HoleQS,
            List(),
            List(ReduceFuncs.First(DeleteKeyR(HoleF, keyF("a")))),
            DeleteKeyR(ReduceIndexF(0.right), keyF("b"))))))).embed

      val e =
        QC(Subset(
          base,
          Free.roll(QCT(Map(
            HoleQS,
            prjFrom(HoleF, "a", "b")))),
          Take,
          Free.roll(QCT(Reduce(
            HoleQS,
            List(),
            List(ReduceFuncs.First(prjFrom(HoleF, "b", "c"))),
            prjFrom(ReduceIndexF(0.right), "c")))))).embed

      preferProjection[QS](q) must_= e
    }

    "ThetaJoin" >> {
      val q =
        TJ(ThetaJoin(
          base,
          Free.roll(QCT(Map(
            HoleQS,
            DeleteKeyR(HoleF, keyF("a"))))),
          Free.roll(QCT(Map(
            HoleQS,
            DeleteKeyR(HoleF, keyF("b"))))),
          EqR(
            DeleteKeyR(LeftSideF, keyF("b")),
            DeleteKeyR(RightSideF, keyF("a"))),
          JoinType.Inner,
          MapFuncCore.StaticMap(List(
            ejsonStr("left") -> DeleteKeyR(LeftSideF, keyF("c")),
            ejsonStr("right") -> DeleteKeyR(RightSideF, keyF("c")))))).embed

      val e =
        TJ(ThetaJoin(
          base,
          Free.roll(QCT(Map(
            HoleQS,
            prjFrom(HoleF, "b", "c")))),
          Free.roll(QCT(Map(
            HoleQS,
            prjFrom(HoleF, "a", "c")))),
          EqR(
            prjFrom(LeftSideF, "c"),
            prjFrom(RightSideF, "c")),
          JoinType.Inner,
          MapFuncCore.StaticMap(List(
            ejsonStr("left") -> prjFrom(LeftSideF, "b"),
            ejsonStr("right") -> prjFrom(RightSideF, "a"))))).embed

      preferProjection[QS](q) must_= e
    }
  }
}
