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

import quasar.Qspec
import quasar.contrib.iota._
import quasar.contrib.pathy._
import quasar.ejson.{EJson, Fixed}
import quasar.fp._
import quasar.qscript._

import iotaz.{CopK, TNilK}
import iotaz.TListK.:::
import matryoshka.data.Fix
import matryoshka.implicits._
import pathy.Path._
import scalaz.Const

object OptimizeSpec extends Qspec {

  type QSBase =
    QScriptCore[Fix, ?]          :::
    EquiJoin[Fix, ?]             :::
    Const[ShiftedRead[AFile], ?] :::
    TNilK

  // initial QScript
  type QS[A] = CopK[QSBase, A]

  implicit val QS: Injectable[QS, QScriptTotal[Fix, ?]] =
    SubInject[QS, QScriptTotal[Fix, ?]]

  val qsDsl = construction.mkDefaults[Fix, QS]
  val fix = qsDsl.fix
  val recFunc = qsDsl.recFunc

  // initial QScript with ExtraShiftedRead
  type QSExtra[A] = CopK[Const[ExtraShiftedRead[AFile], ?] ::: QSBase, A]

  implicit val QSExtra: Injectable[QSExtra, QScriptTotal[Fix, ?]] =
    SubInject[QSExtra, QScriptTotal[Fix, ?]]

  val qsExtraDsl = construction.mkDefaults[Fix, QSExtra]
  val fixE = qsExtraDsl.fix
  val recFuncE = qsExtraDsl.recFunc
  val funcE = qsExtraDsl.func

  val ejs = Fixed[Fix[EJson]]

  val optimize = new Optimize[Fix]

  "ExtraLeftShift rewrite" >> {

    val extraShiftFunc: QSExtra[Fix[QSExtra]] => QSExtra[Fix[QSExtra]] =
      liftFG[QScriptCore[Fix, ?], QSExtra, Fix[QSExtra]](optimize.extraShift[QSExtra, AFile])

    def extraShift(expr: Fix[QSExtra]): Fix[QSExtra] =
      expr.transCata[Fix[QSExtra]](extraShiftFunc)

    "rewrite when the read is shifted at the top-level and only the shifted values are referenced" >> {
      "with IncludeId" >> {
        val initial: Fix[QSExtra] =
          fixE.LeftShift(
            fixE.ShiftedRead[AFile](rootDir </> file("foo"), ExcludeId),
            recFuncE.Hole,
            IncludeId, // IncludeId
            ShiftType.Map,
            OnUndefined.Omit,
            funcE.ConcatMaps(
              funcE.MakeMapS("k1", funcE.ProjectIndexI(funcE.RightSide, 0)),
              funcE.MakeMapS("v1", funcE.ProjectIndexI(funcE.RightSide, 1))))

        val expected: Fix[QSExtra] =
          fixE.Map(
            fixE.ExtraShiftedRead[AFile](
              rootDir </> file("foo"),
              IncludeId,
              ShiftKey(ShiftedKey)),
            recFuncE.ConcatMaps(
              recFuncE.MakeMapS("k1",
                recFuncE.ProjectIndexI(recFuncE.ProjectKeyS(recFuncE.Hole, ShiftedKey), 0)),
              recFuncE.MakeMapS("v1",
                recFuncE.ProjectIndexI(recFuncE.ProjectKeyS(recFuncE.Hole, ShiftedKey), 1))))

        extraShift(initial) must equal(expected)
      }

      "with IdOnly" >> {
        val initial: Fix[QSExtra] =
          fixE.LeftShift(
            fixE.ShiftedRead[AFile](rootDir </> file("foo"), ExcludeId),
            recFuncE.Hole,
            IdOnly, // IdOnly
            ShiftType.Map,
            OnUndefined.Omit,
            funcE.MakeMapS("k1", funcE.RightSide))

        val expected: Fix[QSExtra] =
          fixE.Map(
            fixE.ExtraShiftedRead[AFile](
              rootDir </> file("foo"),
              IdOnly,
              ShiftKey(ShiftedKey)),
            recFuncE.MakeMapS("k1",
              recFuncE.ProjectKeyS(recFuncE.Hole, ShiftedKey)))

        extraShift(initial) must equal(expected)
      }

      "with ExcludeId" >> {
        val initial: Fix[QSExtra] =
          fixE.LeftShift(
            fixE.ShiftedRead[AFile](rootDir </> file("foo"), ExcludeId),
            recFuncE.Hole,
            ExcludeId, // ExcludeId
            ShiftType.Map,
            OnUndefined.Omit,
            funcE.MakeMapS("v1", funcE.RightSide))

        val expected: Fix[QSExtra] =
          fixE.Map(
            fixE.ExtraShiftedRead[AFile](
              rootDir </> file("foo"),
              ExcludeId,
              ShiftKey(ShiftedKey)),
            recFuncE.MakeMapS("v1",
              recFuncE.ProjectKeyS(recFuncE.Hole, ShiftedKey)))

        extraShift(initial) must equal(expected)
      }
    }

    "not rewrite when the shift source is not Hole" >> {
      val initial: Fix[QSExtra] =
        fixE.LeftShift(
          fixE.ShiftedRead[AFile](rootDir </> file("foo"), ExcludeId),
          recFuncE.ProjectKeyS(recFunc.Hole, "xyz"), // shift source is not Hole
          IncludeId,
          ShiftType.Map,
          OnUndefined.Omit,
          funcE.ConcatMaps(
            funcE.MakeMapS("k1", funcE.ProjectIndexI(funcE.RightSide, 0)),
            funcE.MakeMapS("v1", funcE.ProjectIndexI(funcE.RightSide, 1))))

      extraShift(initial) must equal(initial)
    }

    "not rewrite when the non-shifted data is referenced (via LeftSide)" >> {
      val initial: Fix[QSExtra] =
        fixE.LeftShift(
          fixE.ShiftedRead[AFile](rootDir </> file("foo"), ExcludeId),
          recFunc.Hole,
          IncludeId,
          ShiftType.Map,
          OnUndefined.Emit,
          funcE.ConcatMaps(
            funcE.MakeMapS("k1", funcE.ProjectIndexI(funcE.RightSide, 0)),
            funcE.MakeMapS("v1", funcE.LeftSide))) // LeftSide is referenced

      extraShift(initial) must equal(initial)
    }
  }

  "no-op Map rewrite" >> {

    val elideNoopMapFunc: QS[Fix[QS]] => QS[Fix[QS]] =
      liftFG[QScriptCore[Fix, ?], QS, Fix[QS]](optimize.elideNoopMap[QS])

    def elideNoopMap(expr: Fix[QS]): Fix[QS] =
      expr.transCata[Fix[QS]](elideNoopMapFunc)

    "elide outer no-op Map" >> {
      val src: Fix[QS] =
        fix.ShiftedRead[AFile](rootDir </> file("foo"), ExcludeId)

      elideNoopMap(fix.Map(src, recFunc.Hole)) must equal(src)
    }

    "elide nested no-op Map" >> {
      val src: Fix[QS] =
        fix.Map(
          fix.ShiftedRead[AFile](rootDir </> file("foo"), ExcludeId),
          recFunc.ProjectKeyS(recFunc.Hole, "bar"))

      val qs: Fix[QS] =
        fix.Filter(
          fix.Map(src, recFunc.Hole),
          recFunc.ProjectKeyS(recFunc.Hole, "baz"))

      val expected: Fix[QS] =
        fix.Filter(
          src,
          recFunc.ProjectKeyS(recFunc.Hole, "baz"))

      elideNoopMap(qs) must equal(expected)
    }

    "elide double no-op Map" >> {
      val src: Fix[QS] =
        fix.ShiftedRead[AFile](rootDir </> file("foo"), ExcludeId)

      elideNoopMap(fix.Map(fix.Map(src, recFunc.Hole), recFunc.Hole)) must equal(src)
    }
  }
}
