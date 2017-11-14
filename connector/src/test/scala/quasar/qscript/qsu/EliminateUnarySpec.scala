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

package quasar.qscript.qsu

import quasar.{Data, Qspec}
import quasar.qscript.{Hole, LeftSide, MapFuncsCore, RightSide, SrcHole, Take}
import quasar.qscript.qsu.{QScriptUniform => QSU}

import matryoshka.data.Fix
import scalaz.Inject

object EliminateUnarySpec extends Qspec with QSUTTypes[Fix] {
  import QSUGraph.Extractors._

  val qsu = QScriptUniform.Dsl[Fix]
  val elim = EliminateUnary[Fix]

  val IC = Inject[MapFuncCore, MapFunc]

  "unary node elimination" should {
    "eliminate a single unary node" in {
      val qgraph = QSUGraph.fromTree[Fix](
        qsu.unary(qsu.unreferenced(), IC(MapFuncsCore.Negate(SrcHole: Hole))))

      elim(qgraph) must beLike {
        case Map(Unreferenced(), _) => ok
      }
    }

    "eliminate unary within a read" in {
      val qgraph = QSUGraph.fromTree[Fix](qsu.tread1("foo"))

      elim(qgraph) must beLike {
        case AutoJoin2C(
          Transpose(Read(_), QSU.Rotation.ShiftMap),
          DataConstantMapped(Data.Int(_)),
          MapFuncsCore.ProjectIndex(LeftSide, RightSide)) => ok
      }
    }

    "eliminate unary around and within a read" in {
      val qgraph = QSUGraph.fromTree[Fix](
        qsu.unary(qsu.tread1("foo"), IC(MapFuncsCore.Negate(SrcHole: Hole))))

      elim(qgraph) must beLike {
        case Map(
          AutoJoin2C(
            Transpose(Read(_), QSU.Rotation.ShiftMap),
            DataConstantMapped(Data.Int(_)),
            MapFuncsCore.ProjectIndex(LeftSide, RightSide)),
          FMFC1(MapFuncsCore.Negate(SrcHole))) => ok
      }
    }

    "eliminate unary around and within a read within a subset" in {
      val qgraph = QSUGraph.fromTree[Fix](
        qsu.subset(
          qsu.unary(
            qsu.tread1("foo"),
            IC(MapFuncsCore.Negate(SrcHole: Hole))),
          Take,
          qsu.cint(11)))

      elim(qgraph) must beLike {
        case Subset(
          Map(
            AutoJoin2C(
              Transpose(Read(_), QSU.Rotation.ShiftMap),
              DataConstantMapped(Data.Int(_)),
              MapFuncsCore.ProjectIndex(LeftSide, RightSide)),
            FMFC1(MapFuncsCore.Negate(SrcHole))),
          Take,
          DataConstantMapped(Data.Int(i))) => i mustEqual 11
      }
    }
  }
}
