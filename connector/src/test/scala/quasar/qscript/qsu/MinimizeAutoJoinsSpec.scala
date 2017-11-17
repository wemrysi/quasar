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

import quasar.{Planner, Qspec, TreeMatchers, Type}, Planner.PlannerError
import quasar.ejson.{EJson, Fixed}
import quasar.fp._
import quasar.qscript.{
  construction,
  Hole,
  HoleF,
  MapFuncsCore
}
import slamdata.Predef._

import matryoshka._
import matryoshka.data.Fix
import pathy.Path, Path.Sandboxed
import scalaz.{EitherT, Need, StateT}

object MinimizeAutoJoinsSpec extends Qspec with TreeMatchers with QSUTTypes[Fix] {
  import QSUGraph.Extractors._

  type F[A] = EitherT[StateT[Need, Long, ?], PlannerError, A]

  val qsu = QScriptUniform.Dsl[Fix]
  val func = construction.Func[Fix]
  val maj = MinimizeAutoJoins[Fix]

  val J = Fixed[Fix[EJson]]

  val afile = Path.rootDir[Sandboxed] </> Path.file("afile")

  "unary node elimination" should {
    "linearize .foo + .bar" in {
      val qgraph = QSUGraph.fromTree[Fix](
        qsu.autojoin2(
          qsu.map(
            qsu.read(afile),
            func.ProjectKey(HoleF, func.Constant(J.str("foo")))),
          qsu.map(
            qsu.read(afile),
            func.ProjectKey(HoleF, func.Constant(J.str("bar")))),
          _(MapFuncsCore.Add(_, _))))

      runOn(qgraph) must beLike {
        case Map(Read(_), fm) =>
          // must_=== doesn't work
          fm must beTreeEqual(
            func.Add(
              func.ProjectKey(HoleF, func.Constant(J.str("foo"))),
              func.ProjectKey(HoleF, func.Constant(J.str("bar")))))
      }
    }

    "convert Typecheck to a Map(_, Guard)" in {
      val qgraph = QSUGraph.fromTree[Fix](
        qsu.autojoin3(
          qsu.read(afile),
          qsu.read(afile),
          qsu.map1(
            qsu.unreferenced(),
            MapFuncsCore.Undefined[Fix, Hole](): MapFuncCore[Hole]),
          _(MapFuncsCore.Guard(_, Type.AnyObject, _, _))))

      runOn(qgraph) must beLike {
        case Map(Read(_), fm) =>
          // must_=== doesn't work
          fm must beTreeEqual(func.Guard(HoleF, Type.AnyObject, HoleF, func.Undefined))
      }
    }
  }

  def runOn(qgraph: QSUGraph): QSUGraph = {
    val results = maj[F](qgraph).run.eval(0L).value.toEither
    results must beRight

    results.right.get
  }
}
