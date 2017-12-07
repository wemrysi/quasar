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

import slamdata.Predef._
import quasar.Qspec
import quasar.Planner.PlannerError
import quasar.contrib.matryoshka._
import quasar.contrib.pathy.AFile
import quasar.ejson.{EJson, Fixed}
import quasar.ejson.implicits._
import quasar.fp._
import quasar.qscript.construction

import matryoshka._
import matryoshka.data._
import matryoshka.data.free._
import pathy.Path
import scalaz.{\/, \/-, EitherT, Need, StateT}

object ExtractFreeMapSpec extends Qspec with QSUTTypes[Fix] {
  import QScriptUniform.DTrans
  import QSUGraph.Extractors._

  type F[A] = EitherT[StateT[Need, Long, ?], PlannerError, A]
  type QSU[A] = QScriptUniform[A]

  val ejs = Fixed[Fix[EJson]]
  val qsu = QScriptUniform.DslT[Fix]
  val func = construction.Func[Fix]

  val orders: AFile = Path.rootDir </> Path.dir("client") </> Path.file("orders")

  def extractFM(graph: QSUGraph) = ExtractFreeMap[Fix, F](graph)

  "extracting mappable region" should {

    "convert mappable filter" >> {
      val predicate = func.ProjectKey(func.Hole, func.Constant(ejs.str("foo")))

      val graph = QSUGraph.fromTree[Fix](
        qsu.lpFilter(
          qsu.read(orders),
          qsu.map(qsu.read(orders), predicate)))

      evaluate(extractFM(graph)) must beLike {
        case \/-(QSFilter(Read(`orders`), fm)) => fm must_= predicate
      }
    }

    "convert mappable group key" >> {
      val predicate = func.ProjectKey(func.Hole, func.Constant(ejs.str("foo")))

      val graph = QSUGraph.fromTree[Fix](
        qsu.groupBy(
          qsu.read(orders),
          qsu.map(qsu.read(orders), predicate)))

      evaluate(extractFM(graph)) must beLike {
        case \/-(DimEdit(Read(`orders`), DTrans.Group(fm))) => fm must_= predicate
      }
    }
  }

  def evaluate[A](fa: F[A]): PlannerError \/ A = fa.run.eval(0L).value
}
