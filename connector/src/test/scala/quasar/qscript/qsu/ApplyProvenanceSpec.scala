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

import slamdata.Predef.{Map => SMap, _}

import quasar.Qspec
import quasar.Planner.PlannerError
import quasar.contrib.pathy.AFile
import quasar.ejson.{EJson, Fixed}
import quasar.qscript.construction
import quasar.qscript.provenance.Dimensions
import quasar.qscript.HoleF
import quasar.qscript.MapFuncsCore.IntLit

import matryoshka.data.Fix
import org.specs2.matcher.{Expectable, Matcher, MatchResult}
import pathy.Path, Path.{file, Sandboxed}
import scalaz.{\/, Cofree, IList}

object ApplyProvenanceSpec extends Qspec with QSUTTypes[Fix] {

  import ApplyProvenance.AuthenticatedQSU

  type F[A] = PlannerError \/ A
  type QSU[A] = QScriptUniform[A]

  val J = Fixed[Fix[EJson]]

  val qsu = QScriptUniform.AnnotatedDsl[Fix, String]
  val opt = QScriptUniform.Optics[Fix]
  val func = construction.Func[Fix]

  val app = ApplyProvenance[Fix]
  val qprov = QProv[Fix]

  val root = Path.rootDir[Sandboxed]
  val afile: AFile = root </> file("foobar")

  "provenance application" should {

    "produce provenance for map" in {

      val fm: FreeMap = func.Add(HoleF, IntLit(17))

      val tree: Cofree[QSU, String] =
        qsu.map("name0",
          (qsu.read("name1", afile), fm))

      val dims: SMap[String, Dimensions[qprov.P]] = SMap(
        "name0" -> IList(qprov.prov.proj(J.str("foobar"))),
        "name1" -> IList(qprov.prov.proj(J.str("foobar"))))

      tree must haveDimensions(dims)
    }
  }

  // checks the expected dimensions
  // checks that the graph has not changed
  def haveDimensions(expected: SMap[String, Dimensions[qprov.P]])
      : Matcher[Cofree[QSU, String]] =
    new Matcher[Cofree[QSU, String]] {
      def apply[S <: Cofree[QSU, String]](s: Expectable[S]): MatchResult[S] = {

        val (renames, inputGraph): (QSUGraph.Renames, QSUGraph) =
          QSUGraph.fromAnnotatedTree[Fix](s.value.map(v => Some(Symbol(v))))

	// TODO map over values too
        val expectedDims: QSUDims[Fix] =
	  expected.map { case (k, v) => (renames(Symbol(k)), v) }

	val actual: PlannerError \/ AuthenticatedQSU[Fix] = app[F](inputGraph)

	// TODO use Show
        actual.bimap[MatchResult[S], MatchResult[S]](
        { err =>
          failure(s"provenance application produced unexpected planner error: ${err}", s)
        },
        { case auth @ AuthenticatedQSU(resultGraph, resultDims) =>
          result(
            (resultGraph == inputGraph) && (resultDims == expectedDims), // TODO better equals
            s"received expected authenticated QSU:\n${auth}",
            s"received unexpected authenticated QSU:\n${auth}\nexpected:\n${expected}",
            s)
        }).merge
      }
    }
}
