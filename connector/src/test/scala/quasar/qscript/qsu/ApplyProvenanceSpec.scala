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
import quasar.ejson.implicits._
import quasar.fp._
import quasar.qscript.construction
import quasar.qscript.{HoleF, ReduceFuncs}
import quasar.qscript.MapFuncsCore.IntLit

import matryoshka._
import matryoshka.data.Fix
import org.specs2.matcher.{Expectable, Matcher, MatchResult}
import pathy.Path, Path.{file, Sandboxed}
import scalaz.{\/, Cofree, Equal, IList}
import scalaz.syntax.equal._
import scalaz.syntax.show._
import scalaz.std.map._

object ApplyProvenanceSpec extends Qspec with QSUTTypes[Fix] {

  import ApplyProvenance.AuthenticatedQSU
  import QScriptUniform.DTrans

  type F[A] = PlannerError \/ A
  type QSU[A] = QScriptUniform[A]

  val J = Fixed[Fix[EJson]]

  val qsu = QScriptUniform.AnnotatedDsl[Fix, Symbol]
  val func = construction.Func[Fix]

  val app = ApplyProvenance[Fix, F] _
  val qprov = QProv[Fix]

  val root = Path.rootDir[Sandboxed]
  val afile: AFile = root </> file("foobar")

  // FIXME: Figure out how to get this to resolve normally.
  implicit val eqP: Equal[qprov.P] =
    qprov.prov.provenanceEqual(Equal[qprov.D], Equal[QIdAccess])

  "provenance application" should {

    "produce provenance for map" in {

      val fm: FreeMap = func.Add(HoleF, IntLit(17))

      val tree: Cofree[QSU, Symbol] =
        qsu.map('name0,
          (qsu.read('name1, afile), fm))

      val dims: SMap[Symbol, QDims] = SMap(
        'name0 -> IList(qprov.prov.proj(J.str("foobar"))),
        'name1 -> IList(qprov.prov.proj(J.str("foobar"))))

      tree must haveDimensions(dims)
    }

    "compute correct provenance nested dimEdits" >> {
      val tree =
        qsu.lpReduce('n4, (
          qsu.map('n3, (
            qsu.dimEdit('n2, (
              qsu.dimEdit('n1, (
                qsu.read('n0, afile),
                DTrans.Group(func.ProjectKeyS(func.Hole, "x")))),
              DTrans.Group(func.ProjectKeyS(func.Hole, "y")))),
            func.ProjectKeyS(func.Hole, "pop"))),
          ReduceFuncs.Sum(())))

      tree must haveDimensions(SMap(
        'n4 -> IList(
          qprov.prov.value(IdAccess.bucket('n4, 1))
        , qprov.prov.value(IdAccess.bucket('n4, 0)))
      , 'n3 -> IList(
          qprov.prov.proj(J.str("foobar"))
        , qprov.prov.value(IdAccess.groupKey('n2, 1))
        , qprov.prov.value(IdAccess.groupKey('n2, 0)))
      , 'n2 -> IList(
          qprov.prov.proj(J.str("foobar"))
        , qprov.prov.value(IdAccess.groupKey('n2, 1))
        , qprov.prov.value(IdAccess.groupKey('n2, 0)))
      ))
    }
  }

  // checks the expected dimensions
  def haveDimensions(expected: SMap[Symbol, QDims])
      : Matcher[Cofree[QSU, Symbol]] =
    new Matcher[Cofree[QSU, Symbol]] {
      def apply[S <: Cofree[QSU, Symbol]](s: Expectable[S]): MatchResult[S] = {

        val (renames, inputGraph): (QSUGraph.Renames, QSUGraph) =
          QSUGraph.fromAnnotatedTree[Fix](s.value.map(Some(_)))

        val expectedDims: SMap[Symbol, QDims] =
          expected.map { case (k, v) =>
            val newP = renames.foldLeft(v)((p, t) => qprov.rename(t._1, t._2, p))
            (renames(k), newP)
          }

        val actual: PlannerError \/ AuthenticatedQSU[Fix] = app(inputGraph)

        actual.bimap[MatchResult[S], MatchResult[S]](
        { err =>
          failure(s"provenance application produced unexpected planner error: ${err}", s)
        },
        { case aqsu @ AuthenticatedQSU(resultGraph, qauth) =>
          result(
            qauth.dims ≟ expectedDims,
            s"received expected authenticated QSU:\n${aqsu.shows}",
            s"received unexpected authenticated QSU:\n${aqsu.shows}\nexpected:\n[\n${printMultiline(expected)}\n]",
            s)
        }).merge
      }
    }
}
