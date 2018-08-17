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

package quasar.qsu

import slamdata.Predef.{Map => SMap, _}

import quasar.Qspec
import quasar.contrib.pathy.AFile
import quasar.ejson.{EJson, Fixed}
import quasar.ejson.implicits._
import quasar.fp._
import quasar.contrib.iota._
import quasar.qscript.{construction, PlannerError, ReduceFuncs}
import quasar.qscript.MapFuncsCore.RecIntLit
import quasar.qscript.provenance.Dimensions

import matryoshka._
import matryoshka.data.Fix
import org.specs2.matcher.{Expectable, Matcher, MatchResult}
import pathy.Path, Path.{file, Sandboxed}
import scalaz.{\/, Cofree}
import scalaz.syntax.equal._
import scalaz.syntax.show._
import scalaz.std.list._
import scalaz.std.map._

object ApplyProvenanceSpec extends Qspec with QSUTTypes[Fix] {

  import ApplyProvenance.AuthenticatedQSU
  import QScriptUniform.{DTrans, Retain, Rotation}

  type F[A] = PlannerError \/ A
  type QSU[A] = QScriptUniform[A]

  val J = Fixed[Fix[EJson]]

  val qsu = QScriptUniform.AnnotatedDsl[Fix, Symbol]
  val func = construction.Func[Fix]
  val recFunc = construction.RecFunc[Fix]

  val app = ApplyProvenance[Fix, F] _
  val qprov = QProv[Fix]
  val P = qprov.prov

  val root = Path.rootDir[Sandboxed]
  val afile: AFile = root </> file("foobar")

  import P.implicits._

  "provenance application" should {

    "produce provenance for map" in {
      val fm: RecFreeMap = recFunc.Add(recFunc.Hole, RecIntLit(17))

      val tree: Cofree[QSU, Symbol] =
        qsu.map('name0,
          (qsu.read('name1, afile), fm))

      val dims: SMap[Symbol, QDims] = SMap(
        'name0 -> Dimensions.origin(P.prjPath(J.str("foobar"))),
        'name1 -> Dimensions.origin(P.prjPath(J.str("foobar"))))

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
            recFunc.ProjectKeyS(recFunc.Hole, "pop"))),
          ReduceFuncs.Sum(())))

      tree must haveDimensions(SMap(
        'n4 -> Dimensions.origin(
          P.value(IdAccess.bucket('n4, 1))
        , P.value(IdAccess.bucket('n4, 0)))
      , 'n3 -> Dimensions.origin(
          P.prjValue(J.str("pop")) ≺: P.prjPath(J.str("foobar"))
        , P.value(IdAccess.groupKey('n2, 1))
        , P.value(IdAccess.groupKey('n2, 0)))
      , 'n2 -> Dimensions.origin(
          P.prjPath(J.str("foobar"))
        , P.value(IdAccess.groupKey('n2, 1))
        , P.value(IdAccess.groupKey('n2, 0)))
      ))
    }

    "compute provenance for squash" >> {
      val tree =
        qsu.dimEdit('n0, (
          qsu.map('n1, (
            qsu.transpose('n2, (
              qsu.read('n3, afile),
              Retain.Values,
              Rotation.ShiftMap)),
            recFunc.Add(
              recFunc.Constant(J.int(7)),
              recFunc.ProjectKeyS(recFunc.Hole, "bar")))),
          DTrans.Squash[Fix]()))

      tree must haveDimensions(SMap(
        'n0 -> Dimensions.origin(
          P.thenn(
            P.value(IdAccess.identity('n2))
          , P.prjPath(J.str("foobar"))))
      , 'n2 -> Dimensions.origin(
          P.value(IdAccess.identity('n2))
        , P.prjPath(J.str("foobar")))
      , 'n3 -> Dimensions.origin(
          P.prjPath(J.str("foobar")))
      ))
    }
  }

  "left shift provenance" >> {
    "flatten with trivial struct and repair is dimensional flatten" >> todo

    "shift with trivial struct and repair is dimensional shift" >> todo

    "include id affects provenance" >> todo

    "struct affects provenance prior to shift" >> todo

    "repair affects provenance after shift" >> todo

    "non-structure preserving result in repair results in shift provenance" >> todo
  }

  "MapFunc provenance" >> {
    "make map injects" >> todo

    "make array injects" >> todo

    "project key projects" >> todo

    "project index projects" >> todo

    "concat map joins" >> todo

    "concat array joins, updating rhs injects" >> todo

    "delete key is identity" >> todo

    "if undefined is identity" >> todo

    "filtering cond is identity" >> todo

    "two-branch cond is union" >> todo

    "filtering guard is identity" >> todo

    "two branch guard is union" >> todo

    "typecheck is identity" >> todo
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
            s"received unexpected authenticated QSU:\n${aqsu.shows}\n" +
            s"expected:\n[\n${printMultiline(expected.toList)}\n]",
            s)
        }).merge
      }
    }
}
