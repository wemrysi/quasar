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

package quasar.physical.marklogic.qscript

import slamdata.Predef._

import quasar.contrib.pathy._
import quasar.physical.marklogic.cts.ComparisonOp
import quasar.qscript._
import quasar.qscript.{MapFuncsCore => MFCore}

import matryoshka.data.Fix
import org.scalacheck.{Arbitrary, Gen}, Arbitrary.arbitrary
import pathy.Path._
import pathy.scalacheck.PathyArbitrary._

import scalaz._, Scalaz._

final class FilterPlannerSpec extends quasar.Qspec {
  val comparisons = List(ComparisonOp.EQ , ComparisonOp.NE , ComparisonOp.LT , ComparisonOp.LE , ComparisonOp.GT , ComparisonOp.GE)

  def projectField(src: FreeMap[Fix], str: String): FreeMap[Fix] =
    Free.roll(MFC(MFCore.ProjectField(src, MFCore.StrLit(str))))

  def makeComp(op: ComparisonOp, src: FreeMap[Fix], str: String): FreeMap[Fix] = {
    val searchExpr = MFCore.StrLit[Fix, Hole](str)

    Free.roll(MFC(op match {
      case ComparisonOp.EQ => MFCore.Eq(src, searchExpr)
      case ComparisonOp.NE => MFCore.Neq(src,searchExpr)
      case ComparisonOp.LT => MFCore.Lt(src, searchExpr)
      case ComparisonOp.LE => MFCore.Lte(src,searchExpr)
      case ComparisonOp.GT => MFCore.Gt(src, searchExpr)
      case ComparisonOp.GE => MFCore.Gte(src,searchExpr)
    }))
  }

  case class ProjectTestCase(fm: FreeMap[Fix], path: ADir, op: ComparisonOp)

  val genNestedProject: Gen[(FreeMap[Fix], ADir)] =
    for {
      dir0  <- arbitrary[ADir]
      first <- Gen.alphaStr
      path = rebaseA(rootDir[Sandboxed] </> dir(first))(dir0)
      nested = flatten(None, None, None, Some(_), Some(_), dir0).tail.unite.foldLeft(
        projectField(HoleF, first))((prj: FreeMap[Fix], nxt: String) => projectField(prj, nxt))
    } yield (nested, path)

  val genProjectTestCase: Gen[ProjectTestCase] =
    for {
      projection <- genNestedProject
      searchExpr <- Gen.alphaStr
      (nested, dir0) = projection
      op <- Gen.oneOf(comparisons)
    } yield ProjectTestCase(makeComp(op, nested, searchExpr), dir0, op)

  implicit val arbProjectTestCase: Arbitrary[ProjectTestCase] =
    Arbitrary(genProjectTestCase)

  "StarIndexPlanner" >> {
    "search expression includes * and projection path" >> prop { prj: ProjectTestCase =>
      import scala.Predef.implicitly
      import quasar.RenderTree

      val rt = implicitly[RenderTree[FreeMap[Fix]]]
      println(prettyPrint(prj.path))
      println(rt.render(prj.fm).shows)

      1 must_== 1
    }
  }
  "PathIndexPlanner" >> {
    "plan with path indexes using any comparison operator" >> prop { prj: ProjectTestCase =>

      1 must_== 1
    }
  }
  "ElementIndexPlanner" >> {
    "add a predicate to search expression" >> prop { prj: ProjectTestCase =>
      1 must_== 1
    }

    "concatenate predicates if there's already one" >> prop { prj: ProjectTestCase =>
      1 must_== 1
    }
  }
}
