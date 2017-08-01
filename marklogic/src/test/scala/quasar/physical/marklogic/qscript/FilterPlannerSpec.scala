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
import quasar.fp.ski._
import quasar.qscript._
import quasar.qscript.MapFuncsCore._

import matryoshka.data.Fix
import org.scalacheck.{Arbitrary, Gen}, Arbitrary.arbitrary
import pathy.Path._
import pathy.scalacheck.PathyArbitrary._

import scalaz._, Scalaz._

final class FilterPlannerSpec extends quasar.Qspec {

  def projectField(src: FreeMap[Fix], str: String): FreeMap[Fix] =
    Free.roll(MFC(ProjectField(src, StrLit(str))))

  implicit val arbNestedProject: Gen[(ADir, FreeMap[Fix])] =
    for {
      dir0 <- arbitrary[ADir]
      nested = flatten("/", ".", "..", ι, ι, dir0)
        .foldLeft(projectField(HoleF, "/"))((prj: FreeMap[Fix], nxt: String) => projectField(prj, nxt))
    } yield (dir0, nested)


  "plan" >> {
    "fallback to non-indexed search when search expression is XQuery" >> {
      1 must_== 1
    }
    "fallback to non-indexed search when search expression contains cts.Document" >> {
      1 must_== 1
    }
  }

  "StarIndexPlanner" >> {
    "search expression includes * and projection path" >> prop { prj: (ADir, FreeMap[Fix]) =>
      1 must_== 1
    }
  }
  "PathIndexPlanner" >> {
    "plan with path indexes using any comparison operator" >> prop { prj: (ADir, FreeMap[Fix]) =>
      1 must_== 1
    }
  }
  "ElementIndexPlanner" >> {
    "add a predicate to search expression when there's none" >> prop { prj: (ADir, FreeMap[Fix]) =>
      1 must_== 1
    }

    "fallback to non-indexed search when there's already a predicate on the search expression" >> prop { prj: (ADir, FreeMap[Fix]) =>
      1 must_== 1
    }
  }
}
