/*
 * Copyright 2014–2016 SlamData Inc.
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

import quasar.Predef._
import quasar.LogicalPlan
import quasar.fp._
import quasar.fs._
import quasar.std.StdLib._

import matryoshka._, FunctorT.ops._
import org.specs2.mutable._
import org.specs2.scalaz._
import pathy.Path._
//import shapeless.contrib.scalaz.instances.deriveEqual
import scalaz._
import Scalaz._

class QScriptSpec extends Specification with ScalazMatchers {
  import DataLevelOps._
  import MapFuncs._
  import Transform._

  def callIt(lp: Fix[LogicalPlan]): Inner[Fix] =
    lp.transCata(lpToQScript[Fix])
       .transCata(liftQSAlgebra(elideNopJoins[Fix, QScriptPure[Fix, ?]]))
       .transCata(liftQSAlgebra(elideNopMaps[Fix, QScriptPure[Fix, ?]]))
       .transCata(liftQSAlgebra2(coalesceMap[Fix, QScriptPure[Fix, ?]]))

  def RootR = CorecursiveOps[Fix, QScriptPure[Fix, ?]](E.inj(Const[DeadEnd, Inner[Fix]](Root))).embed

  def ObjectProjectR[A](src: Free[MapFunc[Fix, ?], A], field: Free[MapFunc[Fix, ?], A]): Free[MapFunc[Fix, ?], A] =
    Free.roll(ObjectProject(src, field))

  def StrR[A](s: String): Free[MapFunc[Fix, ?], A] =
    Free.roll(StrLit[Fix, Free[MapFunc[Fix, ?], A]](s))

  def lpRead(path: String): Fix[LogicalPlan] =
    LogicalPlan.Read(sandboxAbs(posixCodec.parseAbsFile(path).get))

  "replan" should {
    "convert a very simple read" in {
      callIt(lpRead("/foo")) must
      equal(
        F.inj(Map(RootR, ObjectProjectR(UnitF, StrR("foo")))).embed)
    }

    "convert a simple read" in {
      callIt(lpRead("/some/foo/bar")) must
      equal(
        F.inj(
          Map(RootR,
            ObjectProjectR(
              ObjectProjectR(
                ObjectProjectR(
                  UnitF,
                  StrR("some")),
                StrR("foo")),
              StrR("bar")))).embed)

      // Map(Root, ObjectProject(ObjectProject(ObjectProject((), "some"), "foo"), "bar"))
    }

    "convert a basic invoke" in {
      callIt(math.Add(lpRead("/foo"), lpRead("/bar")).embed) must
      equal(
        F.inj(
          Map(RootR,
            Free.roll(Add[Fix, FreeMap[Fix]](
              ObjectProjectR(UnitF, StrR("foo")),
              ObjectProjectR(UnitF, StrR("bar")))))).embed)
    }
  }
}
