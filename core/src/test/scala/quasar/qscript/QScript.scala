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

import matryoshka._, FunctorT.ops._
import org.specs2.mutable._
import org.specs2.scalaz._
import pathy.Path._
import shapeless.contrib.scalaz.instances._
import scalaz._

class QScriptSpec extends Specification with ScalazMatchers {
  import DataLevelOps._
  import MapFuncs._
  import Transform._

  implicit val ma: Mergeable.Aux[Fix, QScriptPure[Fix, Unit]] = scala.Predef.implicitly

  def callIt(lp: Fix[LogicalPlan]): Inner[Fix] = lp.transCata(lpToQScript[Fix])

  def RootR = CorecursiveOps[Fix, QScriptPure[Fix, ?]](E.inj(Const[DeadEnd, Inner[Fix]](Root))).embed

  def ObjectProjectR[A](src: Free[MapFunc[Fix, ?], A], field: Free[MapFunc[Fix, ?], A]): Free[MapFunc[Fix, ?], A] =
    Free.roll(ObjectProject(src, field))

  def StrR[A](s: String): Free[MapFunc[Fix, ?], A] =
    Free.roll(StrLit[Fix, Free[MapFunc[Fix, ?], A]](s))

  "replan" should {
    "convert a simple read" in {
      callIt(quasar.LogicalPlan.Read(file("/some/foo/bar"))) must
      equal(
        F.inj(
          Map(RootR,
            ObjectProjectR(ObjectProjectR(ObjectProjectR(UnitF, StrR("some")),
                                          StrR("foo")),
                           StrR("bar")))))

      // Map(Root, ObjectProject(ObjectProject(ObjectProject((), "some"), "foo"), "bar"))
    }
  }
}
