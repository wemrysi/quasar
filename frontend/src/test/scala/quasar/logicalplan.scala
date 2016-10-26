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

package quasar

import quasar.Predef._
import quasar.fp._

import matryoshka.Fix
import org.scalacheck._
import org.specs2.scalaz._
import scalaz._, Scalaz._
import scalaz.scalacheck.ScalazProperties._
import shapeless.contrib.scalaz.instances._
import pathy.Path._

class LogicalPlanSpecs extends Spec with frontend.LogicalPlanHelpers {
  import LogicalPlan._

  implicit val arbLogicalPlan: Arbitrary ~> λ[α => Arbitrary[LogicalPlan[α]]] =
    new (Arbitrary ~> λ[α => Arbitrary[LogicalPlan[α]]]) {
      def apply[α](arb: Arbitrary[α]): Arbitrary[LogicalPlan[α]] =
        Arbitrary {
          Gen.oneOf(readGen[α], addGen(arb), constGen[α], letGen(arb), freeGen[α](Nil))
        }
    }

  // Switch this to parameterize over Funcs as well
  def addGen[A: Arbitrary]: Gen[LogicalPlan[A]] = for {
    l <- Arbitrary.arbitrary[A]
    r <- Arbitrary.arbitrary[A]
  } yield Invoke(std.MathLib.Add, Func.Input2(l, r))

  def letGen[A: Arbitrary]: Gen[LogicalPlan[A]] = for {
    n            <- Gen.choose(0, 1000)
    (form, body) <- Arbitrary.arbitrary[(A, A)]
  } yield Let(Symbol("tmp" + n), form, body)

  def readGen[A]: Gen[LogicalPlan[A]] = Gen.const(Read(rootDir </> file("foo")))

  import DataArbitrary._

  def constGen[A]: Gen[LogicalPlan[A]] = for {
    data <- Arbitrary.arbitrary[Data]
  } yield Constant(data)

  def freeGen[A](vars: List[Symbol]): Gen[LogicalPlan[A]] = for {
    n <- Gen.choose(0, 1000)
  } yield Free(Symbol("tmp" + n))

  implicit val arbIntLP = arbLogicalPlan(Arbitrary.arbInt)

  checkAll(traverse.laws[LogicalPlan])

  import quasar.std.StdLib._, relations._, quasar.std.StdLib.set._, structural._, math._

  "normalizeTempNames" should {
    "rename simple nested lets" in {
      LogicalPlan.normalizeTempNames(
        fixLet('foo, fixRead(file("foo")),
          fixLet('bar, fixRead(file("bar")),
            Fix(MakeObjectN(
              fixConstant(Data.Str("x")) -> Fix(ObjectProject(fixFree('foo), fixConstant(Data.Str("x")))),
              fixConstant(Data.Str("y")) -> Fix(ObjectProject(fixFree('bar), fixConstant(Data.Str("y"))))))))) must_==
        fixLet('__tmp0, fixRead(file("foo")),
          fixLet('__tmp1, fixRead(file("bar")),
            Fix(MakeObjectN(
              fixConstant(Data.Str("x")) -> Fix(ObjectProject(fixFree('__tmp0), fixConstant(Data.Str("x")))),
              fixConstant(Data.Str("y")) -> Fix(ObjectProject(fixFree('__tmp1), fixConstant(Data.Str("y"))))))))
    }

    "rename shadowed name" in {
      LogicalPlan.normalizeTempNames(
        fixLet('x, fixRead(file("foo")),
          fixLet('x, Fix(MakeObjectN(
              fixConstant(Data.Str("x")) -> Fix(ObjectProject(fixFree('x), fixConstant(Data.Str("x")))))),
            fixFree('x)))) must_==
        fixLet('__tmp0, fixRead(file("foo")),
          fixLet('__tmp1, Fix(MakeObjectN(
              fixConstant(Data.Str("x")) -> Fix(ObjectProject(fixFree('__tmp0), fixConstant(Data.Str("x")))))),
            fixFree('__tmp1)))
    }
  }

  "normalizeLets" should {
    "re-nest" in {
      LogicalPlan.normalizeLets(
        fixLet('bar,
          fixLet('foo,
            fixRead(file("foo")),
            Fix(Filter(fixFree('foo), Fix(Eq(Fix(ObjectProject(fixFree('foo), fixConstant(Data.Str("x")))), fixConstant(Data.Str("z"))))))), 
          Fix(MakeObjectN(
            fixConstant(Data.Str("y")) -> Fix(ObjectProject(fixFree('bar), fixConstant(Data.Str("y")))))))) must_==
        fixLet('foo,
          fixRead(file("foo")),
          fixLet('bar,
            Fix(Filter(fixFree('foo), Fix(Eq(Fix(ObjectProject(fixFree('foo), fixConstant(Data.Str("x")))), fixConstant(Data.Str("z")))))), 
            Fix(MakeObjectN(
              fixConstant(Data.Str("y")) -> Fix(ObjectProject(fixFree('bar), fixConstant(Data.Str("y"))))))))
    }

    "re-nest deep" in {
      LogicalPlan.normalizeLets(
        fixLet('baz,
          fixLet('bar,
            fixLet('foo,
              fixRead(file("foo")),
              Fix(Filter(fixFree('foo), Fix(Eq(Fix(ObjectProject(fixFree('foo), fixConstant(Data.Str("x")))), fixConstant(Data.Int(0))))))),
            Fix(Filter(fixFree('bar), Fix(Eq(Fix(ObjectProject(fixFree('foo), fixConstant(Data.Str("y")))), fixConstant(Data.Int(1))))))),
          Fix(MakeObjectN(
            fixConstant(Data.Str("z")) -> Fix(ObjectProject(fixFree('bar), fixConstant(Data.Str("z")))))))) must_==
        fixLet('foo,
          fixRead(file("foo")),
          fixLet('bar,
            Fix(Filter(fixFree('foo), Fix(Eq(Fix(ObjectProject(fixFree('foo), fixConstant(Data.Str("x")))), fixConstant(Data.Int(0)))))),
            fixLet('baz,
              Fix(Filter(fixFree('bar), Fix(Eq(Fix(ObjectProject(fixFree('foo), fixConstant(Data.Str("y")))), fixConstant(Data.Int(1)))))),
              Fix(MakeObjectN(
                fixConstant(Data.Str("z")) -> Fix(ObjectProject(fixFree('bar), fixConstant(Data.Str("z")))))))))
    }

    "hoist multiple fixLets" in {
      LogicalPlan.normalizeLets(
        Fix(Add(
          fixLet('x, fixConstant(Data.Int(0)), Fix(Add(fixFree('x), fixConstant(Data.Int(1))))),
          fixLet('y, fixConstant(Data.Int(2)), Fix(Add(fixFree('y), fixConstant(Data.Int(3)))))))) must_==
        fixLet('x, fixConstant(Data.Int(0)),
          fixLet('y, fixConstant(Data.Int(2)),
            Fix(Add(
              Fix(Add(fixFree('x), fixConstant(Data.Int(1)))),
              Fix(Add(fixFree('y), fixConstant(Data.Int(3))))))))
    }

    "hoist deep fixLet by one level" in {
      LogicalPlan.normalizeLets(
        Fix(Add(
          fixConstant(Data.Int(0)),
          Fix(Add(
            fixLet('x, fixConstant(Data.Int(1)), Fix(Add(fixFree('x), fixConstant(Data.Int(2))))),
            fixConstant(Data.Int(3))))))) must_==
        Fix(Add(
          fixConstant(Data.Int(0)),
          fixLet('x, fixConstant(Data.Int(1)),
            Fix(Add(
              Fix(Add(fixFree('x), fixConstant(Data.Int(2)))),
              fixConstant(Data.Int(3)))))))
    }
  }
}
