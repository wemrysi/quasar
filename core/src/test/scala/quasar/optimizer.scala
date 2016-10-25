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
import quasar.LogicalPlan._
import quasar.frontend.LogicalPlanHelpers
import quasar.sql.CompilerHelpers
import quasar.std._, StdLib.set._, StdLib.structural._

import matryoshka._, FunctorT.ops._
import org.scalacheck._
import pathy.Path._
import scalaz.{Free => _, _}, Scalaz._
import scalaz.scalacheck.ScalaCheckBinding._
import scalaz.scalacheck.ScalazProperties._

class OptimizerSpec extends quasar.Qspec with CompilerHelpers with TreeMatchers with LogicalPlanHelpers {
  "simplify" should {

    "inline trivial binding" in {
      Optimizer.simplify[Fix](fixLet('tmp0, read("foo"), Free('tmp0))) must
        beTree(read("foo"))
    }

    "not inline binding that's used twice" in {
      Optimizer.simplify[Fix](fixLet('tmp0, read("foo"),
        makeObj(
          "bar" -> ObjectProject(Free('tmp0), fixConstant(Data.Str("bar"))),
          "baz" -> ObjectProject(Free('tmp0), fixConstant(Data.Str("baz")))))) must
        beTree(
          fixLet('tmp0, read("foo"),
            makeObj(
              "bar" -> ObjectProject(Free('tmp0), fixConstant(Data.Str("bar"))),
              "baz" -> ObjectProject(Free('tmp0), fixConstant(Data.Str("baz"))))))
    }

    "completely inline stupid lets" in {
      Optimizer.simplify[Fix](fixLet('tmp0, read("foo"), fixLet('tmp1, Free('tmp0), Free('tmp1)))) must
        beTree(read("foo"))
    }

    "inline correct value for shadowed binding" in {
      Optimizer.simplify[Fix](fixLet('tmp0, read("foo"),
        fixLet('tmp0, read("bar"),
          makeObj(
            "bar" -> ObjectProject(Free('tmp0), fixConstant(Data.Str("bar"))))))) must
        beTree(
          makeObj(
            "bar" -> ObjectProject(read("bar"), fixConstant(Data.Str("bar")))))
    }

    "inline a binding used once, then shadowed once" in {
      Optimizer.simplify[Fix](fixLet('tmp0, read("foo"),
        ObjectProject(Free('tmp0),
          fixLet('tmp0, read("bar"),
            makeObj(
              "bar" -> ObjectProject(Free('tmp0), fixConstant(Data.Str("bar")))))))) must
        beTree(
          Invoke(ObjectProject, Func.Input2(
            read("foo"),
            makeObj(
              "bar" -> ObjectProject(read("bar"), fixConstant(Data.Str("bar")))))))
    }

    "inline a binding used once, then shadowed twice" in {
      Optimizer.simplify[Fix](fixLet('tmp0, read("foo"),
        ObjectProject(Free('tmp0),
          fixLet('tmp0, read("bar"),
            makeObj(
              "bar" -> ObjectProject(Free('tmp0), fixConstant(Data.Str("bar"))),
              "baz" -> ObjectProject(Free('tmp0), fixConstant(Data.Str("baz")))))))) must
        beTree(
          Invoke(ObjectProject, Func.Input2(
            read("foo"),
            fixLet('tmp0, read("bar"),
              makeObj(
                "bar" -> ObjectProject(Free('tmp0), fixConstant(Data.Str("bar"))),
                "baz" -> ObjectProject(Free('tmp0), fixConstant(Data.Str("baz"))))))))
    }

    "partially inline a more interesting case" in {
      Optimizer.simplify[Fix](fixLet('tmp0, read("person"),
        fixLet('tmp1,
          makeObj(
            "name" -> ObjectProject(Free('tmp0), fixConstant(Data.Str("name")))),
          fixLet('tmp2,
            OrderBy[FLP](
              Free('tmp1),
              MakeArray[FLP](
                ObjectProject(Free('tmp1), fixConstant(Data.Str("name")))),
              fixConstant(Data.Str("foobar"))),
            Free('tmp2))))) must
        beTree(
          fixLet('tmp1,
            makeObj(
              "name" ->
                ObjectProject(read("person"), fixConstant(Data.Str("name")))),
            OrderBy[FLP](
              Free('tmp1),
              MakeArray[FLP](
                ObjectProject(Free('tmp1), fixConstant(Data.Str("name")))),
              fixConstant(Data.Str("foobar")))))
    }
  }

  "preferProjections" should {
    "ignore a delete with unknown shape" in {
      Optimizer.preferProjections(
        DeleteField(Read(file("zips")),
          fixConstant(Data.Str("pop")))) must
        beTree[Fix[LogicalPlan]](
          DeleteField(Read(file("zips")),
            fixConstant(Data.Str("pop"))))
    }

    "convert a delete after a projection" in {
      Optimizer.preferProjections(
        fixLet('meh, Read(file("zips")),
          DeleteField[FLP](
            makeObj(
              "city" -> ObjectProject(Free('meh), fixConstant(Data.Str("city"))),
              "pop"  -> ObjectProject(Free('meh), fixConstant(Data.Str("pop")))),
            fixConstant(Data.Str("pop"))))) must
      beTree(
        fixLet('meh, Read(file("zips")),
          makeObj(
            "city" ->
              ObjectProject(
                makeObj(
                  "city" ->
                    ObjectProject(Free('meh), fixConstant(Data.Str("city"))),
                  "pop" ->
                    ObjectProject(Free('meh), fixConstant(Data.Str("pop")))),
                fixConstant(Data.Str("city"))))))
    }

    "convert a delete when the shape is hidden by a Free" in {
      Optimizer.preferProjections(
        fixLet('meh, Read(file("zips")),
          fixLet('meh2,
            makeObj(
              "city" -> ObjectProject(Free('meh), fixConstant(Data.Str("city"))),
              "pop"  -> ObjectProject(Free('meh), fixConstant(Data.Str("pop")))),
            makeObj(
              "orig" -> Free('meh2),
              "cleaned" ->
                DeleteField(Free('meh2), fixConstant(Data.Str("pop"))))))) must
      beTree(
        fixLet('meh, Read(file("zips")),
          fixLet('meh2,
            makeObj(
              "city" -> ObjectProject(Free('meh), fixConstant(Data.Str("city"))),
              "pop"  -> ObjectProject(Free('meh), fixConstant(Data.Str("pop")))),
            makeObj(
              "orig" -> Free('meh2),
              "cleaned" ->
                makeObj(
                  "city" ->
                    ObjectProject(Free('meh2), fixConstant(Data.Str("city"))))))))
    }
  }

  "Component" should {
    implicit def componentArbitrary[A: Arbitrary]: Arbitrary[Optimizer.Component[A]] =
      Arbitrary(Arbitrary.arbitrary[A]) ∘ (Optimizer.NeitherCond(_))

    implicit def ArbComponentInt: Arbitrary[Optimizer.Component[Int]] =
      componentArbitrary[Int]

    implicit def ArbComponentInt2Int: Arbitrary[Optimizer.Component[Int => Int]] =
      componentArbitrary[Int => Int]

    // FIXME this test isn't really testing much at this point because
    // we cannot test the equality of two functions
    implicit def EqualComponent: Equal[Optimizer.Component[Int]] = new Equal[Optimizer.Component[Int]] {
      def equal(a1: Optimizer.Component[Int], a2: Optimizer.Component[Int]): Boolean = true
    }

    "obey applicative laws" in {
      applicative.laws[Optimizer.Component]
    }
  }
}
