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
import quasar.sql.CompilerHelpers
import quasar.std._, StdLib.set._, StdLib.structural._

import matryoshka._, FunctorT.ops._
import org.scalacheck._
import pathy.Path._
import scalaz.{Free => _, _}, Scalaz._
import scalaz.scalacheck.ScalaCheckBinding._
import scalaz.scalacheck.ScalazProperties._

class OptimizerSpec extends quasar.Qspec with CompilerHelpers with TreeMatchers {
  import quasar.frontend.fixpoint.lpf

  "simplify" should {

    "inline trivial binding" in {
      Optimizer.simplify[Fix](lpf.let('tmp0, read("foo"), lpf.free('tmp0))) must
        beTree(read("foo"))
    }

    "not inline binding that's used twice" in {
      Optimizer.simplify[Fix](lpf.let('tmp0, read("foo"),
        makeObj(
          "bar" -> ObjectProject(lpf.free('tmp0), lpf.constant(Data.Str("bar"))),
          "baz" -> ObjectProject(lpf.free('tmp0), lpf.constant(Data.Str("baz")))))) must
        beTree(
          lpf.let('tmp0, read("foo"),
            makeObj(
              "bar" -> ObjectProject(lpf.free('tmp0), lpf.constant(Data.Str("bar"))),
              "baz" -> ObjectProject(lpf.free('tmp0), lpf.constant(Data.Str("baz"))))))
    }

    "completely inline stupid lets" in {
      Optimizer.simplify[Fix](lpf.let('tmp0, read("foo"), lpf.let('tmp1, lpf.free('tmp0), lpf.free('tmp1)))) must
        beTree(read("foo"))
    }

    "inline correct value for shadowed binding" in {
      Optimizer.simplify[Fix](lpf.let('tmp0, read("foo"),
        lpf.let('tmp0, read("bar"),
          makeObj(
            "bar" -> ObjectProject(lpf.free('tmp0), lpf.constant(Data.Str("bar"))))))) must
        beTree(
          makeObj(
            "bar" -> ObjectProject(read("bar"), lpf.constant(Data.Str("bar")))))
    }

    "inline a binding used once, then shadowed once" in {
      Optimizer.simplify[Fix](lpf.let('tmp0, read("foo"),
        ObjectProject(lpf.free('tmp0),
          lpf.let('tmp0, read("bar"),
            makeObj(
              "bar" -> ObjectProject(lpf.free('tmp0), lpf.constant(Data.Str("bar")))))))) must
        beTree(
          lpf.invoke(ObjectProject, Func.Input2(
            read("foo"),
            makeObj(
              "bar" -> ObjectProject(read("bar"), lpf.constant(Data.Str("bar")))))))
    }

    "inline a binding used once, then shadowed twice" in {
      Optimizer.simplify[Fix](lpf.let('tmp0, read("foo"),
        ObjectProject(lpf.free('tmp0),
          lpf.let('tmp0, read("bar"),
            makeObj(
              "bar" -> ObjectProject(lpf.free('tmp0), lpf.constant(Data.Str("bar"))),
              "baz" -> ObjectProject(lpf.free('tmp0), lpf.constant(Data.Str("baz")))))))) must
        beTree(
          lpf.invoke(ObjectProject, Func.Input2(
            read("foo"),
            lpf.let('tmp0, read("bar"),
              makeObj(
                "bar" -> ObjectProject(lpf.free('tmp0), lpf.constant(Data.Str("bar"))),
                "baz" -> ObjectProject(lpf.free('tmp0), lpf.constant(Data.Str("baz"))))))))
    }

    "partially inline a more interesting case" in {
      Optimizer.simplify[Fix](lpf.let('tmp0, read("person"),
        lpf.let('tmp1,
          makeObj(
            "name" -> ObjectProject(lpf.free('tmp0), lpf.constant(Data.Str("name")))),
          lpf.let('tmp2,
            OrderBy[FLP](
              lpf.free('tmp1),
              MakeArray[FLP](
                ObjectProject(lpf.free('tmp1), lpf.constant(Data.Str("name")))),
              lpf.constant(Data.Str("foobar"))),
            lpf.free('tmp2))))) must
        beTree(
          lpf.let('tmp1,
            makeObj(
              "name" ->
                ObjectProject(read("person"), lpf.constant(Data.Str("name")))),
            OrderBy[FLP](
              lpf.free('tmp1),
              MakeArray[FLP](
                ObjectProject(lpf.free('tmp1), lpf.constant(Data.Str("name")))),
              lpf.constant(Data.Str("foobar")))))
    }
  }

  "preferProjections" should {
    "ignore a delete with unknown shape" in {
      Optimizer.preferProjections(
        DeleteField(lpf.read(file("zips")),
          lpf.constant(Data.Str("pop")))) must
        beTree[Fix[LogicalPlan]](
          DeleteField(lpf.read(file("zips")),
            lpf.constant(Data.Str("pop"))))
    }

    "convert a delete after a projection" in {
      Optimizer.preferProjections(
        lpf.let('meh, lpf.read(file("zips")),
          DeleteField[FLP](
            makeObj(
              "city" -> ObjectProject(lpf.free('meh), lpf.constant(Data.Str("city"))),
              "pop"  -> ObjectProject(lpf.free('meh), lpf.constant(Data.Str("pop")))),
            lpf.constant(Data.Str("pop"))))) must
      beTree(
        lpf.let('meh, lpf.read(file("zips")),
          makeObj(
            "city" ->
              ObjectProject(
                makeObj(
                  "city" ->
                    ObjectProject(lpf.free('meh), lpf.constant(Data.Str("city"))),
                  "pop" ->
                    ObjectProject(lpf.free('meh), lpf.constant(Data.Str("pop")))),
                lpf.constant(Data.Str("city"))))))
    }

    "convert a delete when the shape is hidden by a lpf.free" in {
      Optimizer.preferProjections(
        lpf.let('meh, lpf.read(file("zips")),
          lpf.let('meh2,
            makeObj(
              "city" -> ObjectProject(lpf.free('meh), lpf.constant(Data.Str("city"))),
              "pop"  -> ObjectProject(lpf.free('meh), lpf.constant(Data.Str("pop")))),
            makeObj(
              "orig" -> lpf.free('meh2),
              "cleaned" ->
                DeleteField(lpf.free('meh2), lpf.constant(Data.Str("pop"))))))) must
      beTree(
        lpf.let('meh, lpf.read(file("zips")),
          lpf.let('meh2,
            makeObj(
              "city" -> ObjectProject(lpf.free('meh), lpf.constant(Data.Str("city"))),
              "pop"  -> ObjectProject(lpf.free('meh), lpf.constant(Data.Str("pop")))),
            makeObj(
              "orig" -> lpf.free('meh2),
              "cleaned" ->
                makeObj(
                  "city" ->
                    ObjectProject(lpf.free('meh2), lpf.constant(Data.Str("city"))))))))
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
