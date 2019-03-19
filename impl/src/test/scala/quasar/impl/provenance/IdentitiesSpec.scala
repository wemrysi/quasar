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

package quasar.impl.provenance

import slamdata.Predef.{Int, None, Set, Some, StringContext}

import quasar.Qspec
import quasar.qscript.provenance.CatsNonEmptyListGenerator

import scala.Predef.$conforms

import cats.data.NonEmptyList
import cats.instances.int._
import cats.instances.option._
import cats.kernel.laws.discipline.{SemilatticeTests, EqTests}
import cats.syntax.eq._
import cats.syntax.foldable._
import cats.syntax.list._

import org.specs2.matcher._
import org.specs2.mutable.SpecificationLike
import org.specs2.scalacheck.Parameters

import org.typelevel.discipline.specs2.mutable.Discipline

import shims._

object IdentitiesSpec extends Qspec
    with SpecificationLike
    with IdentitiesGenerator
    with CatsNonEmptyListGenerator
    with Discipline {

  implicit val params = Parameters(maxSize = 10)

  type Vecs = NonEmptyList[NonEmptyList[NonEmptyList[Int]]]

  def beEquivalentToDistinct(vecs: Vecs): Matcher[Identities[Int]] =
    new Matcher[Identities[Int]] {
      def apply[S <: Identities[Int]](ex: Expectable[S]) = {
        val exp = ex.value.expanded
        val distinct = vecs.distinct

        def x = distinct.forall(v => exp.exists(_ eqv v))
        def y = exp.forall(v => distinct.exists(_ eqv v))

        result(x && y, "are equivalent", s"are not equivalent\nexpected: ${distinct.show}\nactual: ${exp.show}", ex)
      }
    }

  def vecs(v: NonEmptyList[Int], vs: NonEmptyList[Int]*): Vecs =
    NonEmptyList.of(v, vs: _*).map(_.map(NonEmptyList.one(_)))

  "roundtrips distinct vectors" >> prop { vecs: Vecs =>
    Identities.collapsed(vecs) must beEquivalentToDistinct(vecs)
  }

  "breadth is number of distinct vectors" >> prop { vecs: Vecs =>
    Identities.collapsed(vecs).breadth must_=== vecs.distinct.length
  }

  "depth is length of longest vector" >> prop { vecs: Vecs =>
    val depth = vecs.map(_.length).maximumOption getOrElse 0
    Identities.collapsed(vecs).depth must_=== depth
  }

  "init" >> {
    "undefined on singleton vector" >> {
      Identities(42).init must beNone
    }

    "elides singleton vectors from set" >> prop { (x: Int, y: Int, ys: NonEmptyList[Int]) =>
      val a = Identities(x)
      val b = Identities.fromReducible(ys)

      a.merge(b :+ y).init eqv Some(b)
    }

    "removes the last conjoined regions" >> prop { ids: Identities[Int] =>
      val exp = ids.expanded.toList.flatMap(_.init.toNel.toList).toNel

      exp match {
        case Some(xs) =>
          ids.init must beSome(beEquivalentToDistinct(xs))

        case None =>
          ids.init must beNone
      }
    }
  }

  "merge" >> {
    "elides duplicates" >> prop { ints: NonEmptyList[Int] =>
      val ids = Identities.fromReducible(ints)
      (ids merge ids).expanded === NonEmptyList.one(ints.map(NonEmptyList.one(_)))
    }

    "preserves differences" >> prop { (ints: NonEmptyList[Int], x: Int, y: Int) => (x =!= y) ==> {
      val vs = vecs(
        ints ::: x :: ints,
        ints ::: y :: ints)

      val ids = Identities.collapsed(vs)

      ids must beEquivalentToDistinct(vs)
      ids.storageSize must_=== (2 * ints.length + 2)
    }}

    "preserves subsumes efficiently" >> prop { (ints: NonEmptyList[Int], x: Int) =>
      val vs = vecs(ints :+ x, ints)
      val ids = Identities.collapsed(vs)

      ids must beEquivalentToDistinct(vs)
      ids.storageSize must_=== (ints.length + 1)
    }

    "preserves subsumed efficiently" >> prop { (ints: NonEmptyList[Int], x: Int) =>
      val vs = vecs(ints, ints :+ x)
      val ids = Identities.collapsed(vs)

      ids must beEquivalentToDistinct(vs)
      ids.storageSize must_=== (ints.length + 1)
    }

    "treats conj and snoc distinctly" >> prop { (ints: NonEmptyList[Int], x: Int) =>
      val ids = Identities.fromReducible(ints)
      val a = ids :+ x
      val b = ids :≻ x

      val conj = {
        val r = ints.reverse.map(NonEmptyList.one(_))
        NonEmptyList(r.head :+ x, r.tail).reverse
      }

      val exp =
        NonEmptyList.of(
          (ints :+ x).map(NonEmptyList.one(_)),
          conj)

      a.merge(b) must beEquivalentToDistinct(exp)
    }

    "is space efficient" >> prop { (init: NonEmptyList[Int], endh: Int, endt: Set[Int]) =>
      val ends = NonEmptyList.fromListUnsafe((endt + endh).toList)
      val vecs = ends.map(i => (init :+ i :+ init.head).map(NonEmptyList.one(_)))

      Identities.collapsed(vecs).storageSize must_=== (init.length + ends.length + 1)
    }

    "shares common prefix subgraphs" >> {
      val vs = vecs(
        NonEmptyList.of(1, 2, 6, 7, 8),
        NonEmptyList.of(3, 4, 6, 7, 8),

        NonEmptyList.of(1, 2, 8, 9, 10),
        NonEmptyList.of(3, 4, 8, 9, 10))

      val ids = Identities.collapsed(vs)

      ids must beEquivalentToDistinct(vs)
      ids.storageSize must_=== 10
    }.pendingUntilFixed("actual size is 11")

    "shares common suffix subgraphs" >> {
      val vs = vecs(
        NonEmptyList.of(1, 2, 3, 4, 5),
        NonEmptyList.of(8, 9, 3, 4, 5))

      val ids = Identities.collapsed(vs)

      ids must beEquivalentToDistinct(vs)
      ids.storageSize must_=== 7
    }

    "merge diverge merge" >> {
      val vs = vecs(
        NonEmptyList.of(1, 2, 3, 4, 5),
        NonEmptyList.of(1, 2, 6, 4, 5))

      val ids = Identities.collapsed(vs)

      ids must beEquivalentToDistinct(vs)
      ids.storageSize must_=== 6
    }

    "properly backtracks to avoid inappropriate cartesians" >> {
      // Asserts that the '3's are not merged as doing so would
      // result in 4 paths instead of just these two.
      val vs = vecs(
        NonEmptyList.of(1, 2, 3, 4, 5),
        NonEmptyList.of(5, 4, 3, 2, 1))

      val ids = Identities.collapsed(vs)

      ids must beEquivalentToDistinct(vs)
      ids.storageSize must_=== 10
    }

    "split minimal vertices at each level when backtracking" >> {
      // At level 3, 'c' and 'z' are merge candidates, however
      // the paths after 'c' diverge, so they should not be merged
      // whereas the paths after 'z' converge so we can safely merge
      // them.
      val vs = vecs(
        NonEmptyList.of(1, 2, 3, 4, 5),
        NonEmptyList.of(7, 8, 9, 4, 5),

        NonEmptyList.of(11, 12, 3, 13, 5),
        NonEmptyList.of(15, 17, 9, 4, 5))

      val ids = Identities.collapsed(vs)

      ids must beEquivalentToDistinct(vs)
      ids.storageSize must_=== 14
    }

    "maximally merge when multiple options" >> {
      val vs = vecs(
        NonEmptyList.of(1, 2, 3, 4, 5),

        NonEmptyList.of(6, 7, 3, 4, 8),

        NonEmptyList.of(9, 10, 3, 4, 8))

      val ids = Identities.collapsed(vs)

      ids must beEquivalentToDistinct(vs)
      ids.storageSize must_=== 12
    }

    "build appropriate cartesians" >> {
      val vs = vecs(
        NonEmptyList.of(1, 2, 6, 7, 8),
        NonEmptyList.of(3, 4, 6, 7, 8),

        NonEmptyList.of(1, 2, 6, 9, 10),
        NonEmptyList.of(3, 4, 6, 9, 10))

      val ids = Identities.collapsed(vs)

      ids must beEquivalentToDistinct(vs)
      ids.storageSize must_=== 9
    }

    "avoid converging when one side longer than the other" >> {
      val vs = vecs(
        NonEmptyList.of(1, 2, 3, 4),
        NonEmptyList.of(6, 7, 3))

      val ids = Identities.collapsed(vs)

      ids must beEquivalentToDistinct(vs)
      ids.storageSize must_=== 7
    }

    "avoid converging when one side is longer and multiple convergence" >> {
      val vs = vecs(
        NonEmptyList.of(1, 2, 3, 4, 5, 6),
        NonEmptyList.of(6, 7, 3, 4, 5))

      val ids = Identities.collapsed(vs)

      ids must beEquivalentToDistinct(vs)
      ids.storageSize must_=== 11
    }

    "continue a merge when multiple candidates exist" >> {
      val vs = vecs(
        NonEmptyList.of(1, 2, 3, 4, 5),
        NonEmptyList.of(7, 2, 6, 8, 10),
        NonEmptyList.of(7, 2, 11, 13, 15))

      val ids = Identities.collapsed(vs)

      ids must beEquivalentToDistinct(vs)
      ids.storageSize must_=== 13
    }
  }

  "submerge" >> {
    "properly replaces shared root node" >> {
      val vs = NonEmptyList.of(
        NonEmptyList.one(NonEmptyList.of(0, 1)),
        NonEmptyList.of(NonEmptyList.of(0, 1, 2), NonEmptyList.of(3, 4)))

      val exp = NonEmptyList.of(
        NonEmptyList.of(NonEmptyList.one(9), NonEmptyList.of(0, 1)),
        NonEmptyList.of(NonEmptyList.of(0, 1, 2), NonEmptyList.one(9), NonEmptyList.of(3, 4)))

      val ids = Identities.collapsed(vs)

      ids.submerge(9) must beEquivalentToDistinct(exp)
    }

    "insert a value just before the final region" >> prop { (vs: Vecs, x: Int) =>
      val exp = vs.map(_.reverse match {
        case NonEmptyList(l, i) => NonEmptyList(l, NonEmptyList.one(x) :: i).reverse
      })

      Identities.collapsed(vs).submerge(x) must beEquivalentToDistinct(exp)
    }
  }

  "equality" >> {
    "singleton components differentiate when all multi are equal" >> {
      val as = vecs(
        NonEmptyList.of(1),
        NonEmptyList.of(2),
        NonEmptyList.of(3, 4, 5))

      val bs = vecs(
        NonEmptyList.of(1),
        NonEmptyList.of(7),
        NonEmptyList.of(3, 4, 5))

      Identities.collapsed(as) =!= Identities.collapsed(bs)
    }
  }

  checkAll("Eq[Identities[Int]]", EqTests[Identities[Int]].eqv)
  checkAll("Semilattice[Identities[Int]]", SemilatticeTests[Identities[Int]].semilattice)
}
