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

/*
package quasar.qscript.provenance

import slamdata.Predef.{Boolean, Char, Int, Option}
import quasar.contrib.matryoshka.arbitrary._
import quasar.Qspec

import scala.Predef.implicitly
import scala.util.{Either, Left, Right}

import matryoshka.data.Fix

import monocle.std.either.stdLeft

import org.scalacheck.{Arbitrary, Properties}

import org.specs2.scalacheck._

import scalaz.{@@, IList, NonEmptyList, Writer, Validation => V, Show}
import scalaz.Tags.Conjunction
import scalaz.scalacheck.ScalazProperties.{equal => eql, semigroup, semilattice}
import scalaz.std.anyVal._
import scalaz.std.either._
import scalaz.std.option._
import scalaz.std.tuple._
import scalaz.syntax.equal._
import scalaz.syntax.show._

final class ProvSpec extends Qspec with ProvFGenerator {

  implicit val params = Parameters(maxSize = 10)

  import ProvSpec.I
  import ProvSpec.P.implicits._

  val P = ProvSpec.P

  val T: Boolean = true
  val F: Boolean = false

  val p1 = P.project('x', F)
  val p2 = P.project('y', F)
  val p3 = P.project('z', F)

  val pk1 = P.project('a', T)
  val pk2 = P.project('b', T)

  val ik1 = P.inject('a', T)
  val ik2 = P.inject('b', T)
  val ik3 = P.inject('c', T)

  val v1i: I = Right(1)
  val v1 = P.inflate(v1i, T)
  val v2i: I = Right(2)
  val v2 = P.inflate(v2i, T)
  val v3i: I = Right(3)
  val v3 = P.inflate(v3i, T)
  val v4i: I = Right(4)
  val v4 = P.inflate(v4i, T)

  def autojoined(a: Fix[P.PF], b: Fix[P.PF]): (JoinKeys[I], Boolean) =
    P.autojoined[Writer[JoinKeys[I], ?]](a, b).run

  "apply projection" >> {
    "∏(k) ≺ I(k) ≺ p == p" >> {
      P.applyProjection(P.thenn(pk1, P.thenn(ik1, p1))) must_= V.success(Option(p1))
    }

    "∏(k) ≺ {I(k) ∧ p} ≺ q == q" >> {
      P.applyProjection(P.thenn(pk1, P.thenn(P.both(ik1, p2), p1))) must_= V.success(Option(p1))
    }

    "∏(k) ≺ {I(k) ∧ (∃ ≺ q)} ≺ r == q ≺ r" >> {
      val fp = P.fresh()
      val tree = P.thenn(pk1, P.thenn(P.both(ik1, P.thenn(fp, p2)), p1))
      P.applyProjection(tree) must_= V.success(Option(P.thenn(p2, p1)))
    }

    "∏(k) ≺ {I(j) ∧ (I(k) ≺ q)} ≺ r == q ≺ r" >> {
      val tree = P.thenn(pk1, P.thenn(P.both(ik2, P.thenn(ik1, p2)), p1))
      P.applyProjection(tree) must_= V.success(Option(P.thenn(p2, p1)))
    }

    "∏(k) ≺ {I(k) ∧ (I(j) ≺ q)} ≺ r == r" >> {
      val tree = P.thenn(pk1, P.thenn(P.both(ik1, P.thenn(ik2, p2)), p1))
      P.applyProjection(tree) must_= V.success(Option(p1))
    }

    "∏(k) ≺ I(j) ≺ q == Failure" >> prop { (k: Char, j: Char, p: Fix[P.PF]) => (k =/= j) ==> {
      val tree = P.thenn(P.project(k, T), P.thenn(P.inject(j, T), p))
      P.applyProjection(tree) must beFailure
    }}

    "∏(k :: T) ≺ I(k :: F) ≺ q == Failure" >> prop { (k: Char, p: Fix[P.PF]) =>
      val tree = P.thenn(P.project(k, T), P.thenn(P.inject(k, F), p))
      P.applyProjection(tree) must beFailure
    }

    "∏(k) ≺ {p ∧ q} ≺ r == id" >> {
      val tree = P.thenn(pk1, P.thenn(P.both(p3, p2), p1))
      P.applyProjection(tree).map(_.shows) must beSuccess(Option(tree).shows)
    }

    "∏(k) ≺ {I(j) ∧ I(k)} ≺ r == r" >> {
      val tree = P.thenn(pk1, P.thenn(P.both(ik2, ik1), p1))
      P.applyProjection(tree) must_= V.success(Option(p1))
    }

    "∏(k) ≺ {I(k) ∧ I(j)} ≺ r == r" >> {
      val tree = P.thenn(pk1, P.thenn(P.both(ik1, ik2), p1))
      P.applyProjection(tree) must_= V.success(Option(p1))
    }

    "∏(k) ≺ {I(j) ∧ I(l) ≺ q} ≺ r == Failure" >> {
      val tree = P.thenn(pk1, P.thenn(P.both(ik2, P.thenn(ik3, p2)), p1))
      P.applyProjection(tree) must beFailure
    }

    "∏(k) ≺ {I(j) ∧ p ≺ q} ≺ r == ∏(k) ≺ p ≺ q ≺ r" >> {
      val tree = P.thenn(pk1, P.thenn(P.both(ik2, P.thenn(v1, p2)), p1))
      P.applyProjection(tree) must_= V.success(Option(P.thenn(pk1, P.thenn(v1, P.thenn(p2, p1)))))
    }

    "multiple projects on nested conjunctions" >> {
      //{I(l) ≺ I(0) ≺ 1 ∧ I(r)} ≺ 1)
      val x = P.thenn(P.inject('r', T), v1)
      val y = P.both(P.thenn(P.inject('l', T), P.thenn(P.inject('0', T), v1)), x)

      val lhs = P.thenn(P.inject('l', T), y)
      val rhs = P.thenn(P.inject('r', T), v1)

      // I(l) ≺ {{I(l) ≺ I(0) ≺ 1 ∧ I(r) ≺ 1} ∧ I(r)} ≺ 1
      val tree = P.both(lhs, rhs)

      val prjL = P.applyProjection(P.thenn(P.project('l', T), tree))
      val prjLR = prjL.map(_.map(l => P.applyProjection(P.thenn(P.project('r', T), l))))

      prjL must_= V.success(Option(y))

      prjLR must_= V.success(Option(V.success(Option(v1))))
    }
  }

  "conjunction" >> {
    "(z ≺ y ≺ x) ∧ (w ≺ y ≺ x) == (z ∧ w) ≺ y ≺ x" >> {
      val l = pk1 ≺: p2 ≺: p1
      val r = pk2 ≺: p2 ≺: p1
      (l ∧ r) must_= P.both(pk1, pk2) ≺: p2 ≺: p1
    }

    "({z ∧ q} ≺ y ≺ x) ∧ ({w ∧ q} ≺ y ≺ x) == {z ∧ w ∧ q} ≺ y ≺ x" >> {
      val l = (pk1 ∧ v1) ≺: p2 ≺: p1
      val r = (pk2 ∧ v1) ≺: p2 ≺: p1
      (l ∧ r) must_= P.both(pk1, P.both(pk2, v1)) ≺: p2 ≺: p1
    }

    "({a ∧ b} ≺ y ≺ x) ∧ ({c ∧ d} ≺ y ≺ x) == {a ∧ b ∧ c ∧ d} ≺ y ≺ x" >> {
      val l = (pk1 ∧ v1) ≺: p2 ≺: p1
      val r = (pk2 ∧ v2) ≺: p2 ≺: p1
      (l ∧ r) must_= P.both(v2, P.both(pk1, P.both(pk2, v1))) ≺: p2 ≺: p1
    }

    "(a ≺ b ≺ x) ∧ (c ≺ d ≺ x) == {a ≺ b ∧ c ≺ d} ≺ x" >> {
      val l = pk1 ≺: v1 ≺: p1
      val r = pk2 ≺: v2 ≺: p1
      (l ∧ r) must_= P.both(pk1 ≺: v1, pk2 ≺: v2) ≺: p1
    }
  }

  "autojoined" >> {
    "p1 ⋈ p1" >> {
      autojoined(p1, p1) must_= ((JoinKeys.empty, true))
    }

    "p1 ⋈ p2" >> {
      autojoined(p1, p2) must_= ((JoinKeys.empty, false))
    }

    "∏(k :: T) ⋈ ∏(k :: F)" >> {
      val kt = P.project('k', T)
      val kf = P.project('k', F)

      autojoined(kt, kf) must_= ((JoinKeys.empty, false))
    }

    "∆ :: T ⋈ ∏ :: T" >> {
      autojoined(v1, pk1) must_= ((JoinKeys.singleton(v1i, Left('a')), true))
    }

    "∆ :: T ⋈ ∏ :: F" >> {
      val v = P.inflate(Right(3), T)
      val pk = P.project('y', F)
      autojoined(v, pk) must_= ((JoinKeys.empty, false))
    }

    "∆ :: T ⋈ ∆ :: T" >> {
      autojoined(v1, v2) must_= ((JoinKeys.singleton(v1i, v2i), true))
    }

    "∆ :: T ⋈ ∆ :: F" >> {
      val vt = P.inflate(Right(1), T)
      val vf = P.inflate(Right(1), F)
      autojoined(vt, vf) must_= ((JoinKeys.empty, false))
    }

    "{v1 ∧ v2} ⋈ v3" >> {
      val keys =
        JoinKeys(IList(NonEmptyList(NonEmptyList(
          JoinKey(v1i, v3i),
          JoinKey(v2i, v3i)))))

      autojoined(P.both(v1, v2), v3) must_= ((keys, true))
    }

    "{v1 ∧ v2} ⋈ {v3 ∧ v4}" >> {
      val keys =
        JoinKeys(IList(NonEmptyList(NonEmptyList(
          JoinKey(v1i, v3i),
          JoinKey(v1i, v4i),
          JoinKey(v2i, v3i),
          JoinKey(v2i, v4i)))))

      autojoined(P.both(v1, v2), P.both(v3, v4)) must_= ((keys, true))
    }

    "{v1 ∧ v2} ⋈ (v3 ≺ v4)" >> {
      val keys =
        JoinKeys(IList(NonEmptyList(NonEmptyList(
          JoinKey(v1i, v4i),
          JoinKey(v2i, v4i)))))

      autojoined(P.both(v1, v2), P.thenn(v3, v4)) must_= ((keys, false))
    }

    "(v2 ≺ v1) ⋈ v3" >> {
      autojoined(P.thenn(v2, v1), v3) must_= ((JoinKeys.singleton(v1i, v3i), false))
    }

    "(v2 ≺ v1) ⋈ (v4 ≺ v3)" >> {
      val keys =
        JoinKeys(IList(NonEmptyList(
          NonEmptyList(JoinKey(v1i, v3i)),
         NonEmptyList(JoinKey(v2i, v4i)))))

      autojoined(P.thenn(v2, v1), P.thenn(v4, v3)) must_= ((keys, true))
    }

    "(pk1 ≺ v1) ⋈ (pk1 ≺ v3)" >> {
      autojoined(P.thenn(pk1, v1), P.thenn(pk1, v3)) must_= ((JoinKeys.singleton(v1i, v3i), true))
    }

    "(pk1 ≺ v1) ⋈ (pk2 ≺ v3)" >> {
      autojoined(P.thenn(pk1, v1), P.thenn(pk2, v3)) must_= ((JoinKeys.singleton(v1i, v3i), false))
    }

    "({v1 ∧ v2} ≺ p) ⋈ (v3 ≺ p)" >> {
      val keys =
        JoinKeys(IList(NonEmptyList(NonEmptyList(
          JoinKey(v1i, v3i),
          JoinKey(v2i, v3i)))))

      autojoined(P.thenn(P.both(v1, v2), p1), P.thenn(v3, p1)) must_= ((keys, true))
    }

    "{v1 ∧ (v2 ≺ p)} ⋈ (v3 ≺ p)" >> {
      val keys =
        JoinKeys(IList(NonEmptyList(NonEmptyList(
          JoinKey(v2i, v3i)))))

      autojoined(P.both(v1, P.thenn(v2, p1)), P.thenn(v3, p1)) must_= ((keys, true))
    }
  }

  implicit def conjProvArbitrary: Arbitrary[Fix[P.PF] @@ Conjunction] =
    Conjunction.subst(implicitly[Arbitrary[Fix[P.PF]]])

  implicit def conjProvShow: Show[Fix[P.PF] @@ Conjunction] =
    Conjunction.subst(Show[Fix[P.PF]])

  "laws" >> addFragments(properties(IList(
    eql.laws[Fix[P.PF]],
    semigroup.laws[Fix[P.PF]],
    semilattice.laws[Fix[P.PF] @@ Conjunction]
  ).foldLeft(new Properties("scalaz")) { (p, l) => p.include(l); p }))
}

object ProvSpec {
  type I = Either[Char, Int]
  val P = Prov[Char, I, Boolean, Fix[ProvF[Char, I, Boolean, ?]]](stdLeft)
}
*/
