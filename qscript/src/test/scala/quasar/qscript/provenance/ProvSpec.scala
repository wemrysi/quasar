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

package quasar.qscript.provenance

import slamdata.Predef.{Boolean, Char, Int, Option}
import quasar.contrib.matryoshka.arbitrary._
import quasar.Qspec

import scala.Predef.implicitly

import matryoshka.data.Fix
import org.scalacheck.{Arbitrary, Properties}
import org.specs2.scalacheck._
import scalaz.{@@, IList, NonEmptyList, Writer, Validation => V, Show}
import scalaz.Tags.Conjunction
import scalaz.scalacheck.ScalazProperties.{equal => eql, semigroup, semilattice}
import scalaz.std.anyVal._
import scalaz.std.option._
import scalaz.std.tuple._
import scalaz.syntax.equal._
import scalaz.syntax.show._

final class ProvSpec extends Qspec with ProvFGenerator {

  implicit val params = Parameters(maxSize = 10)

  import ProvSpec.P.implicits._

  val P = ProvSpec.P

  val p1 = P.prjPath('x')
  val p2 = P.prjPath('y')
  val p3 = P.prjPath('z')

  val pk1 = P.prjValue('a')
  val pk2 = P.prjValue('b')

  val ik1 = P.injValue('a')
  val ik2 = P.injValue('b')
  val ik3 = P.injValue('c')

  val v1i = 1
  val v1 = P.value(v1i)
  val v2i = 2
  val v2 = P.value(v2i)
  val v3i = 3
  val v3 = P.value(v3i)
  val v4i = 4
  val v4 = P.value(v4i)

  def autojoined(a: Fix[P.PF], b: Fix[P.PF]): (JoinKeys[Int], Boolean) =
    P.autojoined[Writer[JoinKeys[Int], ?]](a, b).run

  "apply projection" >> {
    "prj(k) << inj(k) << p == p" >> {
      P.applyProjection(P.thenn(pk1, P.thenn(ik1, p1))) must_= V.success(Option(p1))
    }

    "prj(k) << (inj(k) ∧ p) << q == q" >> {
      P.applyProjection(P.thenn(pk1, P.thenn(P.both(ik1, p2), p1))) must_= V.success(Option(p1))
    }

    "prj(k) << (inj(k) ∧ (∃ << q)) << r == q << r" >> {
      val fp = P.fresh()
      val tree = P.thenn(pk1, P.thenn(P.both(ik1, P.thenn(fp, p2)), p1))
      P.applyProjection(tree) must_= V.success(Option(P.thenn(p2, p1)))
    }

    "prj(k) << (inj(j) ∧ (inj(k) << q)) << r == q << r" >> {
      val tree = P.thenn(pk1, P.thenn(P.both(ik2, P.thenn(ik1, p2)), p1))
      P.applyProjection(tree) must_= V.success(Option(P.thenn(p2, p1)))
    }

    "prj(k) << (inj(k) ∧ (inj(j) << q)) << r == r" >> {
      val tree = P.thenn(pk1, P.thenn(P.both(ik1, P.thenn(ik2, p2)), p1))
      P.applyProjection(tree) must_= V.success(Option(p1))
    }

    "prj(k) << inj(j) << q == Failure" >> prop { (k: Char, j: Char, p: Fix[P.PF]) => (k =/= j) ==> {
      val tree = P.thenn(P.prjValue(k), P.thenn(P.injValue(j), p))
      P.applyProjection(tree) must beFailure
    }}

    "prj(k) << (p ∧ q) << r == id" >> {
      val tree = P.thenn(pk1, P.thenn(P.both(p3, p2), p1))
      P.applyProjection(tree).map(_.shows) must beSuccess(Option(tree).shows)
    }

    "prj(k) << (inj(j) ∧ inj(k)) << r == r" >> {
      val tree = P.thenn(pk1, P.thenn(P.both(ik2, ik1), p1))
      P.applyProjection(tree) must_= V.success(Option(p1))
    }

    "prj(k) << (inj(k) ∧ inj(j)) << r == r" >> {
      val tree = P.thenn(pk1, P.thenn(P.both(ik1, ik2), p1))
      P.applyProjection(tree) must_= V.success(Option(p1))
    }

    "prj(k) << (inj(j) ∧ inj(l) << q) << r == Failure" >> {
      val tree = P.thenn(pk1, P.thenn(P.both(ik2, P.thenn(ik3, p2)), p1))
      P.applyProjection(tree) must beFailure
    }

    "prj(k) << (inj(j) ∧ p << q) << r == prj(k) << p << q << r" >> {
      val tree = P.thenn(pk1, P.thenn(P.both(ik2, P.thenn(v1, p2)), p1))
      P.applyProjection(tree) must_= V.success(Option(P.thenn(pk1, P.thenn(v1, P.thenn(p2, p1)))))
    }

    "multiple projects on nested conjunctions" >> {
      //inj{l} ≺ inj{0} ≺ 1 ∧ inj{r} ≺ 1)
      val x = P.thenn(P.injValue('r'), v1)
      val y = P.both(P.thenn(P.injValue('l'), P.thenn(P.injValue('0'), v1)), x)

      val lhs = P.thenn(P.injValue('l'), y)
      val rhs = P.thenn(P.injValue('r'), v1)

      // inj{l} ≺ (inj{l} ≺ inj{0} ≺ 1 ∧ inj{r} ≺ 1) ∧ inj{r} ≺ 1
      val tree = P.both(lhs, rhs)

      val prjL = P.applyProjection(P.thenn(P.prjValue('l'), tree))
      val prjLR = prjL.map(_.map(l => P.applyProjection(P.thenn(P.prjValue('r'), l))))

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

    "((z ∧ q) ≺ y ≺ x) ∧ ((w ∧ q) ≺ y ≺ x) == (z ∧ w ∧ q) ≺ y ≺ x" >> {
      val l = (pk1 ∧ v1) ≺: p2 ≺: p1
      val r = (pk2 ∧ v1) ≺: p2 ≺: p1
      (l ∧ r) must_= P.both(pk1, P.both(pk2, v1)) ≺: p2 ≺: p1
    }

    "((a ∧ b) ≺ y ≺ x) ∧ ((c ∧ d) ≺ y ≺ x) == (a ∧ b ∧ c ∧ d) ≺ y ≺ x" >> {
      val l = (pk1 ∧ v1) ≺: p2 ≺: p1
      val r = (pk2 ∧ v2) ≺: p2 ≺: p1
      (l ∧ r) must_= P.both(v2, P.both(pk1, P.both(pk2, v1))) ≺: p2 ≺: p1
    }

    "(a ≺ b ≺ x) ∧ (c ≺ d ≺ x) == (a ≺ b ∧ c ≺ d) ≺ x" >> {
      val l = pk1 ≺: v1 ≺: p1
      val r = pk2 ≺: v2 ≺: p1
      (l ∧ r) must_= P.both(pk1 ≺: v1, pk2 ≺: v2) ≺: p1
    }
  }

  "autojoined" >> {
    "p1 θ p1" >> {
      autojoined(p1, p1) must_= ((JoinKeys.empty, true))
    }

    "p1 θ p2" >> {
      autojoined(p1, p2) must_= ((JoinKeys.empty, false))
    }

    "pk1 θ pk1" >> {
      autojoined(pk1, pk1) must_= ((JoinKeys.empty, true))
    }

    "pk1 θ pk2" >> {
      autojoined(pk1, pk2) must_= ((JoinKeys.empty, false))
    }

    "prjPath θ prjValue" >> {
      autojoined(p1, pk1) must_= ((JoinKeys.empty, false))
    }

    "value θ prjPath" >> {
      autojoined(v1, p1) must_= ((JoinKeys.empty, false))
    }

    "value θ prjValue" >> {
      autojoined(v1, pk1) must_= ((JoinKeys.empty, false))
    }

    "value θ value" >> {
      autojoined(v1, v2) must_= ((JoinKeys.singleton(v1i, v2i), true))
    }

    "(v1 ∧ v2) θ v3" >> {
      val keys =
        JoinKeys(IList(NonEmptyList(NonEmptyList(
          JoinKey(v1i, v3i),
          JoinKey(v2i, v3i)))))

      autojoined(P.both(v1, v2), v3) must_= ((keys, true))
    }

    "(v1 ∧ v2) θ (v3 ∧ v4)" >> {
      val keys =
        JoinKeys(IList(NonEmptyList(NonEmptyList(
          JoinKey(v1i, v3i),
          JoinKey(v1i, v4i),
          JoinKey(v2i, v3i),
          JoinKey(v2i, v4i)))))

      autojoined(P.both(v1, v2), P.both(v3, v4)) must_= ((keys, true))
    }

    "(v1 ∧ v2) θ (v3 << v4)" >> {
      val keys =
        JoinKeys(IList(NonEmptyList(NonEmptyList(
          JoinKey(v1i, v4i),
          JoinKey(v2i, v4i)))))

      autojoined(P.both(v1, v2), P.thenn(v3, v4)) must_= ((keys, false))
    }

    "(v2 << v1) θ v3" >> {
      autojoined(P.thenn(v2, v1), v3) must_= ((JoinKeys.singleton(v1i, v3i), false))
    }

    "(v2 << v1) θ (v4 << v3)" >> {
      val keys =
        JoinKeys(IList(NonEmptyList(
          NonEmptyList(JoinKey(v1i, v3i)),
         NonEmptyList(JoinKey(v2i, v4i)))))

      autojoined(P.thenn(v2, v1), P.thenn(v4, v3)) must_= ((keys, true))
    }

    "(pk1 << v1) θ (pk1 << v3)" >> {
      autojoined(P.thenn(pk1, v1), P.thenn(pk1, v3)) must_= ((JoinKeys.singleton(v1i, v3i), true))
    }

    "(pk1 << v1) θ (pk2 << v3)" >> {
      autojoined(P.thenn(pk1, v1), P.thenn(pk2, v3)) must_= ((JoinKeys.singleton(v1i, v3i), false))
    }

    "((v1 ∧ v2) << p) θ (v3 << p)" >> {
      val keys =
        JoinKeys(IList(NonEmptyList(NonEmptyList(
          JoinKey(v1i, v3i),
          JoinKey(v2i, v3i)))))

      autojoined(P.thenn(P.both(v1, v2), p1), P.thenn(v3, p1)) must_= ((keys, true))
    }

    "(v1 ∧ (v2 << p)) θ (v3 << p)" >> {
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
  val P = Prov[Char, Int, Fix[ProvF[Char, Int, ?]]]
}
